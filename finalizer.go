package finalizer

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/0xsequence/ethkit/ethwallet"
	"github.com/0xsequence/ethkit/go-ethereum/core/types"
	"github.com/holiman/uint256"
)

type FinalizerOptions[T any] struct {
	Wallet  *ethwallet.Wallet
	Chain   Chain
	Mempool Mempool[T]
	Logger  *slog.Logger

	PollInterval, PollTimeout, RetryDelay time.Duration

	FeeMargin, PriceBump int

	SubscriptionBuffer int
}

func (o FinalizerOptions[T]) IsValid() error {
	if o.Wallet == nil {
		return fmt.Errorf("no wallet")
	}

	if o.Chain == nil {
		return fmt.Errorf("no chain")
	}

	if o.Mempool == nil {
		return fmt.Errorf("no mempool")
	}

	if o.PollInterval <= 0 {
		return fmt.Errorf("non-positive poll interval %v", o.PollInterval)
	}

	if o.PollTimeout <= 0 {
		return fmt.Errorf("non-positive poll timeout %v", o.PollTimeout)
	}

	if o.RetryDelay <= 0 {
		return fmt.Errorf("non-positive retry delay %v", o.RetryDelay)
	}

	if o.FeeMargin < 0 {
		return fmt.Errorf("negative fee margin %v", o.FeeMargin)
	}

	if o.PriceBump < 0 {
		return fmt.Errorf("negative price bump %v", o.PriceBump)
	}

	if o.SubscriptionBuffer < 0 {
		return fmt.Errorf("negative subscription buffer %v", o.SubscriptionBuffer)
	}

	return nil
}

type Finalizer[T any] struct {
	FinalizerOptions[T]

	isRunning atomic.Bool

	subscriptions   map[chan Event[T]]struct{}
	subscriptionsMu sync.RWMutex

	sendMu sync.Mutex
}

type Event[T any] struct {
	Removed, Added *Transaction[T]
}

type Transaction[T any] struct {
	*types.Transaction

	Metadata T
}

func NewFinalizer[T any](options FinalizerOptions[T]) (*Finalizer[T], error) {
	if err := options.IsValid(); err != nil {
		return nil, err
	}

	if options.Logger == nil {
		options.Logger = slog.New(slog.DiscardHandler)
	}

	return &Finalizer[T]{
		FinalizerOptions: options,

		subscriptions: map[chan Event[T]]struct{}{},
	}, nil
}

func (f *Finalizer[T]) IsRunning() bool {
	return f.isRunning.Load()
}

func (f *Finalizer[T]) Run(ctx context.Context) error {
	if !f.isRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("already running")
	}
	defer f.isRunning.Store(false)

	diffs, err := f.Chain.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("unable to subscribe to chain: %w", err)
	}

	go func() {
		for diff := range diffs {
			func() {
				events := map[uint64]*Event[T]{}

				ctx, cancel := context.WithTimeout(ctx, f.PollTimeout)
				defer cancel()

				removed, err := f.Mempool.Transactions(ctx, diff.Removed)
				if err != nil {
					f.Logger.ErrorContext(ctx, "unable to read removed transactions", slog.Any("error", err))
				}

				added, err := f.Mempool.Transactions(ctx, diff.Added)
				if err != nil {
					f.Logger.ErrorContext(ctx, "unable to read added transactions", slog.Any("error", err))
				}

				for _, transaction := range removed {
					if transaction != nil {
						event := events[transaction.Nonce()]
						if event == nil {
							event = &Event[T]{}
							events[transaction.Nonce()] = event
						}

						event.Removed = transaction

						f.Logger.DebugContext(
							ctx,
							"transaction reorged",
							slog.String("transaction", transaction.Hash().String()),
							slog.Uint64("nonce", transaction.Nonce()),
						)
					}
				}

				for _, transaction := range added {
					if transaction != nil {
						event := events[transaction.Nonce()]
						if event == nil {
							event = &Event[T]{}
							events[transaction.Nonce()] = event
						}

						event.Added = transaction

						f.Logger.DebugContext(
							ctx,
							"transaction mined",
							slog.String("transaction", transaction.Hash().String()),
							slog.Uint64("nonce", transaction.Nonce()),
							slog.String("gasPrice", transaction.GasPrice().String()),
							slog.String("priorityFee", transaction.GasTipCap().String()),
						)
					}
				}

				f.subscriptionsMu.RLock()
				defer f.subscriptionsMu.RUnlock()

				for _, event := range events {
					if event.Added == nil || event.Removed == nil || event.Added.Hash() != event.Removed.Hash() {
						for subscription := range f.subscriptions {
							subscription <- *event
						}
					}
				}
			}()
		}
	}()

	ticks := time.Tick(f.PollInterval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticks:
			err := func() error {
				ctx, cancel := context.WithTimeout(ctx, f.PollTimeout)
				defer cancel()

				f.Logger.DebugContext(ctx, "polling", slog.Duration("interval", f.PollInterval), slog.Duration("timeout", f.PollTimeout))

				chainNonce, err := f.Chain.LatestNonce(ctx, f.Wallet.Address())
				if err != nil {
					return fmt.Errorf("unable to read chain nonce: %w", err)
				}

				transactions, err := f.Mempool.PriciestTransactions(ctx, chainNonce, time.Now().Add(-f.RetryDelay))
				if err != nil {
					return fmt.Errorf("unable to read mempool transactions: %w", err)
				}

				baseFee, err := f.Chain.BaseFee(ctx)
				if err != nil {
					f.Logger.ErrorContext(ctx, "unable to read base fee", slog.Any("error", err))
					baseFee = new(big.Int)
				}
				baseFee = withMargin(baseFee, f.FeeMargin)

				priorityFee, err := f.Chain.PriorityFee(ctx)
				if err != nil {
					f.Logger.ErrorContext(ctx, "unable to read priority fee", slog.Any("error", err))
					priorityFee = new(big.Int)
				}

				gasPrice := new(big.Int).Add(baseFee, priorityFee)

				nonce := chainNonce
				for ; transactions[nonce] != nil; nonce++ {
					transaction := transactions[nonce]

					var replacement *types.Transaction
					switch transaction.Type() {
					case types.LegacyTxType:
						if transaction.GasPrice().Cmp(gasPrice) < 0 {
							gasPrice := maxBigInt(withMargin(transaction.GasPrice(), f.PriceBump), gasPrice)

							replacement = types.NewTx(&types.LegacyTx{
								Nonce:    transaction.Nonce(),
								GasPrice: gasPrice,
								Gas:      transaction.Gas(),
								To:       transaction.To(),
								Value:    transaction.Value(),
								Data:     transaction.Data(),
							})

							f.Logger.DebugContext(
								ctx,
								"replacing",
								slog.Uint64("transaction", transaction.Nonce()),
								slog.String("nonce", transaction.Hash().String()),
								slog.String("oldGasPrice", transaction.GasPrice().String()),
								slog.String("newGasPrice", replacement.GasPrice().String()),
							)
						}

					case types.AccessListTxType:
						if transaction.GasPrice().Cmp(gasPrice) < 0 {
							gasPrice := maxBigInt(withMargin(transaction.GasPrice(), f.PriceBump), gasPrice)

							replacement = types.NewTx(&types.AccessListTx{
								ChainID:    transaction.ChainId(),
								Nonce:      transaction.Nonce(),
								GasPrice:   gasPrice,
								Gas:        transaction.Gas(),
								To:         transaction.To(),
								Value:      transaction.Value(),
								Data:       transaction.Data(),
								AccessList: transaction.AccessList(),
							})

							f.Logger.DebugContext(
								ctx,
								"replacing",
								slog.Uint64("transaction", transaction.Nonce()),
								slog.String("nonce", transaction.Hash().String()),
								slog.String("oldGasPrice", transaction.GasPrice().String()),
								slog.String("newGasPrice", replacement.GasPrice().String()),
							)
						}

					case types.DynamicFeeTxType:
						if transaction.GasFeeCapIntCmp(gasPrice) < 0 || transaction.GasTipCapIntCmp(priorityFee) < 0 {
							gasPrice := maxBigInt(withMargin(transaction.GasFeeCap(), f.PriceBump), gasPrice)
							priorityFee := maxBigInt(withMargin(transaction.GasTipCap(), f.PriceBump), priorityFee)

							replacement = types.NewTx(&types.DynamicFeeTx{
								ChainID:    transaction.ChainId(),
								Nonce:      transaction.Nonce(),
								GasTipCap:  priorityFee,
								GasFeeCap:  gasPrice,
								Gas:        transaction.Gas(),
								To:         transaction.To(),
								Value:      transaction.Value(),
								Data:       transaction.Data(),
								AccessList: transaction.AccessList(),
							})

							f.Logger.DebugContext(
								ctx,
								"replacing",
								slog.Uint64("transaction", transaction.Nonce()),
								slog.String("nonce", transaction.Hash().String()),
								slog.String("oldGasPrice", transaction.GasFeeCap().String()),
								slog.String("newGasPrice", replacement.GasFeeCap().String()),
								slog.String("oldPriorityFee", transaction.GasTipCap().String()),
								slog.String("newPriorityFee", replacement.GasTipCap().String()),
							)
						}

					case types.BlobTxType:
						if transaction.GasFeeCapIntCmp(gasPrice) < 0 || transaction.GasTipCapIntCmp(priorityFee) < 0 {
							gasPrice := maxBigInt(withMargin(transaction.GasFeeCap(), f.PriceBump), gasPrice)
							priorityFee := maxBigInt(withMargin(transaction.GasTipCap(), f.PriceBump), priorityFee)

							replacement = types.NewTx(&types.BlobTx{
								ChainID:    uint256.MustFromBig(transaction.ChainId()),
								Nonce:      transaction.Nonce(),
								GasTipCap:  uint256.MustFromBig(priorityFee),
								GasFeeCap:  uint256.MustFromBig(gasPrice),
								Gas:        transaction.Gas(),
								To:         dereference(transaction.To()),
								Value:      uint256.MustFromBig(transaction.Value()),
								Data:       transaction.Data(),
								AccessList: transaction.AccessList(),
								BlobFeeCap: uint256.MustFromBig(withMargin(transaction.BlobGasFeeCap(), f.PriceBump)),
								BlobHashes: transaction.BlobHashes(),
								Sidecar:    transaction.BlobTxSidecar(),
							})

							f.Logger.DebugContext(
								ctx,
								"replacing",
								slog.Uint64("transaction", transaction.Nonce()),
								slog.String("nonce", transaction.Hash().String()),
								slog.String("oldGasPrice", transaction.GasFeeCap().String()),
								slog.String("newGasPrice", replacement.GasFeeCap().String()),
								slog.String("oldPriorityFee", transaction.GasTipCap().String()),
								slog.String("newPriorityFee", replacement.GasTipCap().String()),
							)
						}

					case types.SetCodeTxType:
						if transaction.GasFeeCapIntCmp(gasPrice) < 0 || transaction.GasTipCapIntCmp(priorityFee) < 0 {
							gasPrice := maxBigInt(withMargin(transaction.GasFeeCap(), f.PriceBump), gasPrice)
							priorityFee := maxBigInt(withMargin(transaction.GasTipCap(), f.PriceBump), priorityFee)

							replacement = types.NewTx(&types.SetCodeTx{
								ChainID:    uint256.MustFromBig(transaction.ChainId()),
								Nonce:      transaction.Nonce(),
								GasTipCap:  uint256.MustFromBig(priorityFee),
								GasFeeCap:  uint256.MustFromBig(gasPrice),
								Gas:        transaction.Gas(),
								To:         dereference(transaction.To()),
								Value:      uint256.MustFromBig(transaction.Value()),
								Data:       transaction.Data(),
								AccessList: transaction.AccessList(),
								AuthList:   transaction.SetCodeAuthorizations(),
							})

							f.Logger.DebugContext(
								ctx,
								"replacing",
								slog.Uint64("transaction", transaction.Nonce()),
								slog.String("nonce", transaction.Hash().String()),
								slog.String("oldGasPrice", transaction.GasFeeCap().String()),
								slog.String("newGasPrice", replacement.GasFeeCap().String()),
								slog.String("oldPriorityFee", transaction.GasTipCap().String()),
								slog.String("newPriorityFee", replacement.GasTipCap().String()),
							)
						}
					}

					if replacement == nil {
						f.Logger.DebugContext(
							ctx,
							"resending",
							slog.String("transaction", transaction.Hash().String()),
							slog.Uint64("nonce", transaction.Nonce()),
							slog.String("gasPrice", transaction.GasPrice().String()),
							slog.String("priorityFee", transaction.GasFeeCap().String()),
						)

						if err := f.Mempool.Commit(ctx, transaction.Transaction, transaction.Metadata); err != nil {
							f.Logger.ErrorContext(ctx, "unable to update transaction in mempool", slog.Any("error", err), slog.String("transaction", transaction.Hash().String()))
						}

						if err := f.Chain.Send(ctx, transaction.Transaction); err != nil {
							f.Logger.ErrorContext(ctx, "unable to resend transaction to chain", slog.Any("error", err), slog.String("transaction", transaction.Hash().String()))
						}
					} else {
						if replacement, err = f.Wallet.SignTransaction(replacement, f.Chain.ChainID()); err == nil {
							if err := f.Mempool.Commit(ctx, replacement, transaction.Metadata); err != nil {
								f.Logger.ErrorContext(ctx, "unable to commit replacement transaction to mempool", slog.Any("error", err))
								continue
							}

							if err := f.Chain.Send(ctx, replacement); err != nil {
								f.Logger.ErrorContext(ctx, "unable to send replacement transaction to chain", slog.Any("error", err), slog.String("transaction", replacement.Hash().String()))
							}
						} else {
							f.Logger.ErrorContext(ctx, "unable to sign replacement transaction", slog.Any("error", err))
							continue
						}
					}
				}

				if nonce-chainNonce != uint64(len(transactions)) {
					f.Logger.WarnContext(ctx, "missing nonce", slog.Uint64("from", chainNonce), slog.Uint64("missing", nonce), slog.Int("transactions", len(transactions)))
				}

				return nil
			}()
			if err != nil {
				f.Logger.ErrorContext(ctx, "unable to poll", slog.Any("error", err))
			}
		}
	}
}

func (f *Finalizer[T]) Subscribe(ctx context.Context) <-chan Event[T] {
	f.subscriptionsMu.Lock()
	defer f.subscriptionsMu.Unlock()

	subscription := make(chan Event[T], f.SubscriptionBuffer)
	f.subscriptions[subscription] = struct{}{}

	go func() {
		<-ctx.Done()

		f.subscriptionsMu.Lock()
		defer f.subscriptionsMu.Unlock()

		delete(f.subscriptions, subscription)
		close(subscription)
	}()

	return subscription
}

func (f *Finalizer[T]) Send(ctx context.Context, transaction *types.Transaction, metadata T) (*types.Transaction, error) {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()

	baseFee, err := f.Chain.BaseFee(ctx)
	if err != nil {
		f.Logger.ErrorContext(ctx, "unable to read base fee", slog.Any("error", err))
		baseFee = new(big.Int)
	}
	baseFee = withMargin(baseFee, f.FeeMargin)

	priorityFee, err := f.Chain.PriorityFee(ctx)
	if err != nil {
		f.Logger.ErrorContext(ctx, "unable to read priority fee", slog.Any("error", err))
		priorityFee = new(big.Int)
	}

	gasPrice := new(big.Int).Add(baseFee, priorityFee)

	switch transaction.Type() {
	case types.LegacyTxType:
		transaction = types.NewTx(&types.LegacyTx{
			Nonce:    transaction.Nonce(),
			GasPrice: maxBigInt(transaction.GasPrice(), gasPrice),
			Gas:      transaction.Gas(),
			To:       transaction.To(),
			Value:    transaction.Value(),
			Data:     transaction.Data(),
		})

	case types.AccessListTxType:
		transaction = types.NewTx(&types.AccessListTx{
			ChainID:    transaction.ChainId(),
			Nonce:      transaction.Nonce(),
			GasPrice:   maxBigInt(transaction.GasPrice(), gasPrice),
			Gas:        transaction.Gas(),
			To:         transaction.To(),
			Value:      transaction.Value(),
			Data:       transaction.Data(),
			AccessList: transaction.AccessList(),
		})

	case types.DynamicFeeTxType:
		transaction = types.NewTx(&types.DynamicFeeTx{
			ChainID:    transaction.ChainId(),
			Nonce:      transaction.Nonce(),
			GasTipCap:  maxBigInt(transaction.GasTipCap(), priorityFee),
			GasFeeCap:  maxBigInt(transaction.GasFeeCap(), gasPrice),
			Gas:        transaction.Gas(),
			To:         transaction.To(),
			Value:      transaction.Value(),
			Data:       transaction.Data(),
			AccessList: transaction.AccessList(),
		})

	case types.BlobTxType:
		transaction = types.NewTx(&types.BlobTx{
			ChainID:    uint256.MustFromBig(transaction.ChainId()),
			Nonce:      transaction.Nonce(),
			GasTipCap:  uint256.MustFromBig(maxBigInt(transaction.GasTipCap(), priorityFee)),
			GasFeeCap:  uint256.MustFromBig(maxBigInt(transaction.GasFeeCap(), gasPrice)),
			Gas:        transaction.Gas(),
			To:         dereference(transaction.To()),
			Value:      uint256.MustFromBig(transaction.Value()),
			Data:       transaction.Data(),
			AccessList: transaction.AccessList(),
			BlobFeeCap: uint256.MustFromBig(transaction.BlobGasFeeCap()),
			BlobHashes: transaction.BlobHashes(),
			Sidecar:    transaction.BlobTxSidecar(),
		})

	case types.SetCodeTxType:
		transaction = types.NewTx(&types.SetCodeTx{
			ChainID:    uint256.MustFromBig(transaction.ChainId()),
			Nonce:      transaction.Nonce(),
			GasTipCap:  uint256.MustFromBig(maxBigInt(transaction.GasTipCap(), priorityFee)),
			GasFeeCap:  uint256.MustFromBig(maxBigInt(transaction.GasFeeCap(), gasPrice)),
			Gas:        transaction.Gas(),
			To:         dereference(transaction.To()),
			Value:      uint256.MustFromBig(transaction.Value()),
			Data:       transaction.Data(),
			AccessList: transaction.AccessList(),
			AuthList:   transaction.SetCodeAuthorizations(),
		})
	}

	mempoolNonce, err := f.Mempool.Nonce(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to read mempool nonce: %w", err)
	}

	chainNonce, err := f.Chain.PendingNonce(ctx, f.Wallet.Address())
	if err != nil {
		return nil, fmt.Errorf("unable to read chain nonce: %w", err)
	}

	if chainNonce > mempoolNonce {
		f.Logger.WarnContext(ctx, "chain nonce > mempool nonce", slog.Uint64("chain", chainNonce), slog.Uint64("mempool", mempoolNonce))
	}

	transaction = withNonce(transaction, max(mempoolNonce, chainNonce))

	transaction, err = f.Wallet.SignTransaction(transaction, f.Chain.ChainID())
	if err != nil {
		return nil, fmt.Errorf("unable to sign transaction: %w", err)
	}

	f.Logger.DebugContext(
		ctx,
		"sending",
		slog.String("transaction", transaction.Hash().String()),
		slog.Uint64("nonce", transaction.Nonce()),
		slog.String("gasPrice", transaction.GasPrice().String()),
		slog.String("priorityFee", transaction.GasTipCap().String()),
	)

	if err := f.Mempool.Commit(ctx, transaction, metadata); err != nil {
		return nil, fmt.Errorf("unable to commit transaction to mempool: %w", err)
	}

	if err := f.Chain.Send(ctx, transaction); err != nil {
		f.Logger.ErrorContext(ctx, "unable to send transaction to chain", slog.Any("error", err), slog.String("transaction", transaction.Hash().String()))
	}

	return transaction, nil
}
