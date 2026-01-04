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

type FinalizerOptions struct {
	Wallet  *ethwallet.Wallet
	Chain   Chain
	Mempool Mempool
	Logger  *slog.Logger

	PollInterval, PollTimeout, RetryDelay time.Duration

	FeeMargin, PriceBump int

	SubscriptionBuffer int
}

func (o FinalizerOptions) IsValid() error {
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

type Finalizer struct {
	FinalizerOptions

	isRunning atomic.Bool

	subscriptions   map[chan Event]struct{}
	subscriptionsMu sync.RWMutex

	sendMu sync.Mutex
}

type Event struct {
	Removed, Added *types.Transaction
}

func NewFinalizer(options FinalizerOptions) (*Finalizer, error) {
	if err := options.IsValid(); err != nil {
		return nil, err
	}

	if options.Logger == nil {
		options.Logger = slog.New(slog.DiscardHandler)
	}

	return &Finalizer{
		FinalizerOptions: options,

		subscriptions: map[chan Event]struct{}{},
	}, nil
}

func (f *Finalizer) IsRunning() bool {
	return f.isRunning.Load()
}

func (f *Finalizer) Run(ctx context.Context) error {
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
				events := map[uint64]*Event{}

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
							event = &Event{}
							events[transaction.Nonce()] = event
						}

						event.Removed = transaction
					}
				}

				for _, transaction := range added {
					if transaction != nil {
						event := events[transaction.Nonce()]
						if event == nil {
							event = &Event{}
							events[transaction.Nonce()] = event
						}

						event.Added = transaction
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

				chainNonce, err := f.Chain.LatestNonce(ctx, f.Wallet.Address())
				if err != nil {
					return fmt.Errorf("unable to read chain nonce: %w", err)
				}

				transactions, err := f.Mempool.PriciestTransactions(ctx, chainNonce, time.Now().Add(-f.RetryDelay))
				if err != nil {
					return fmt.Errorf("unable to read mempool transactions: %w", err)
				}

				gasPrice, err := f.Chain.GasPrice(ctx)
				if err != nil {
					f.Logger.ErrorContext(ctx, "unable to read gas price", slog.Any("error", err))
					gasPrice = new(big.Int)
				}
				minGasPrice := withMargin(gasPrice, f.FeeMargin)

				baseFee, err := f.Chain.BaseFee(ctx)
				if err != nil {
					f.Logger.ErrorContext(ctx, "unable to read base fee", slog.Any("error", err))
					baseFee = new(big.Int)
				}
				safeBaseFee := withMargin(baseFee, f.FeeMargin)

				priorityFee, err := f.Chain.PriorityFee(ctx)
				if err != nil {
					f.Logger.ErrorContext(ctx, "unable to read priority fee", slog.Any("error", err))
					priorityFee = new(big.Int)
				}
				minPriorityFee := withMargin(priorityFee, f.FeeMargin)

				maxFee := new(big.Int).Add(baseFee, priorityFee)
				minMaxFee := new(big.Int).Add(safeBaseFee, minPriorityFee)

				nonce := chainNonce
				for ; transactions[nonce] != nil; nonce++ {
					transaction := transactions[nonce]

					var replacement *types.Transaction
					switch transaction.Type() {
					case types.LegacyTxType:
						if transaction.GasPrice().Cmp(gasPrice) < 0 {
							gasPrice := maxBigInt(withMargin(transaction.GasPrice(), f.PriceBump), minGasPrice)

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
							gasPrice := maxBigInt(withMargin(transaction.GasPrice(), f.PriceBump), minGasPrice)

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
						if transaction.GasFeeCapIntCmp(maxFee) < 0 || transaction.GasTipCapIntCmp(priorityFee) < 0 {
							maxFee := maxBigInt(withMargin(transaction.GasFeeCap(), f.PriceBump), minMaxFee)
							priorityFee := maxBigInt(withMargin(transaction.GasTipCap(), f.PriceBump), minPriorityFee)

							replacement = types.NewTx(&types.DynamicFeeTx{
								ChainID:    transaction.ChainId(),
								Nonce:      transaction.Nonce(),
								GasTipCap:  priorityFee,
								GasFeeCap:  maxFee,
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
								slog.String("oldMaxFee", transaction.GasFeeCap().String()),
								slog.String("newMaxFee", replacement.GasFeeCap().String()),
								slog.String("oldPriorityFee", transaction.GasTipCap().String()),
								slog.String("newPriorityFee", replacement.GasTipCap().String()),
							)
						}

					case types.BlobTxType:
						if transaction.GasFeeCapIntCmp(maxFee) < 0 || transaction.GasTipCapIntCmp(priorityFee) < 0 {
							maxFee := maxBigInt(withMargin(transaction.GasFeeCap(), f.PriceBump), minMaxFee)
							priorityFee := maxBigInt(withMargin(transaction.GasTipCap(), f.PriceBump), minPriorityFee)

							replacement = types.NewTx(&types.BlobTx{
								ChainID:    uint256.MustFromBig(transaction.ChainId()),
								Nonce:      transaction.Nonce(),
								GasTipCap:  uint256.MustFromBig(priorityFee),
								GasFeeCap:  uint256.MustFromBig(maxFee),
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
								slog.String("oldMaxFee", transaction.GasFeeCap().String()),
								slog.String("newMaxFee", replacement.GasFeeCap().String()),
								slog.String("oldPriorityFee", transaction.GasTipCap().String()),
								slog.String("newPriorityFee", replacement.GasTipCap().String()),
							)
						}

					case types.SetCodeTxType:
						if transaction.GasFeeCapIntCmp(maxFee) < 0 || transaction.GasTipCapIntCmp(priorityFee) < 0 {
							maxFee := maxBigInt(withMargin(transaction.GasFeeCap(), f.PriceBump), minMaxFee)
							priorityFee := maxBigInt(withMargin(transaction.GasTipCap(), f.PriceBump), minPriorityFee)

							replacement = types.NewTx(&types.SetCodeTx{
								ChainID:    uint256.MustFromBig(transaction.ChainId()),
								Nonce:      transaction.Nonce(),
								GasTipCap:  uint256.MustFromBig(priorityFee),
								GasFeeCap:  uint256.MustFromBig(maxFee),
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
								slog.String("oldMaxFee", transaction.GasFeeCap().String()),
								slog.String("newMaxFee", replacement.GasFeeCap().String()),
								slog.String("oldPriorityFee", transaction.GasTipCap().String()),
								slog.String("newPriorityFee", replacement.GasTipCap().String()),
							)
						}
					}

					if replacement == nil {
						f.Logger.DebugContext(ctx, "resending", slog.String("transaction", transaction.Hash().String()), slog.Uint64("nonce", transaction.Nonce()))

						if err := f.Mempool.Commit(ctx, transaction); err != nil {
							f.Logger.ErrorContext(ctx, "unable to update transaction in mempool", slog.Any("error", err), slog.String("transaction", transaction.Hash().String()))
						}

						if err := f.Chain.Send(ctx, transaction); err != nil {
							f.Logger.ErrorContext(ctx, "unable to resend transaction to chain", slog.Any("error", err), slog.String("transaction", transaction.Hash().String()))
						}
					} else {
						if replacement, err = f.Wallet.SignTransaction(replacement, f.Chain.ChainID()); err == nil {
							if err := f.Mempool.Commit(ctx, replacement); err != nil {
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

func (f *Finalizer) Subscribe(ctx context.Context) <-chan Event {
	f.subscriptionsMu.Lock()
	defer f.subscriptionsMu.Unlock()

	subscription := make(chan Event, f.SubscriptionBuffer)
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

func (f *Finalizer) Send(ctx context.Context, transaction *types.Transaction) (*types.Transaction, error) {
	f.sendMu.Lock()
	defer f.sendMu.Unlock()

	switch transaction.Type() {
	case types.LegacyTxType:
		gasPrice, err := f.Chain.GasPrice(ctx)
		if err != nil {
			f.Logger.ErrorContext(ctx, "unable to read gas price", slog.Any("error", err))
			gasPrice = new(big.Int)
		}

		gasPrice = maxBigInt(transaction.GasPrice(), withMargin(gasPrice, f.FeeMargin))

		transaction = types.NewTx(&types.LegacyTx{
			Nonce:    transaction.Nonce(),
			GasPrice: gasPrice,
			Gas:      transaction.Gas(),
			To:       transaction.To(),
			Value:    transaction.Value(),
			Data:     transaction.Data(),
		})

	case types.AccessListTxType:
		gasPrice, err := f.Chain.GasPrice(ctx)
		if err != nil {
			f.Logger.ErrorContext(ctx, "unable to read gas price", slog.Any("error", err))
			gasPrice = new(big.Int)
		}

		gasPrice = maxBigInt(transaction.GasPrice(), withMargin(gasPrice, f.FeeMargin))

		transaction = types.NewTx(&types.AccessListTx{
			ChainID:    transaction.ChainId(),
			Nonce:      transaction.Nonce(),
			GasPrice:   gasPrice,
			Gas:        transaction.Gas(),
			To:         transaction.To(),
			Value:      transaction.Value(),
			Data:       transaction.Data(),
			AccessList: transaction.AccessList(),
		})

	case types.DynamicFeeTxType:
		baseFee, err := f.Chain.BaseFee(ctx)
		if err != nil {
			f.Logger.ErrorContext(ctx, "unable to read base fee", slog.Any("error", err))
			baseFee = new(big.Int)
		}

		priorityFee, err := f.Chain.PriorityFee(ctx)
		if err != nil {
			f.Logger.ErrorContext(ctx, "unable to read priority fee", slog.Any("error", err))
			priorityFee = new(big.Int)
		}

		priorityFee = maxBigInt(transaction.GasTipCap(), withMargin(priorityFee, f.FeeMargin))

		baseFee = withMargin(baseFee, f.FeeMargin)
		maxFee := maxBigInt(transaction.GasFeeCap(), new(big.Int).Add(baseFee, priorityFee))

		transaction = types.NewTx(&types.DynamicFeeTx{
			ChainID:    transaction.ChainId(),
			Nonce:      transaction.Nonce(),
			GasTipCap:  priorityFee,
			GasFeeCap:  maxFee,
			Gas:        transaction.Gas(),
			To:         transaction.To(),
			Value:      transaction.Value(),
			Data:       transaction.Data(),
			AccessList: transaction.AccessList(),
		})

	case types.BlobTxType:
		baseFee, err := f.Chain.BaseFee(ctx)
		if err != nil {
			f.Logger.ErrorContext(ctx, "unable to read base fee", slog.Any("error", err))
			baseFee = new(big.Int)
		}

		priorityFee, err := f.Chain.PriorityFee(ctx)
		if err != nil {
			f.Logger.ErrorContext(ctx, "unable to read priority fee", slog.Any("error", err))
			priorityFee = new(big.Int)
		}

		priorityFee = maxBigInt(transaction.GasTipCap(), withMargin(priorityFee, f.FeeMargin))

		baseFee = withMargin(baseFee, f.FeeMargin)
		maxFee := maxBigInt(transaction.GasFeeCap(), new(big.Int).Add(baseFee, priorityFee))

		transaction = types.NewTx(&types.BlobTx{
			ChainID:    uint256.MustFromBig(transaction.ChainId()),
			Nonce:      transaction.Nonce(),
			GasTipCap:  uint256.MustFromBig(priorityFee),
			GasFeeCap:  uint256.MustFromBig(maxFee),
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
		baseFee, err := f.Chain.BaseFee(ctx)
		if err != nil {
			f.Logger.ErrorContext(ctx, "unable to read base fee", slog.Any("error", err))
			baseFee = new(big.Int)
		}

		priorityFee, err := f.Chain.PriorityFee(ctx)
		if err != nil {
			f.Logger.ErrorContext(ctx, "unable to read priority fee", slog.Any("error", err))
			priorityFee = new(big.Int)
		}

		priorityFee = maxBigInt(transaction.GasTipCap(), withMargin(priorityFee, f.FeeMargin))

		baseFee = withMargin(baseFee, f.FeeMargin)
		maxFee := maxBigInt(transaction.GasFeeCap(), new(big.Int).Add(baseFee, priorityFee))

		transaction = types.NewTx(&types.SetCodeTx{
			ChainID:    uint256.MustFromBig(transaction.ChainId()),
			Nonce:      transaction.Nonce(),
			GasTipCap:  uint256.MustFromBig(priorityFee),
			GasFeeCap:  uint256.MustFromBig(maxFee),
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

	f.Logger.DebugContext(ctx, "sending", slog.String("transaction", transaction.Hash().String()), slog.Uint64("nonce", transaction.Nonce()))

	if err := f.Mempool.Commit(ctx, transaction); err != nil {
		return nil, fmt.Errorf("unable to commit transaction to mempool: %w", err)
	}

	if err := f.Chain.Send(ctx, transaction); err != nil {
		f.Logger.ErrorContext(ctx, "unable to send transaction to chain", slog.Any("error", err), slog.String("transaction", transaction.Hash().String()))
	}

	return transaction, nil
}
