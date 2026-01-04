package finalizer

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"

	"github.com/0xsequence/ethkit/ethmonitor"
	"github.com/0xsequence/ethkit/ethrpc"
	"github.com/0xsequence/ethkit/go-ethereum/common"
	"github.com/0xsequence/ethkit/go-ethereum/core/types"
)

type Chain interface {
	ChainID() *big.Int

	LatestNonce(ctx context.Context, address common.Address) (uint64, error)
	PendingNonce(ctx context.Context, address common.Address) (uint64, error)

	Send(ctx context.Context, transaction *types.Transaction) error

	BaseFee(ctx context.Context) (*big.Int, error)
	PriorityFee(ctx context.Context) (*big.Int, error)

	Subscribe(ctx context.Context) (<-chan Diff, error)
}

type Diff struct {
	Removed, Added map[common.Hash]struct{}
}

type EthkitChainOptions struct {
	ChainID *big.Int

	Provider *ethrpc.Provider
	Monitor  *ethmonitor.Monitor
	Logger   *slog.Logger

	PriorityFee *big.Int
}

func (o EthkitChainOptions) IsValid() error {
	if o.ChainID == nil {
		return fmt.Errorf("no chain id")
	} else if o.ChainID.Sign() <= 0 {
		return fmt.Errorf("non-positive chain id %v", o.ChainID)
	}

	if o.Provider == nil {
		return fmt.Errorf("no provider")
	}

	if o.Monitor == nil {
		return fmt.Errorf("no monitor")
	}

	if o.PriorityFee != nil && o.PriorityFee.Sign() < 0 {
		return fmt.Errorf("negative priority fee %v", o.PriorityFee)
	}

	return nil
}

type ethkitChain struct {
	EthkitChainOptions

	baseFee, priorityFee *big.Int
	mu                   sync.Mutex
}

func NewEthkitChain(options EthkitChainOptions) (Chain, error) {
	if err := options.IsValid(); err != nil {
		return nil, err
	}

	if options.Logger == nil {
		options.Logger = slog.New(slog.DiscardHandler)
	}

	return &ethkitChain{EthkitChainOptions: options}, nil
}

func (c *ethkitChain) ChainID() *big.Int {
	return new(big.Int).Set(c.EthkitChainOptions.ChainID)
}

func (c *ethkitChain) LatestNonce(ctx context.Context, address common.Address) (uint64, error) {
	return c.Provider.NonceAt(ctx, address, nil)
}

func (c *ethkitChain) PendingNonce(ctx context.Context, address common.Address) (uint64, error) {
	return c.Provider.PendingNonceAt(ctx, address)
}

func (c *ethkitChain) Send(ctx context.Context, transaction *types.Transaction) error {
	return c.Provider.SendTransaction(ctx, transaction)
}

func (c *ethkitChain) BaseFee(ctx context.Context) (*big.Int, error) {
	block, err := c.Provider.BlockByNumber(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to get latest block: %w", err)
	}

	baseFee := block.BaseFee()
	if baseFee == nil {
		return nil, fmt.Errorf("no base fee")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.baseFee == nil || baseFee.Cmp(c.baseFee) != 0 {
		c.Logger.DebugContext(ctx, "base fee", slog.String("baseFee", baseFee.String()), slog.String("block", block.Number().String()))
		c.baseFee = new(big.Int).Set(baseFee)
	}

	return baseFee, nil
}

func (c *ethkitChain) PriorityFee(ctx context.Context) (*big.Int, error) {
	priorityFee := new(big.Int)
	if c.EthkitChainOptions.PriorityFee != nil {
		priorityFee.Set(c.EthkitChainOptions.PriorityFee)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.priorityFee == nil || priorityFee.Cmp(c.priorityFee) != 0 {
		c.Logger.DebugContext(ctx, "priority fee", slog.String("priorityFee", priorityFee.String()))
		c.priorityFee = new(big.Int).Set(priorityFee)
	}

	return priorityFee, nil
}

func (c *ethkitChain) Subscribe(ctx context.Context) (<-chan Diff, error) {
	diffs := make(chan Diff)

	go func() {
		defer close(diffs)

		subscription := c.Monitor.Subscribe()

		for {
			select {
			case <-ctx.Done():
				subscription.Unsubscribe()
				return

			case <-subscription.Done():
				return

			case blocks, ok := <-subscription.Blocks():
				if !ok {
					return
				}

				diff := Diff{
					Removed: map[common.Hash]struct{}{},
					Added:   map[common.Hash]struct{}{},
				}

				for _, block := range blocks {
					switch block.Event {
					case ethmonitor.Added:
						c.Logger.DebugContext(
							ctx,
							"block mined",
							slog.String("block", block.Hash().String()),
							slog.String("number", block.Number().String()),
							slog.Int("transactions", block.Transactions().Len()),
						)

						for _, transaction := range block.Transactions() {
							diff.Added[transaction.Hash()] = struct{}{}
						}

					case ethmonitor.Removed:
						c.Logger.DebugContext(
							ctx,
							"block reorged",
							slog.String("block", block.Hash().String()),
							slog.String("number", block.Number().String()),
							slog.Int("transactions", block.Transactions().Len()),
						)

						for _, transaction := range block.Transactions() {
							diff.Removed[transaction.Hash()] = struct{}{}
						}
					}
				}

				select {
				case <-ctx.Done():
					subscription.Unsubscribe()
					return

				case <-subscription.Done():
					return

				case diffs <- diff:
				}
			}
		}
	}()

	return diffs, nil
}
