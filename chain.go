package finalizer

import (
	"context"
	"fmt"
	"math/big"

	"github.com/0xsequence/ethkit/ethgas"
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

	GasPrice(ctx context.Context) (*big.Int, error)
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
	GasGauge *ethgas.GasGauge

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

	if o.GasGauge == nil {
		return fmt.Errorf("no gas gauge")
	}

	if o.PriorityFee != nil && o.PriorityFee.Sign() < 0 {
		return fmt.Errorf("negative priority fee %v", o.PriorityFee)
	}

	return nil
}

type ethkitChain struct {
	EthkitChainOptions
}

func NewEthkitChain(options EthkitChainOptions) (Chain, error) {
	if err := options.IsValid(); err != nil {
		return nil, err
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

func (c *ethkitChain) GasPrice(ctx context.Context) (*big.Int, error) {
	gasPrice := c.GasGauge.SuggestedGasPrice().InstantWei
	if gasPrice == nil {
		return nil, fmt.Errorf("no gas price")
	}

	return new(big.Int).Set(gasPrice), nil
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

	return baseFee, nil
}

func (c *ethkitChain) PriorityFee(ctx context.Context) (*big.Int, error) {
	if c.EthkitChainOptions.PriorityFee != nil {
		return new(big.Int).Set(c.EthkitChainOptions.PriorityFee), nil
	} else {
		return new(big.Int), nil
	}
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
						for _, transaction := range block.Transactions() {
							diff.Added[transaction.Hash()] = struct{}{}
						}

					case ethmonitor.Removed:
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
