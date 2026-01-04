package finalizer

import (
	"context"
	"sync"
	"time"

	"github.com/0xsequence/ethkit/go-ethereum/common"
	"github.com/0xsequence/ethkit/go-ethereum/core/types"
)

type Mempool[T any] interface {
	Nonce(ctx context.Context) (uint64, error)

	Commit(ctx context.Context, transaction *types.Transaction, metadata T) error

	Transactions(ctx context.Context, hashes map[common.Hash]struct{}) (map[common.Hash]*Transaction[T], error)
	PriciestTransactions(ctx context.Context, fromNonce uint64, before time.Time) (map[uint64]*Transaction[T], error)
}

type memoryMempool[T any] struct {
	transactions         map[common.Hash]*Transaction[T]
	priciestTransactions map[uint64]*timestampedTransaction[T]
	highestNonce         *uint64
	mu                   sync.RWMutex
}

type timestampedTransaction[T any] struct {
	*Transaction[T]

	timestamp time.Time
}

func NewMemoryMempool[T any]() Mempool[T] {
	return &memoryMempool[T]{
		transactions:         map[common.Hash]*Transaction[T]{},
		priciestTransactions: map[uint64]*timestampedTransaction[T]{},
	}
}

func (m *memoryMempool[T]) Nonce(ctx context.Context) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.highestNonce == nil {
		return 0, nil
	} else {
		return *m.highestNonce + 1, nil
	}
}

func (m *memoryMempool[T]) Commit(ctx context.Context, transaction *types.Transaction, metadata T) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	transaction_ := Transaction[T]{
		Transaction: transaction,
		Metadata:    metadata,
	}

	m.transactions[transaction.Hash()] = &transaction_

	previous := m.priciestTransactions[transaction.Nonce()]
	if previous == nil || transaction.GasFeeCapCmp(previous.Transaction.Transaction) > 0 && transaction.GasTipCapCmp(previous.Transaction.Transaction) > 0 {
		m.priciestTransactions[transaction.Nonce()] = &timestampedTransaction[T]{
			Transaction: &transaction_,
			timestamp:   time.Now(),
		}

		if m.highestNonce == nil || transaction.Nonce() > *m.highestNonce {
			m.highestNonce = new(uint64)
			*m.highestNonce = transaction.Nonce()
		}
	} else if previous.Hash() == transaction.Hash() {
		previous.timestamp = time.Now()
	}

	return nil
}

func (m *memoryMempool[T]) Transactions(ctx context.Context, hashes map[common.Hash]struct{}) (map[common.Hash]*Transaction[T], error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	transactions := make(map[common.Hash]*Transaction[T], len(hashes))
	for hash := range hashes {
		transaction := m.transactions[hash]
		if transaction != nil {
			transactions[hash] = transaction
		}
	}

	return transactions, nil
}

func (m *memoryMempool[T]) PriciestTransactions(ctx context.Context, fromNonce uint64, before time.Time) (map[uint64]*Transaction[T], error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var capacity uint64
	if m.highestNonce != nil && *m.highestNonce+1 > fromNonce {
		capacity = *m.highestNonce + 1 - fromNonce
	}

	transactions := make(map[uint64]*Transaction[T], capacity)
	for nonce := fromNonce; ; nonce++ {
		transaction := m.priciestTransactions[nonce]
		if transaction == nil || !transaction.timestamp.Before(before) {
			break
		}
		transactions[nonce] = transaction.Transaction
	}

	return transactions, nil
}
