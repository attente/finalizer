package finalizer

import (
	"context"
	"sync"
	"time"

	"github.com/0xsequence/ethkit/go-ethereum/common"
	"github.com/0xsequence/ethkit/go-ethereum/core/types"
)

type Mempool interface {
	Nonce(ctx context.Context) (uint64, error)

	Commit(ctx context.Context, transaction *types.Transaction) error

	Transactions(ctx context.Context, hashes map[common.Hash]struct{}) (map[common.Hash]*types.Transaction, error)
	PriciestTransactions(ctx context.Context, fromNonce uint64, before time.Time) (map[uint64]*types.Transaction, error)
}

type memoryMempool struct {
	transactions         map[common.Hash]*types.Transaction
	priciestTransactions map[uint64]*timestampedTransaction
	highestNonce         *uint64
	mu                   sync.RWMutex
}

type timestampedTransaction struct {
	*types.Transaction

	timestamp time.Time
}

func NewMemoryMempool() Mempool {
	return &memoryMempool{
		transactions:         map[common.Hash]*types.Transaction{},
		priciestTransactions: map[uint64]*timestampedTransaction{},
	}
}

func (m *memoryMempool) Nonce(ctx context.Context) (uint64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.highestNonce == nil {
		return 0, nil
	} else {
		return *m.highestNonce + 1, nil
	}
}

func (m *memoryMempool) Commit(ctx context.Context, transaction *types.Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.transactions[transaction.Hash()] = transaction

	previous := m.priciestTransactions[transaction.Nonce()]
	if previous == nil || transaction.GasFeeCapCmp(previous.Transaction) > 0 && transaction.GasTipCapCmp(previous.Transaction) > 0 {
		m.priciestTransactions[transaction.Nonce()] = &timestampedTransaction{
			Transaction: transaction,
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

func (m *memoryMempool) Transactions(ctx context.Context, hashes map[common.Hash]struct{}) (map[common.Hash]*types.Transaction, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	transactions := make(map[common.Hash]*types.Transaction, len(hashes))
	for hash := range hashes {
		transaction := m.transactions[hash]
		if transaction != nil {
			transactions[hash] = transaction
		}
	}

	return transactions, nil
}

func (m *memoryMempool) PriciestTransactions(ctx context.Context, fromNonce uint64, before time.Time) (map[uint64]*types.Transaction, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var capacity uint64
	if m.highestNonce != nil && *m.highestNonce+1 > fromNonce {
		capacity = *m.highestNonce + 1 - fromNonce
	}

	transactions := make(map[uint64]*types.Transaction, capacity)
	for nonce := fromNonce; ; nonce++ {
		transaction := m.priciestTransactions[nonce]
		if transaction == nil || !transaction.timestamp.Before(before) {
			break
		}
		transactions[nonce] = transaction.Transaction
	}

	return transactions, nil
}
