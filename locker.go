package aggd

import (
	"context"
	"sync"
)

type Locker interface {
	Lock(ctx context.Context, key string)
	Unlock(ctx context.Context, key string)
}

func NewMemoryLocker() Locker {
	return &memoryLocker{
		locks: new(sync.Map),
	}
}

type memoryLocker struct {
	// locks uses a sync.Map, from the stdlib docs:
	//
	// The Map type is optimized for two common use cases: (1)
	// when the entry for a given key is only ever written once
	// but read many times, as in caches that only grow, or (2)
	// when multiple goroutines read, write, and overwrite entries
	// for disjoint sets of keys. In these two cases, use of a Map
	// may significantly reduce lock contention compared to a Go
	// map paired with a separate Mutex or RWMutex.
	//
	// this is the first case I think...
	locks *sync.Map
}

func (m *memoryLocker) Lock(_ context.Context, key string) {
	l, _ := m.locks.LoadOrStore(key, new(sync.Mutex))

	l.(*sync.Mutex).Lock()
}

func (m *memoryLocker) Unlock(_ context.Context, key string) {
	l, _ := m.locks.Load(key)

	l.(*sync.Mutex).Unlock()
}
