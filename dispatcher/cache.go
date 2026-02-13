package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrNilLoader is returned when GetOrLoad receives a nil loader.
var ErrNilLoader = errors.New("loader is nil")

type cacheEntry[V any] struct {
	value     V
	expiresAt time.Time
}

type cacheCall[V any] struct {
	done  chan struct{}
	value V
	err   error
}

// TTLCache is a concurrency-safe in-memory cache with stampede protection.
type TTLCache[K comparable, V any] struct {
	ttl time.Duration

	mu       sync.RWMutex
	items    map[K]cacheEntry[V]
	inFlight map[K]*cacheCall[V]
}

// NewTTLCache creates a typed cache. ttl<=0 means no expiration.
func NewTTLCache[K comparable, V any](ttl time.Duration) *TTLCache[K, V] {
	return &TTLCache[K, V]{
		ttl:      ttl,
		items:    make(map[K]cacheEntry[V]),
		inFlight: make(map[K]*cacheCall[V]),
	}
}

// Get returns cached value when present and not expired.
func (c *TTLCache[K, V]) Get(key K) (V, bool) {
	now := time.Now()

	c.mu.RLock()
	entry, ok := c.items[key]
	if !ok {
		c.mu.RUnlock()
		var zero V
		return zero, false
	}
	if c.isExpired(entry, now) {
		c.mu.RUnlock()
		c.mu.Lock()
		entry, ok = c.items[key]
		if ok && c.isExpired(entry, now) {
			delete(c.items, key)
		}
		c.mu.Unlock()
		var zero V
		return zero, false
	}
	c.mu.RUnlock()

	return entry.value, true
}

// Set stores one value.
func (c *TTLCache[K, V]) Set(key K, value V) {
	now := time.Now()

	c.mu.Lock()
	c.setLocked(key, value, now)
	c.mu.Unlock()
}

// Delete removes one key.
func (c *TTLCache[K, V]) Delete(key K) {
	c.mu.Lock()
	delete(c.items, key)
	c.mu.Unlock()
}

// Cleanup removes all expired entries.
func (c *TTLCache[K, V]) Cleanup() {
	if c.ttl <= 0 {
		return
	}

	now := time.Now()
	c.mu.Lock()
	for key, entry := range c.items {
		if c.isExpired(entry, now) {
			delete(c.items, key)
		}
	}
	c.mu.Unlock()
}

// Len returns current map size (expired items are cleaned before counting when ttl>0).
func (c *TTLCache[K, V]) Len() int {
	c.Cleanup()

	c.mu.RLock()
	length := len(c.items)
	c.mu.RUnlock()

	return length
}

// GetOrLoad returns cached value or loads once for concurrent callers of the same key.
func (c *TTLCache[K, V]) GetOrLoad(ctx context.Context, key K, loader func(context.Context, K) (V, error)) (V, error) {
	if loader == nil {
		var zero V
		return zero, ErrNilLoader
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if value, ok := c.Get(key); ok {
		return value, nil
	}

	c.mu.Lock()
	now := time.Now()
	entry, ok := c.items[key]
	if ok && !c.isExpired(entry, now) {
		c.mu.Unlock()
		return entry.value, nil
	}

	inFlight, exists := c.inFlight[key]
	if exists {
		c.mu.Unlock()
		select {
		case <-inFlight.done:
			return inFlight.value, inFlight.err
		case <-ctx.Done():
			var zero V
			return zero, ctx.Err()
		}
	}

	call := &cacheCall[V]{done: make(chan struct{})}
	c.inFlight[key] = call
	c.mu.Unlock()

	var (
		value V
		err   error
	)

	func() {
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}

			err = fmt.Errorf("cache loader panic: %v", recovered)
		}()

		value, err = loader(ctx, key)
	}()

	c.mu.Lock()
	if err == nil {
		c.setLocked(key, value, time.Now())
	}
	delete(c.inFlight, key)
	call.value = value
	call.err = err
	close(call.done)
	c.mu.Unlock()

	return value, err
}

func (c *TTLCache[K, V]) setLocked(key K, value V, now time.Time) {
	entry := cacheEntry[V]{value: value}
	if c.ttl > 0 {
		entry.expiresAt = now.Add(c.ttl)
	}
	c.items[key] = entry
}

func (c *TTLCache[K, V]) isExpired(entry cacheEntry[V], now time.Time) bool {
	if c.ttl <= 0 {
		return false
	}

	return !entry.expiresAt.After(now)
}
