package dispatcher

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTTLCacheGetOrLoadSingleFlight(t *testing.T) {
	cache := NewTTLCache[string, int](time.Second)

	var calls atomic.Int32
	start := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(20)

	for i := 0; i < 20; i++ {
		go func() {
			defer wg.Done()
			<-start

			value, err := cache.GetOrLoad(context.Background(), "k", func(_ context.Context, _ string) (int, error) {
				calls.Add(1)
				time.Sleep(20 * time.Millisecond)
				return 42, nil
			})
			if err != nil {
				t.Errorf("load error: %v", err)
				return
			}
			if value != 42 {
				t.Errorf("value mismatch: got %d", value)
			}
		}()
	}

	close(start)
	wg.Wait()

	if calls.Load() != 1 {
		t.Fatalf("expected loader to run once, got %d", calls.Load())
	}

	value, ok := cache.Get("k")
	if !ok || value != 42 {
		t.Fatalf("expected cached value 42, got %d, ok=%v", value, ok)
	}
}

func TestTTLCacheExpiresValue(t *testing.T) {
	cache := NewTTLCache[string, int](30 * time.Millisecond)
	cache.Set("a", 1)

	value, ok := cache.Get("a")
	if !ok || value != 1 {
		t.Fatalf("expected initial cache hit, got %d, ok=%v", value, ok)
	}

	time.Sleep(50 * time.Millisecond)
	_, ok = cache.Get("a")
	if ok {
		t.Fatalf("expected cache miss after ttl")
	}
}

func TestTTLCacheRejectsNilLoader(t *testing.T) {
	cache := NewTTLCache[string, int](time.Second)
	_, err := cache.GetOrLoad(context.Background(), "k", nil)
	if !errors.Is(err, ErrNilLoader) {
		t.Fatalf("expected ErrNilLoader, got %v", err)
	}
}
