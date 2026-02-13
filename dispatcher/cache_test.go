package dispatcher

import (
	"context"
	"errors"
	"strings"
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

func TestTTLCacheGetOrLoadLoaderPanicDoesNotPoisonKey(t *testing.T) {
	cache := NewTTLCache[string, int](time.Second)

	_, err := cache.GetOrLoad(context.Background(), "panic", func(_ context.Context, _ string) (int, error) {
		panic("boom")
	})
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("expected panic-based error, got %v", err)
	}

	value, err := cache.GetOrLoad(context.Background(), "panic", func(_ context.Context, _ string) (int, error) {
		return 7, nil
	})
	if err != nil {
		t.Fatalf("expected successful reload after panic, got %v", err)
	}
	if value != 7 {
		t.Fatalf("unexpected value after reload: %d", value)
	}
}

func TestTTLCacheGetOrLoadLoaderPanicUnblocksWaiters(t *testing.T) {
	cache := NewTTLCache[string, int](time.Second)
	started := make(chan struct{})
	release := make(chan struct{})

	leaderDone := make(chan error, 1)
	go func() {
		_, err := cache.GetOrLoad(context.Background(), "k", func(_ context.Context, _ string) (int, error) {
			close(started)
			<-release
			panic("panic-leader")
		})
		leaderDone <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("leader loader was not started")
	}

	type waiterResult struct {
		value int
		err   error
	}

	waiterDone := make(chan waiterResult, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		value, err := cache.GetOrLoad(ctx, "k", func(_ context.Context, _ string) (int, error) {
			return 999, nil
		})
		waiterDone <- waiterResult{value: value, err: err}
	}()

	close(release)

	select {
	case err := <-leaderDone:
		if err == nil || !strings.Contains(err.Error(), "panic") {
			t.Fatalf("expected panic-based leader error, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("leader did not finish")
	}

	select {
	case result := <-waiterDone:
		if errors.Is(result.err, context.DeadlineExceeded) {
			t.Fatalf("waiter timed out waiting for in-flight completion")
		}
		if result.err == nil {
			if result.value != 999 {
				t.Fatalf("unexpected waiter value: %d", result.value)
			}
			return
		}
		if !strings.Contains(result.err.Error(), "panic") {
			t.Fatalf("unexpected waiter error: %v", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter did not finish")
	}
}

func TestTTLCacheGetOrLoadNotTiedToLeaderContextCancellation(t *testing.T) {
	cache := NewTTLCache[string, int](time.Second)

	leaderCtx, cancelLeader := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelLeader()

	leaderStarted := make(chan struct{})
	leaderDone := make(chan error, 1)
	go func() {
		_, err := cache.GetOrLoad(leaderCtx, "k", func(ctx context.Context, _ string) (int, error) {
			close(leaderStarted)
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(60 * time.Millisecond):
				return 42, nil
			}
		})
		leaderDone <- err
	}()

	select {
	case <-leaderStarted:
	case <-time.After(time.Second):
		t.Fatal("leader loader was not started")
	}

	followerDone := make(chan struct {
		value int
		err   error
	}, 1)
	go func() {
		value, err := cache.GetOrLoad(context.Background(), "k", func(_ context.Context, _ string) (int, error) {
			return 7, nil
		})
		followerDone <- struct {
			value int
			err   error
		}{value: value, err: err}
	}()

	select {
	case result := <-followerDone:
		if result.err != nil {
			t.Fatalf("follower should not fail due to leader timeout: %v", result.err)
		}
		if result.value != 42 {
			t.Fatalf("follower value mismatch: got %d", result.value)
		}
	case <-time.After(time.Second):
		t.Fatal("follower did not finish")
	}

	select {
	case err := <-leaderDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("leader should respect its own timeout, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("leader did not finish")
	}
}
