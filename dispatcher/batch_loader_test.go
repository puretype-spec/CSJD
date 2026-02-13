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

func TestBatchLoaderBatchesConcurrentLoads(t *testing.T) {
	var callCount atomic.Int32
	var batchMu sync.Mutex
	batchSizes := make([]int, 0)

	loader, err := NewBatchLoader[int, int](BatchLoaderConfig{MaxBatch: 16, MaxWait: 20 * time.Millisecond}, func(_ context.Context, keys []int) (map[int]int, error) {
		callCount.Add(1)

		batchMu.Lock()
		batchSizes = append(batchSizes, len(keys))
		batchMu.Unlock()

		out := make(map[int]int, len(keys))
		for _, key := range keys {
			out[key] = key * 10
		}

		return out, nil
	})
	if err != nil {
		t.Fatalf("new batch loader: %v", err)
	}
	defer loader.Close()

	start := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(8)

	results := make([]int, 8)
	for i := 0; i < 8; i++ {
		idx := i
		go func() {
			defer wg.Done()
			<-start

			value, loadErr := loader.Load(context.Background(), idx)
			if loadErr != nil {
				t.Errorf("load error: %v", loadErr)
				return
			}

			results[idx] = value
		}()
	}

	close(start)
	wg.Wait()

	for i := 0; i < 8; i++ {
		if results[i] != i*10 {
			t.Fatalf("unexpected value for key %d: %d", i, results[i])
		}
	}

	if callCount.Load() > 2 {
		t.Fatalf("expected <=2 fetch calls, got %d", callCount.Load())
	}

	batchMu.Lock()
	defer batchMu.Unlock()
	maxBatch := 0
	for _, batchSize := range batchSizes {
		if batchSize > maxBatch {
			maxBatch = batchSize
		}
	}
	if maxBatch < 2 {
		t.Fatalf("expected at least one batched request, sizes=%v", batchSizes)
	}
}

func TestBatchLoaderMissingKey(t *testing.T) {
	loader, err := NewBatchLoader[int, int](BatchLoaderConfig{MaxBatch: 4, MaxWait: 10 * time.Millisecond}, func(_ context.Context, _ []int) (map[int]int, error) {
		return map[int]int{}, nil
	})
	if err != nil {
		t.Fatalf("new batch loader: %v", err)
	}
	defer loader.Close()

	_, err = loader.Load(context.Background(), 7)
	if !errors.Is(err, ErrMissingBatchResult) {
		t.Fatalf("expected ErrMissingBatchResult, got %v", err)
	}
}

func TestBatchLoaderClose(t *testing.T) {
	loader, err := NewBatchLoader[int, int](BatchLoaderConfig{MaxBatch: 4, MaxWait: 10 * time.Millisecond}, func(_ context.Context, keys []int) (map[int]int, error) {
		out := make(map[int]int, len(keys))
		for _, key := range keys {
			out[key] = key
		}
		return out, nil
	})
	if err != nil {
		t.Fatalf("new batch loader: %v", err)
	}

	loader.Close()

	_, err = loader.Load(context.Background(), 1)
	if !errors.Is(err, ErrBatchLoaderClosed) {
		t.Fatalf("expected ErrBatchLoaderClosed, got %v", err)
	}
}

func TestBatchLoaderCloseCancelsInFlightFetch(t *testing.T) {
	started := make(chan struct{})

	loader, err := NewBatchLoader[int, int](BatchLoaderConfig{MaxBatch: 1, MaxWait: 5 * time.Millisecond}, func(ctx context.Context, _ []int) (map[int]int, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	if err != nil {
		t.Fatalf("new batch loader: %v", err)
	}

	loadDone := make(chan error, 1)
	go func() {
		_, loadErr := loader.Load(context.Background(), 42)
		loadDone <- loadErr
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("fetch was not started")
	}

	closeDone := make(chan struct{})
	go func() {
		loader.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("loader.Close() timed out")
	}

	select {
	case loadErr := <-loadDone:
		if !errors.Is(loadErr, ErrBatchLoaderClosed) {
			t.Fatalf("expected ErrBatchLoaderClosed, got %v", loadErr)
		}
	case <-time.After(time.Second):
		t.Fatal("load call did not return")
	}
}

func TestBatchLoaderCloseReturnsWhenFetchIgnoresContext(t *testing.T) {
	started := make(chan struct{}, 1)
	releaseFetch := make(chan struct{})

	loader, err := NewBatchLoader[int, int](BatchLoaderConfig{MaxBatch: 1, MaxWait: 5 * time.Millisecond}, func(_ context.Context, _ []int) (map[int]int, error) {
		select {
		case started <- struct{}{}:
		default:
		}

		<-releaseFetch

		return map[int]int{42: 42}, nil
	})
	if err != nil {
		t.Fatalf("new batch loader: %v", err)
	}

	loadDone := make(chan error, 1)
	go func() {
		_, loadErr := loader.Load(context.Background(), 42)
		loadDone <- loadErr
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("fetch was not started")
	}

	start := time.Now()
	loader.Close()
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("loader.Close() should not block on non-cooperative fetch")
	}

	select {
	case loadErr := <-loadDone:
		if !errors.Is(loadErr, ErrBatchLoaderClosed) {
			t.Fatalf("expected ErrBatchLoaderClosed, got %v", loadErr)
		}
	case <-time.After(time.Second):
		t.Fatal("load call did not return")
	}

	close(releaseFetch)

	select {
	case <-loader.runDone:
	case <-time.After(time.Second):
		t.Fatal("run loop did not stop after fetch was released")
	}
}

func TestBatchLoaderFetchPanicReturnsErrorAndKeepsLoopAlive(t *testing.T) {
	var calls atomic.Int32
	loader, err := NewBatchLoader[int, int](BatchLoaderConfig{MaxBatch: 1, MaxWait: 5 * time.Millisecond}, func(_ context.Context, keys []int) (map[int]int, error) {
		if calls.Add(1) == 1 {
			panic("fetch panic")
		}

		out := make(map[int]int, len(keys))
		for _, key := range keys {
			out[key] = key * 10
		}

		return out, nil
	})
	if err != nil {
		t.Fatalf("new batch loader: %v", err)
	}
	defer loader.Close()

	_, err = loader.Load(context.Background(), 1)
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("expected panic-based error, got %v", err)
	}

	value, err := loader.Load(context.Background(), 2)
	if err != nil {
		t.Fatalf("expected loader to keep running after panic, got %v", err)
	}
	if value != 20 {
		t.Fatalf("unexpected value: %d", value)
	}
}
