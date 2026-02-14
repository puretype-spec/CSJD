package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDistributedDispatcherStartFailsWhenEnsureGroupFails(t *testing.T) {
	backend := newFakeDistributedBackend()
	backend.ensureGroupErr = errors.New("group create failed")

	d, err := NewDistributedDispatcher(DistributedConfig{
		Workers: 1,
		Stream:  "jobs",
		Group:   "group-a",
	}, backend)
	if err != nil {
		t.Fatalf("new distributed dispatcher: %v", err)
	}

	if err = d.Start(); err == nil {
		t.Fatal("expected start to fail")
	}
}

func TestDistributedDispatcherSubmitAndProcess(t *testing.T) {
	backend := newFakeDistributedBackend()
	d, err := NewDistributedDispatcher(DistributedConfig{
		Workers:       1,
		Stream:        "jobs",
		Group:         "group-a",
		BatchSize:     8,
		Block:         10 * time.Millisecond,
		ClaimInterval: 100 * time.Millisecond,
		ClaimMinIdle:  200 * time.Millisecond,
		RetryJitter:   0,
	}, backend)
	if err != nil {
		t.Fatalf("new distributed dispatcher: %v", err)
	}

	var handled atomic.Int32
	err = d.RegisterHandler("email", func(_ context.Context, _ Job) error {
		handled.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		if stopErr := d.Stop(context.Background()); stopErr != nil {
			t.Fatalf("stop: %v", stopErr)
		}
	}()

	if err = d.Submit(Job{ID: "d-1", Type: "email"}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return handled.Load() == 1 && d.Metrics().Succeeded == 1
	})
}

func TestDistributedDispatcherRetriesUntilSuccess(t *testing.T) {
	backend := newFakeDistributedBackend()
	d, err := NewDistributedDispatcher(DistributedConfig{
		Workers:            1,
		Stream:             "jobs",
		Group:              "group-a",
		BatchSize:          4,
		Block:              10 * time.Millisecond,
		ClaimInterval:      100 * time.Millisecond,
		ClaimMinIdle:       200 * time.Millisecond,
		RetryJitter:        0,
		RetryMinDelay:      5 * time.Millisecond,
		RetryMaxDelay:      10 * time.Millisecond,
		DefaultMaxAttempts: 3,
	}, backend)
	if err != nil {
		t.Fatalf("new distributed dispatcher: %v", err)
	}

	var calls atomic.Int32
	err = d.RegisterHandler("retry", func(_ context.Context, _ Job) error {
		if calls.Add(1) < 2 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		if stopErr := d.Stop(context.Background()); stopErr != nil {
			t.Fatalf("stop: %v", stopErr)
		}
	}()

	if err = d.Submit(Job{ID: "r-1", Type: "retry"}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return d.Metrics().Succeeded == 1
	})

	metrics := d.Metrics()
	if metrics.Retried < 1 {
		t.Fatalf("expected retry count >=1, got %d", metrics.Retried)
	}
}

func TestDistributedDispatcherQueueFull(t *testing.T) {
	backend := newFakeDistributedBackend()
	d, err := NewDistributedDispatcher(DistributedConfig{
		Workers:        1,
		Stream:         "jobs",
		Group:          "group-a",
		BatchSize:      4,
		Block:          10 * time.Millisecond,
		ClaimInterval:  100 * time.Millisecond,
		ClaimMinIdle:   200 * time.Millisecond,
		RetryJitter:    0,
		MaxPendingJobs: 1,
	}, backend)
	if err != nil {
		t.Fatalf("new distributed dispatcher: %v", err)
	}

	block := make(chan struct{})
	err = d.RegisterHandler("slow", func(_ context.Context, _ Job) error {
		<-block
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err = d.Submit(Job{ID: "q-1", Type: "slow"}); err != nil {
		t.Fatalf("submit first: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		length, lenErr := backend.Len(context.Background())
		return lenErr == nil && length >= 1
	})

	if err = d.Submit(Job{ID: "q-2", Type: "slow"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	close(block)
	if err = d.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestDistributedDispatcherTimeoutDetachBudgetAllowsProgress(t *testing.T) {
	backend := newFakeDistributedBackend()
	d, err := NewDistributedDispatcher(DistributedConfig{
		Workers:             1,
		Stream:              "jobs",
		Group:               "group-a",
		BatchSize:           4,
		Block:               10 * time.Millisecond,
		ClaimInterval:       100 * time.Millisecond,
		ClaimMinIdle:        200 * time.Millisecond,
		RetryJitter:         0,
		DefaultJobTimeout:   40 * time.Millisecond,
		MaxDetachedHandlers: 1,
	}, backend)
	if err != nil {
		t.Fatalf("new distributed dispatcher: %v", err)
	}

	var active atomic.Int32
	var maxActive atomic.Int32
	err = d.RegisterHandler("slow", func(_ context.Context, _ Job) error {
		current := active.Add(1)
		for {
			observed := maxActive.Load()
			if current <= observed {
				break
			}
			if maxActive.CompareAndSwap(observed, current) {
				break
			}
		}

		time.Sleep(200 * time.Millisecond)
		active.Add(-1)
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	if err = d.Submit(Job{ID: "t-1", Type: "slow", MaxAttempts: 1}); err != nil {
		t.Fatalf("submit t-1: %v", err)
	}
	if err = d.Submit(Job{ID: "t-2", Type: "slow", MaxAttempts: 1}); err != nil {
		t.Fatalf("submit t-2: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return d.Metrics().Failed == 2
	})

	metrics := d.Metrics()
	if metrics.Detached < 1 {
		t.Fatalf("expected detached>=1, got %d", metrics.Detached)
	}
	if maxActive.Load() < 2 {
		t.Fatalf("expected overlap after detach, maxActive=%d", maxActive.Load())
	}

	if err = d.Stop(context.Background()); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

type fakeDistributedBackend struct {
	mu sync.Mutex

	ensureGroupErr error

	queue   []distributedMessage
	pending map[string]distributedMessage
	nextID  uint64

	notify chan struct{}
	closed bool
}

func newFakeDistributedBackend() *fakeDistributedBackend {
	return &fakeDistributedBackend{
		queue:   make([]distributedMessage, 0),
		pending: make(map[string]distributedMessage),
		notify:  make(chan struct{}, 1),
	}
}

func (b *fakeDistributedBackend) EnsureGroup(_ context.Context) error {
	return b.ensureGroupErr
}

func (b *fakeDistributedBackend) Add(_ context.Context, body []byte) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return "", ErrDispatcherClosed
	}

	b.nextID++
	id := fmt.Sprintf("id-%d", b.nextID)
	b.queue = append(b.queue, distributedMessage{ID: id, Body: append([]byte(nil), body...)})
	select {
	case b.notify <- struct{}{}:
	default:
	}

	return id, nil
}

func (b *fakeDistributedBackend) ReadGroup(ctx context.Context, count int64, block time.Duration) ([]distributedMessage, error) {
	deadline := time.NewTimer(block)
	defer deadline.Stop()

	for {
		b.mu.Lock()
		if b.closed {
			b.mu.Unlock()
			return nil, context.Canceled
		}
		if len(b.queue) > 0 {
			n := int(count)
			if n > len(b.queue) {
				n = len(b.queue)
			}
			out := make([]distributedMessage, 0, n)
			for i := 0; i < n; i++ {
				message := b.queue[0]
				b.queue = b.queue[1:]
				b.pending[message.ID] = message
				out = append(out, message)
			}
			b.mu.Unlock()
			return out, nil
		}
		b.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, nil
		case <-b.notify:
		}
	}
}

func (b *fakeDistributedBackend) AutoClaim(_ context.Context, _ time.Duration, start string, _ int64) ([]distributedMessage, string, error) {
	return nil, start, nil
}

func (b *fakeDistributedBackend) Ack(_ context.Context, ids ...string) error {
	b.mu.Lock()
	for _, id := range ids {
		delete(b.pending, id)
	}
	b.mu.Unlock()
	return nil
}

func (b *fakeDistributedBackend) Del(_ context.Context, ids ...string) error {
	b.mu.Lock()
	for _, id := range ids {
		delete(b.pending, id)
	}
	b.mu.Unlock()
	return nil
}

func (b *fakeDistributedBackend) Len(_ context.Context) (int64, error) {
	b.mu.Lock()
	length := len(b.queue) + len(b.pending)
	b.mu.Unlock()

	return int64(length), nil
}

func (b *fakeDistributedBackend) Close() error {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	select {
	case b.notify <- struct{}{}:
	default:
	}
	return nil
}
