package dispatcher

import (
	"context"
	"encoding/json"
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

func TestDistributedDispatcherRetryBypassesQueueCap(t *testing.T) {
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
		MaxPendingJobs:     1,
	}, backend)
	if err != nil {
		t.Fatalf("new distributed dispatcher: %v", err)
	}

	var calls atomic.Int32
	err = d.RegisterHandler("retry", func(_ context.Context, _ Job) error {
		if calls.Add(1) == 1 {
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

	if err = d.Submit(Job{ID: "retry-cap-1", Type: "retry"}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return d.Metrics().Succeeded == 1
	})
	metrics := d.Metrics()
	if metrics.Retried != 1 {
		t.Fatalf("expected retried=1 under queue cap, got %d", metrics.Retried)
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

func TestDistributedDispatcherQueueCapIsAtomicAcrossConcurrentSubmit(t *testing.T) {
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
	defer func() {
		close(block)
		if stopErr := d.Stop(context.Background()); stopErr != nil {
			t.Fatalf("stop: %v", stopErr)
		}
	}()

	start := make(chan struct{})
	resultCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		jobID := fmt.Sprintf("atomic-%d", i)
		go func(id string) {
			<-start
			resultCh <- d.Submit(Job{ID: id, Type: "slow"})
		}(jobID)
	}
	close(start)

	results := []error{<-resultCh, <-resultCh}
	var accepted int
	var queueFull int
	for _, submitErr := range results {
		if submitErr == nil {
			accepted++
			continue
		}
		if errors.Is(submitErr, ErrQueueFull) {
			queueFull++
			continue
		}
		t.Fatalf("unexpected submit error: %v", submitErr)
	}
	if accepted != 1 || queueFull != 1 {
		t.Fatalf("expected exactly 1 accepted and 1 queue full, got accepted=%d queueFull=%d", accepted, queueFull)
	}
}

func TestDistributedDispatcherSkipsClaimedDuplicateWhileInFlight(t *testing.T) {
	backend := newFakeDistributedBackend()
	d, err := NewDistributedDispatcher(DistributedConfig{
		Workers:       1,
		Stream:        "jobs",
		Group:         "group-a",
		BatchSize:     4,
		Block:         10 * time.Millisecond,
		ClaimInterval: 10 * time.Millisecond,
		ClaimMinIdle:  20 * time.Millisecond,
		RetryJitter:   0,
	}, backend)
	if err != nil {
		t.Fatalf("new distributed dispatcher: %v", err)
	}

	block := make(chan struct{})
	var calls atomic.Int32
	err = d.RegisterHandler("slow", func(_ context.Context, _ Job) error {
		calls.Add(1)
		<-block
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

	if err = d.Submit(Job{ID: "claim-1", Type: "slow"}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitForCondition(t, time.Second, func() bool {
		return calls.Load() == 1
	})

	pendingMessage, ok := backend.firstPendingMessage()
	if !ok {
		t.Fatal("expected one pending message")
	}
	backend.setAutoClaimMessages(pendingMessage)
	time.Sleep(120 * time.Millisecond)

	close(block)

	waitForCondition(t, time.Second, func() bool {
		return d.Metrics().Succeeded >= 1
	})
	time.Sleep(120 * time.Millisecond)

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected one handler call, got %d", got)
	}
	if got := d.Metrics().Succeeded; got != 1 {
		t.Fatalf("expected succeeded=1, got %d", got)
	}
}

func TestDistributedDispatcherTouchesInFlightMessage(t *testing.T) {
	backend := newFakeDistributedBackend()
	d, err := NewDistributedDispatcher(DistributedConfig{
		Workers:       1,
		Stream:        "jobs",
		Group:         "group-a",
		BatchSize:     4,
		Block:         10 * time.Millisecond,
		ClaimInterval: 500 * time.Millisecond,
		ClaimMinIdle:  40 * time.Millisecond,
		RetryJitter:   0,
	}, backend)
	if err != nil {
		t.Fatalf("new distributed dispatcher: %v", err)
	}

	err = d.RegisterHandler("slow", func(_ context.Context, _ Job) error {
		time.Sleep(220 * time.Millisecond)
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

	if err = d.Submit(Job{ID: "touch-1", Type: "slow", MaxAttempts: 1}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return d.Metrics().Succeeded == 1
	})

	if touches := backend.totalTouches(); touches == 0 {
		t.Fatal("expected at least one heartbeat touch")
	}
}

func TestDistributedDispatcherPublishesDeadLetterOnTerminalFailure(t *testing.T) {
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
		DefaultMaxAttempts: 1,
	}, backend)
	if err != nil {
		t.Fatalf("new distributed dispatcher: %v", err)
	}

	err = d.RegisterHandler("fail", func(_ context.Context, _ Job) error {
		return errors.New("boom")
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

	if err = d.Submit(Job{ID: "dlq-1", Type: "fail"}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return d.Metrics().Failed == 1
	})

	body, ok := backend.deadLetterBody(0)
	if !ok {
		t.Fatal("expected one dead letter message")
	}

	var payload distributedDeadLetter
	if err = json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("unmarshal dead letter: %v", err)
	}

	if payload.MessageID == "" {
		t.Fatal("dead letter message id should not be empty")
	}
	if payload.Reason != "max_attempts_exhausted" {
		t.Fatalf("unexpected dead letter reason: %s", payload.Reason)
	}
	if payload.Error != "boom" {
		t.Fatalf("unexpected dead letter error: %s", payload.Error)
	}
	if payload.Job == nil || payload.Job.ID != "dlq-1" {
		t.Fatalf("unexpected dead letter job payload: %#v", payload.Job)
	}
	if payload.Attempt != 1 {
		t.Fatalf("expected dead letter attempt=1, got %d", payload.Attempt)
	}
}

func TestDistributedDispatcherPublishesDeadLetterOnDecodeFailure(t *testing.T) {
	backend := newFakeDistributedBackend()
	messageID := backend.enqueueRawMessage([]byte("not-json"))

	d, err := NewDistributedDispatcher(DistributedConfig{
		Workers:       1,
		Stream:        "jobs",
		Group:         "group-a",
		BatchSize:     4,
		Block:         10 * time.Millisecond,
		ClaimInterval: 100 * time.Millisecond,
		ClaimMinIdle:  200 * time.Millisecond,
		RetryJitter:   0,
	}, backend)
	if err != nil {
		t.Fatalf("new distributed dispatcher: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		if stopErr := d.Stop(context.Background()); stopErr != nil {
			t.Fatalf("stop: %v", stopErr)
		}
	}()

	waitForCondition(t, 2*time.Second, func() bool {
		return d.Metrics().Failed == 1
	})

	body, ok := backend.deadLetterBody(0)
	if !ok {
		t.Fatal("expected one dead letter message")
	}

	var payload distributedDeadLetter
	if err = json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("unmarshal dead letter: %v", err)
	}

	if payload.MessageID != messageID {
		t.Fatalf("unexpected dead letter message id: %s", payload.MessageID)
	}
	if payload.Reason != "decode_error" {
		t.Fatalf("unexpected dead letter reason: %s", payload.Reason)
	}
	if payload.Job != nil {
		t.Fatalf("decode failure should not include parsed job: %#v", payload.Job)
	}
}

func TestDistributedDispatcherFinalizeRetriesOnAckDelFailures(t *testing.T) {
	backend := newFakeDistributedBackend()
	backend.ackFailCount = 1
	backend.delFailCount = 1

	d, err := NewDistributedDispatcher(DistributedConfig{
		Workers:       1,
		Stream:        "jobs",
		Group:         "group-a",
		BatchSize:     4,
		Block:         10 * time.Millisecond,
		ClaimInterval: 100 * time.Millisecond,
		ClaimMinIdle:  200 * time.Millisecond,
		RetryJitter:   0,
	}, backend)
	if err != nil {
		t.Fatalf("new distributed dispatcher: %v", err)
	}

	err = d.RegisterHandler("ok", func(_ context.Context, _ Job) error {
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

	if err = d.Submit(Job{ID: "finalize-1", Type: "ok"}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return d.Metrics().Succeeded == 1
	})
	if got := backend.pendingLen(); got != 0 {
		t.Fatalf("expected pending cleared after finalize retries, got %d", got)
	}
	if got := d.Metrics().FinalizeErrors; got != 0 {
		t.Fatalf("expected finalize_errors=0, got %d", got)
	}
}

func TestDistributedDispatcherDeadLetterFailureIsObservable(t *testing.T) {
	backend := newFakeDistributedBackend()
	backend.deadLetterFailCount = 1

	d, err := NewDistributedDispatcher(DistributedConfig{
		Workers:            1,
		Stream:             "jobs",
		Group:              "group-a",
		BatchSize:          4,
		Block:              10 * time.Millisecond,
		ClaimInterval:      100 * time.Millisecond,
		ClaimMinIdle:       200 * time.Millisecond,
		RetryJitter:        0,
		DefaultMaxAttempts: 1,
	}, backend)
	if err != nil {
		t.Fatalf("new distributed dispatcher: %v", err)
	}

	err = d.RegisterHandler("fail", func(_ context.Context, _ Job) error {
		return errors.New("boom")
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

	if err = d.Submit(Job{ID: "dlq-fail-1", Type: "fail"}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return d.Metrics().Processed >= 1
	})
	metrics := d.Metrics()
	if metrics.DeadLetterErrors < 1 {
		t.Fatalf("expected dead letter errors >=1, got %d", metrics.DeadLetterErrors)
	}
	if metrics.Failed != 0 {
		t.Fatalf("expected failed=0 when dlq publish fails, got %d", metrics.Failed)
	}
	if got := backend.pendingLen(); got == 0 {
		t.Fatal("expected message to remain pending when dlq publish fails")
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

	queue               []distributedMessage
	pending             map[string]distributedMessage
	autoClaimQueue      []distributedMessage
	deadLetters         [][]byte
	touchCount          map[string]int
	nextID              uint64
	ackFailCount        int
	delFailCount        int
	deadLetterFailCount int

	notify chan struct{}
	closed bool
}

func newFakeDistributedBackend() *fakeDistributedBackend {
	return &fakeDistributedBackend{
		queue:       make([]distributedMessage, 0),
		pending:     make(map[string]distributedMessage),
		deadLetters: make([][]byte, 0),
		touchCount:  make(map[string]int),
		notify:      make(chan struct{}, 1),
	}
}

func (b *fakeDistributedBackend) EnsureGroup(_ context.Context) error {
	return b.ensureGroupErr
}

func (b *fakeDistributedBackend) Add(_ context.Context, body []byte, maxPending int) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return "", ErrDispatcherClosed
	}
	if maxPending > 0 && len(b.queue)+len(b.pending) >= maxPending {
		return "", ErrQueueFull
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
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.autoClaimQueue) == 0 {
		return nil, start, nil
	}

	out := make([]distributedMessage, len(b.autoClaimQueue))
	copy(out, b.autoClaimQueue)
	b.autoClaimQueue = nil

	return out, start, nil
}

func (b *fakeDistributedBackend) Ack(_ context.Context, ids ...string) error {
	b.mu.Lock()
	if b.ackFailCount > 0 {
		b.ackFailCount--
		b.mu.Unlock()
		return errors.New("injected ack failure")
	}
	for _, id := range ids {
		delete(b.pending, id)
	}
	b.mu.Unlock()
	return nil
}

func (b *fakeDistributedBackend) Touch(_ context.Context, id string) error {
	if id == "" {
		return nil
	}

	b.mu.Lock()
	b.touchCount[id]++
	b.mu.Unlock()
	return nil
}

func (b *fakeDistributedBackend) Del(_ context.Context, ids ...string) error {
	b.mu.Lock()
	if b.delFailCount > 0 {
		b.delFailCount--
		b.mu.Unlock()
		return errors.New("injected delete failure")
	}
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

func (b *fakeDistributedBackend) AddDeadLetter(_ context.Context, body []byte) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.deadLetterFailCount > 0 {
		b.deadLetterFailCount--
		return "", errors.New("injected dead letter failure")
	}

	b.deadLetters = append(b.deadLetters, append([]byte(nil), body...))
	b.nextID++

	return fmt.Sprintf("dlq-%d", b.nextID), nil
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

func (b *fakeDistributedBackend) setAutoClaimMessages(messages ...distributedMessage) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.autoClaimQueue = append(make([]distributedMessage, 0, len(messages)), messages...)
}

func (b *fakeDistributedBackend) firstPendingMessage() (distributedMessage, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, message := range b.pending {
		return message, true
	}

	return distributedMessage{}, false
}

func (b *fakeDistributedBackend) totalTouches() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	total := 0
	for _, count := range b.touchCount {
		total += count
	}

	return total
}

func (b *fakeDistributedBackend) deadLetterBody(index int) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if index < 0 || index >= len(b.deadLetters) {
		return nil, false
	}

	return append([]byte(nil), b.deadLetters[index]...), true
}

func (b *fakeDistributedBackend) enqueueRawMessage(body []byte) string {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextID++
	id := fmt.Sprintf("id-%d", b.nextID)
	b.queue = append(b.queue, distributedMessage{ID: id, Body: append([]byte(nil), body...)})
	select {
	case b.notify <- struct{}{}:
	default:
	}

	return id
}

func (b *fakeDistributedBackend) pendingLen() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.pending)
}
