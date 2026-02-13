package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestDispatcherSubmitBatchAndProcessAll(t *testing.T) {
	d, err := New(Config{
		Workers:            8,
		RetryJitter:        0,
		RecentDuplicateTTL: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	var processed atomic.Int64
	err = d.RegisterHandler("email", func(_ context.Context, _ Job) error {
		processed.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start dispatcher: %v", err)
	}

	jobs := make([]Job, 200)
	for i := 0; i < len(jobs); i++ {
		jobs[i] = Job{ID: fmt.Sprintf("job-%03d", i), Type: "email"}
	}

	report := d.SubmitBatch(jobs)
	if report.Accepted != len(jobs) {
		t.Fatalf("accepted jobs mismatch: got %d want %d", report.Accepted, len(jobs))
	}
	if report.Duplicates != 0 || report.Invalid != 0 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if report.Errors != nil {
		t.Fatalf("expected nil errors for all-accepted batch, got len=%d", len(report.Errors))
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return processed.Load() == int64(len(jobs))
	})

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = d.Stop(stopCtx); err != nil {
		t.Fatalf("stop dispatcher: %v", err)
	}

	metrics := d.Metrics()
	if metrics.Succeeded != uint64(len(jobs)) {
		t.Fatalf("succeeded mismatch: got %d want %d", metrics.Succeeded, len(jobs))
	}
	if metrics.Failed != 0 || metrics.Retried != 0 {
		t.Fatalf("unexpected metrics: %s", metrics.String())
	}
}

func TestDispatcherRetriesUntilSuccess(t *testing.T) {
	d, err := New(Config{
		Workers:            2,
		RetryJitter:        0,
		RetryMinDelay:      10 * time.Millisecond,
		RetryMaxDelay:      20 * time.Millisecond,
		RecentDuplicateTTL: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	var calls atomic.Int32
	err = d.RegisterHandler("retry", func(_ context.Context, _ Job) error {
		attempt := calls.Add(1)
		if attempt < 3 {
			return errors.New("temporary failure")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start dispatcher: %v", err)
	}

	err = d.Submit(Job{ID: "retry-job", Type: "retry", MaxAttempts: 5})
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return calls.Load() >= 3
	})

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = d.Stop(stopCtx); err != nil {
		t.Fatalf("stop dispatcher: %v", err)
	}

	metrics := d.Metrics()
	if metrics.Succeeded != 1 {
		t.Fatalf("expected one success, got %d", metrics.Succeeded)
	}
	if metrics.Retried != 2 {
		t.Fatalf("expected 2 retries, got %d", metrics.Retried)
	}
}

func TestDispatcherDuplicateAndRecentWindow(t *testing.T) {
	d, err := New(Config{
		Workers:            1,
		RetryJitter:        0,
		RecentDuplicateTTL: 120 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	block := make(chan struct{})
	var successCount atomic.Int32
	err = d.RegisterHandler("slow", func(_ context.Context, _ Job) error {
		<-block
		successCount.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start dispatcher: %v", err)
	}

	if err = d.Submit(Job{ID: "dup", Type: "slow"}); err != nil {
		t.Fatalf("submit first: %v", err)
	}

	if err = d.Submit(Job{ID: "dup", Type: "slow"}); !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("expected duplicate error, got: %v", err)
	}

	close(block)

	waitForCondition(t, 2*time.Second, func() bool {
		return successCount.Load() == 1
	})

	if err = d.Submit(Job{ID: "dup", Type: "slow"}); !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("expected duplicate in recent window, got: %v", err)
	}

	time.Sleep(150 * time.Millisecond)
	if err = d.Submit(Job{ID: "dup", Type: "slow"}); err != nil {
		t.Fatalf("submit after ttl: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return successCount.Load() == 2
	})

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = d.Stop(stopCtx); err != nil {
		t.Fatalf("stop dispatcher: %v", err)
	}
}

func TestDispatcherRejectsWhenStopped(t *testing.T) {
	d, err := New(Config{Workers: 1, RetryJitter: 0})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	err = d.RegisterHandler("noop", func(_ context.Context, _ Job) error { return nil })
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start dispatcher: %v", err)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = d.Stop(stopCtx); err != nil {
		t.Fatalf("stop dispatcher: %v", err)
	}

	err = d.Submit(Job{ID: "after-stop", Type: "noop"})
	if !errors.Is(err, ErrDispatcherNotStarted) {
		t.Fatalf("expected ErrDispatcherNotStarted, got: %v", err)
	}
}

func TestDispatcherQueueFull(t *testing.T) {
	d, err := New(Config{
		Workers:            1,
		RetryJitter:        0,
		MaxPendingJobs:     1,
		RecentDuplicateTTL: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	block := make(chan struct{})
	err = d.RegisterHandler("block", func(_ context.Context, _ Job) error {
		<-block
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start dispatcher: %v", err)
	}

	if err = d.Submit(Job{ID: "full-1", Type: "block"}); err != nil {
		t.Fatalf("submit first: %v", err)
	}
	if err = d.Submit(Job{ID: "full-2", Type: "block"}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}

	report := d.SubmitBatch([]Job{
		{ID: "full-b1", Type: "block"},
		{ID: "full-b2", Type: "block"},
	})
	if report.Accepted != 0 {
		t.Fatalf("expected 0 accepted in full queue, got %d", report.Accepted)
	}
	if len(report.Errors) != 2 {
		t.Fatalf("expected 2 batch errors, got %d", len(report.Errors))
	}
	if !errors.Is(report.Errors[0], ErrQueueFull) || !errors.Is(report.Errors[1], ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull for both batch jobs, got %+v", report.Errors)
	}

	close(block)
	waitForCondition(t, 2*time.Second, func() bool {
		return d.Metrics().Succeeded == 1
	})

	if err = d.Stop(context.Background()); err != nil {
		t.Fatalf("stop dispatcher: %v", err)
	}
}

func TestDispatcherStopWithNilContext(t *testing.T) {
	d, err := New(Config{Workers: 1, RetryJitter: 0})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	err = d.RegisterHandler("noop", func(_ context.Context, _ Job) error { return nil })
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start dispatcher: %v", err)
	}

	if err = d.Stop(nil); err != nil {
		t.Fatalf("stop with nil context failed: %v", err)
	}
}

func TestDispatcherCopiesPayloadOnSubmit(t *testing.T) {
	d, err := New(Config{Workers: 1, RetryJitter: 0})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	release := make(chan struct{})
	received := make(chan []byte, 1)

	err = d.RegisterHandler("copy", func(_ context.Context, job Job) error {
		<-release
		copied := append([]byte(nil), job.Payload...)
		received <- copied
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start dispatcher: %v", err)
	}

	payload := []byte("alpha")
	if err = d.Submit(Job{ID: "payload-submit", Type: "copy", Payload: payload}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	payload[0] = 'x'
	close(release)

	select {
	case got := <-received:
		if string(got) != "alpha" {
			t.Fatalf("payload was mutated after submit: %q", string(got))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for handler result")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = d.Stop(stopCtx); err != nil {
		t.Fatalf("stop dispatcher: %v", err)
	}
}

func TestDispatcherCopiesPayloadOnSubmitBatch(t *testing.T) {
	d, err := New(Config{Workers: 1, RetryJitter: 0})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	release := make(chan struct{})
	type result struct {
		id      string
		payload string
	}
	results := make(chan result, 2)

	err = d.RegisterHandler("copy-batch", func(_ context.Context, job Job) error {
		<-release
		results <- result{id: job.ID, payload: string(job.Payload)}
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start dispatcher: %v", err)
	}

	payloadA := []byte("first")
	payloadB := []byte("second")

	report := d.SubmitBatch([]Job{
		{ID: "batch-a", Type: "copy-batch", Payload: payloadA},
		{ID: "batch-b", Type: "copy-batch", Payload: payloadB},
	})
	if report.Accepted != 2 {
		t.Fatalf("expected 2 accepted jobs, got %d", report.Accepted)
	}

	payloadA[0] = 'X'
	payloadB[0] = 'Y'
	close(release)

	collected := map[string]string{}
	for i := 0; i < 2; i++ {
		select {
		case item := <-results:
			collected[item.id] = item.payload
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting batch handler results")
		}
	}

	if collected["batch-a"] != "first" {
		t.Fatalf("batch-a payload was mutated: %q", collected["batch-a"])
	}
	if collected["batch-b"] != "second" {
		t.Fatalf("batch-b payload was mutated: %q", collected["batch-b"])
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err = d.Stop(stopCtx); err != nil {
		t.Fatalf("stop dispatcher: %v", err)
	}
}

func TestDispatcherCanRestartAfterStop(t *testing.T) {
	d, err := New(Config{Workers: 1, RetryJitter: 0})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	var processed atomic.Int32
	err = d.RegisterHandler("restart", func(_ context.Context, _ Job) error {
		processed.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start first run: %v", err)
	}
	if err = d.Submit(Job{ID: "restart-1", Type: "restart"}); err != nil {
		t.Fatalf("submit first run: %v", err)
	}
	waitForCondition(t, 2*time.Second, func() bool { return processed.Load() == 1 })
	if err = d.Stop(context.Background()); err != nil {
		t.Fatalf("stop first run: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start second run: %v", err)
	}
	if err = d.Submit(Job{ID: "restart-2", Type: "restart"}); err != nil {
		t.Fatalf("submit second run: %v", err)
	}
	waitForCondition(t, 2*time.Second, func() bool { return processed.Load() == 2 })
	if err = d.Stop(context.Background()); err != nil {
		t.Fatalf("stop second run: %v", err)
	}
}

func TestDispatcherCanRestartAfterStopTimeoutEventually(t *testing.T) {
	d, err := New(Config{Workers: 1, RetryJitter: 0, DefaultJobTimeout: 40 * time.Millisecond})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	err = d.RegisterHandler("slow", func(_ context.Context, _ Job) error {
		// Simulate a non-cooperative handler that ignores cancellation.
		time.Sleep(250 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start first run: %v", err)
	}
	if err = d.Submit(Job{ID: "slow-1", Type: "slow"}); err != nil {
		t.Fatalf("submit slow job: %v", err)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err = d.Stop(stopCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected stop timeout, got %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		err = d.Start()
		if err == nil {
			break
		}
		if !errors.Is(err, ErrDispatcherClosed) {
			t.Fatalf("unexpected start error while waiting restart readiness: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("dispatcher did not become restartable after timeout stop")
		}

		time.Sleep(10 * time.Millisecond)
	}

	if err = d.Submit(Job{ID: "slow-2", Type: "slow"}); err != nil {
		t.Fatalf("submit on restarted dispatcher: %v", err)
	}
	if err = d.Stop(context.Background()); err != nil {
		t.Fatalf("stop restarted dispatcher: %v", err)
	}
}

func TestDispatcherTimeoutDoesNotRetry(t *testing.T) {
	d, err := New(Config{
		Workers:             1,
		RetryJitter:         0,
		RetryMinDelay:       10 * time.Millisecond,
		RetryMaxDelay:       20 * time.Millisecond,
		MaxDetachedHandlers: 0,
		DefaultJobTimeout:   40 * time.Millisecond,
		RecentDuplicateTTL:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	var calls atomic.Int32
	err = d.RegisterHandler("timeout", func(_ context.Context, _ Job) error {
		calls.Add(1)
		// Ignore context on purpose to emulate non-cooperative handlers.
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start dispatcher: %v", err)
	}
	if err = d.Submit(Job{ID: "timeout-job", Type: "timeout", MaxAttempts: 5}); err != nil {
		t.Fatalf("submit timeout job: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		metrics := d.Metrics()
		return metrics.Failed == 1
	})

	metrics := d.Metrics()
	if metrics.Retried != 0 {
		t.Fatalf("expected no retries on timeout, got %d", metrics.Retried)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected one handler invocation, got %d", calls.Load())
	}

	if err = d.Stop(context.Background()); err != nil {
		t.Fatalf("stop dispatcher: %v", err)
	}
}

func TestDispatcherTimeoutDoesNotCreateOverlappingHandlerExecutions(t *testing.T) {
	d, err := New(Config{
		Workers:            1,
		RetryJitter:        0,
		RetryMinDelay:      10 * time.Millisecond,
		RetryMaxDelay:      20 * time.Millisecond,
		DefaultJobTimeout:  40 * time.Millisecond,
		RecentDuplicateTTL: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	var active atomic.Int32
	var maxActive atomic.Int32
	err = d.RegisterHandler("slow-noncoop", func(_ context.Context, _ Job) error {
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
		t.Fatalf("start dispatcher: %v", err)
	}
	if err = d.Submit(Job{ID: "overlap-1", Type: "slow-noncoop", MaxAttempts: 5}); err != nil {
		t.Fatalf("submit overlap-1: %v", err)
	}
	if err = d.Submit(Job{ID: "overlap-2", Type: "slow-noncoop", MaxAttempts: 5}); err != nil {
		t.Fatalf("submit overlap-2: %v", err)
	}

	waitForCondition(t, 3*time.Second, func() bool {
		metrics := d.Metrics()
		return metrics.Failed == 2
	})

	metrics := d.Metrics()
	if metrics.Retried != 0 {
		t.Fatalf("expected no retries for timeout jobs, got %d", metrics.Retried)
	}
	if maxActive.Load() > 1 {
		t.Fatalf("expected non-overlapping non-cooperative handlers, max active=%d", maxActive.Load())
	}

	if err = d.Stop(context.Background()); err != nil {
		t.Fatalf("stop dispatcher: %v", err)
	}
}

func TestDispatcherTimeoutDetachBudgetAllowsProgress(t *testing.T) {
	d, err := New(Config{
		Workers:             1,
		RetryJitter:         0,
		MaxDetachedHandlers: 1,
		DefaultJobTimeout:   40 * time.Millisecond,
		RecentDuplicateTTL:  10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	var active atomic.Int32
	var maxActive atomic.Int32
	err = d.RegisterHandler("slow-detach", func(_ context.Context, _ Job) error {
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
		t.Fatalf("start dispatcher: %v", err)
	}

	if err = d.Submit(Job{ID: "detach-1", Type: "slow-detach", MaxAttempts: 1}); err != nil {
		t.Fatalf("submit detach-1: %v", err)
	}
	if err = d.Submit(Job{ID: "detach-2", Type: "slow-detach", MaxAttempts: 1}); err != nil {
		t.Fatalf("submit detach-2: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return d.Metrics().Failed == 2
	})

	metrics := d.Metrics()
	if metrics.Detached < 1 {
		t.Fatalf("expected at least one detached timeout handler, got %d", metrics.Detached)
	}
	if maxActive.Load() < 2 {
		t.Fatalf("expected detached budget to allow overlap, max active=%d", maxActive.Load())
	}

	if err = d.Stop(context.Background()); err != nil {
		t.Fatalf("stop dispatcher: %v", err)
	}
}

func TestDispatcherRestartDoesNotInheritStuckHandlerSlot(t *testing.T) {
	d, err := New(Config{
		Workers:            1,
		RetryJitter:        0,
		DefaultJobTimeout:  40 * time.Millisecond,
		RecentDuplicateTTL: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	releaseSlow := make(chan struct{})
	var fastCalls atomic.Int32

	err = d.RegisterHandler("slow-stuck", func(_ context.Context, _ Job) error {
		<-releaseSlow
		return nil
	})
	if err != nil {
		t.Fatalf("register slow handler: %v", err)
	}

	err = d.RegisterHandler("fast", func(_ context.Context, _ Job) error {
		fastCalls.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("register fast handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start first run: %v", err)
	}

	if err = d.Submit(Job{ID: "slot-1", Type: "slow-stuck", MaxAttempts: 1}); err != nil {
		t.Fatalf("submit slow job: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return d.Metrics().Failed == 1
	})

	if err = d.Stop(context.Background()); err != nil {
		t.Fatalf("stop first run: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start second run: %v", err)
	}

	if err = d.Submit(Job{ID: "slot-2", Type: "fast", MaxAttempts: 1}); err != nil {
		t.Fatalf("submit fast job: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return fastCalls.Load() == 1
	})

	close(releaseSlow)

	if err = d.Stop(context.Background()); err != nil {
		t.Fatalf("stop second run: %v", err)
	}
}

func waitForCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		if condition() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %s", timeout)
		}

		time.Sleep(10 * time.Millisecond)
	}
}
