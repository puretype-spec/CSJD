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
	if !errors.Is(err, ErrDispatcherClosed) {
		t.Fatalf("expected ErrDispatcherClosed, got: %v", err)
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
