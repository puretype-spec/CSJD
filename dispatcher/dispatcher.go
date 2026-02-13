package dispatcher

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type metricsCounters struct {
	submitted  atomic.Uint64
	accepted   atomic.Uint64
	duplicates atomic.Uint64
	processed  atomic.Uint64
	retried    atomic.Uint64
	succeeded  atomic.Uint64
	failed     atomic.Uint64
	panics     atomic.Uint64
}

// Dispatcher is a concurrency-safe async job dispatcher.
type Dispatcher struct {
	cfg Config

	stateMu   sync.Mutex
	handlerMu sync.RWMutex

	handlers map[string]HandlerFunc

	pending   scheduledJobHeap
	pendingID map[string]struct{}
	inFlight  map[string]struct{}
	recent    *ttlSet

	notifyCh chan struct{}
	readyCh  chan scheduledJob

	runCtx context.Context
	cancel context.CancelFunc

	started bool
	closed  bool

	wg sync.WaitGroup

	backoffRand   *rand.Rand
	backoffRandMu sync.Mutex

	metrics metricsCounters
}

// New creates a dispatcher with validated configuration.
func New(config Config) (*Dispatcher, error) {
	cfg, err := config.normalize()
	if err != nil {
		return nil, err
	}

	d := &Dispatcher{
		cfg:         cfg,
		handlers:    make(map[string]HandlerFunc),
		pending:     make(scheduledJobHeap, 0),
		pendingID:   make(map[string]struct{}),
		inFlight:    make(map[string]struct{}),
		recent:      newTTLSet(cfg.RecentDuplicateTTL),
		notifyCh:    make(chan struct{}, 1),
		readyCh:     make(chan scheduledJob, cfg.ReadyQueueSize),
		backoffRand: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	heap.Init(&d.pending)

	return d, nil
}

// RegisterHandler adds or replaces a handler for a given job type.
func (d *Dispatcher) RegisterHandler(jobType string, handler HandlerFunc) error {
	if jobType == "" {
		return errors.New("job type is required")
	}
	if handler == nil {
		return errors.New("handler is nil")
	}

	d.handlerMu.Lock()
	d.handlers[jobType] = handler
	d.handlerMu.Unlock()

	return nil
}

// Start launches scheduler and worker goroutines.
func (d *Dispatcher) Start() error {
	d.stateMu.Lock()
	if d.started {
		d.stateMu.Unlock()
		return nil
	}
	if d.closed {
		d.stateMu.Unlock()
		return ErrDispatcherClosed
	}

	d.runCtx, d.cancel = context.WithCancel(context.Background())
	d.started = true
	workers := d.cfg.Workers
	d.stateMu.Unlock()

	d.wg.Add(1)
	go d.schedulerLoop()

	for i := 0; i < workers; i++ {
		d.wg.Add(1)
		go d.workerLoop()
	}

	return nil
}

// Stop drains pending work and waits until workers finish, or returns when ctx expires.
func (d *Dispatcher) Stop(ctx context.Context) error {
	d.stateMu.Lock()
	if !d.started {
		d.stateMu.Unlock()
		return nil
	}
	d.closed = true
	d.stateMu.Unlock()
	d.notify()

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		d.cancelRunContext()
		return nil
	case <-ctx.Done():
		d.cancelRunContext()
		return ctx.Err()
	}
}

// Submit enqueues a single job.
func (d *Dispatcher) Submit(job Job) error {
	err := job.validate()
	if err != nil {
		return err
	}

	d.metrics.submitted.Add(1)

	now := time.Now()
	d.stateMu.Lock()
	if !d.started {
		d.stateMu.Unlock()
		return ErrDispatcherNotStarted
	}
	if d.closed {
		d.stateMu.Unlock()
		return ErrDispatcherClosed
	}
	if d.isDuplicateLocked(job.ID, now) {
		d.stateMu.Unlock()
		d.metrics.duplicates.Add(1)
		return ErrDuplicateJob
	}
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}

	d.inFlight[job.ID] = struct{}{}
	heap.Push(&d.pending, scheduledJob{job: job, attempt: 1, runAt: now})
	d.pendingID[job.ID] = struct{}{}
	d.stateMu.Unlock()

	d.metrics.accepted.Add(1)
	d.notify()

	return nil
}

// SubmitBatch enqueues jobs with one lock acquisition to reduce ingestion overhead.
func (d *Dispatcher) SubmitBatch(jobs []Job) BatchSubmitReport {
	report := BatchSubmitReport{Errors: make([]error, len(jobs))}
	if len(jobs) == 0 {
		return report
	}

	d.metrics.submitted.Add(uint64(len(jobs)))

	now := time.Now()
	d.stateMu.Lock()
	if !d.started {
		d.stateMu.Unlock()
		for i := range report.Errors {
			report.Errors[i] = ErrDispatcherNotStarted
		}

		return report
	}
	if d.closed {
		d.stateMu.Unlock()
		for i := range report.Errors {
			report.Errors[i] = ErrDispatcherClosed
		}

		return report
	}

	for i, job := range jobs {
		validationErr := job.validate()
		if validationErr != nil {
			report.Invalid++
			report.Errors[i] = validationErr
			continue
		}

		if d.isDuplicateLocked(job.ID, now) {
			report.Duplicates++
			report.Errors[i] = ErrDuplicateJob
			d.metrics.duplicates.Add(1)
			continue
		}

		if job.CreatedAt.IsZero() {
			job.CreatedAt = now
		}

		d.inFlight[job.ID] = struct{}{}
		heap.Push(&d.pending, scheduledJob{job: job, attempt: 1, runAt: now})
		d.pendingID[job.ID] = struct{}{}
		report.Accepted++
		d.metrics.accepted.Add(1)
	}
	d.stateMu.Unlock()

	if report.Accepted > 0 {
		d.notify()
	}

	return report
}

// Metrics returns a point-in-time counter snapshot.
func (d *Dispatcher) Metrics() MetricsSnapshot {
	return MetricsSnapshot{
		Submitted:  d.metrics.submitted.Load(),
		Accepted:   d.metrics.accepted.Load(),
		Duplicates: d.metrics.duplicates.Load(),
		Processed:  d.metrics.processed.Load(),
		Retried:    d.metrics.retried.Load(),
		Succeeded:  d.metrics.succeeded.Load(),
		Failed:     d.metrics.failed.Load(),
		Panics:     d.metrics.panics.Load(),
	}
}

func (d *Dispatcher) schedulerLoop() {
	defer d.wg.Done()
	defer close(d.readyCh)

	for {
		job, wait, shouldStop := d.nextDueJob()
		if shouldStop {
			return
		}

		if job != nil {
			select {
			case d.readyCh <- *job:
			case <-d.runCtx.Done():
				return
			}

			continue
		}

		if wait < 0 {
			select {
			case <-d.runCtx.Done():
				return
			case <-d.notifyCh:
			}

			continue
		}

		timer := time.NewTimer(wait)
		select {
		case <-d.runCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-d.notifyCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
		}
	}
}

func (d *Dispatcher) nextDueJob() (*scheduledJob, time.Duration, bool) {
	d.stateMu.Lock()
	defer d.stateMu.Unlock()

	now := time.Now()
	d.recent.cleanupExpired(now)

	if d.closed && len(d.pending) == 0 && len(d.inFlight) == 0 {
		return nil, 0, true
	}

	if len(d.pending) == 0 {
		return nil, -1, false
	}

	head := d.pending[0]
	if head.runAt.After(now) {
		return nil, head.runAt.Sub(now), false
	}

	nextValue := heap.Pop(&d.pending)
	next, ok := nextValue.(scheduledJob)
	if !ok {
		return nil, -1, false
	}
	delete(d.pendingID, next.job.ID)

	return &next, 0, false
}

func (d *Dispatcher) workerLoop() {
	defer d.wg.Done()

	for job := range d.readyCh {
		d.metrics.processed.Add(1)

		err := d.execute(job)
		if err == nil {
			d.finishJob(job.job.ID, true)
			continue
		}

		maxAttempts := job.job.effectiveMaxAttempts(d.cfg.DefaultMaxAttempts)
		if isPermanentError(err) || job.attempt >= maxAttempts {
			d.finishJob(job.job.ID, false)
			continue
		}

		d.metrics.retried.Add(1)
		d.reschedule(job)
	}
}

func (d *Dispatcher) execute(item scheduledJob) error {
	handler, ok := d.getHandler(item.job.Type)
	if !ok {
		return MarkPermanent(fmt.Errorf("%w: %s", ErrHandlerNotFound, item.job.Type))
	}

	jobTimeout := item.job.effectiveTimeout(d.cfg.DefaultJobTimeout)
	jobCtx, cancel := context.WithTimeout(d.runCtx, jobTimeout)
	defer cancel()

	var handlerErr error
	func() {
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}

			d.metrics.panics.Add(1)
			handlerErr = MarkPermanent(fmt.Errorf("panic in handler for job %s: %v", item.job.ID, recovered))
		}()

		handlerErr = handler(jobCtx, item.job)
	}()

	if handlerErr != nil && errors.Is(handlerErr, context.Canceled) {
		if d.runCtx.Err() != nil {
			return MarkPermanent(handlerErr)
		}
	}

	return handlerErr
}

func (d *Dispatcher) reschedule(item scheduledJob) {
	delay := d.computeRetryDelay(item.attempt)
	runAt := time.Now().Add(delay)

	d.stateMu.Lock()
	if d.closed {
		d.stateMu.Unlock()
		d.finishJob(item.job.ID, false)
		return
	}

	heap.Push(&d.pending, scheduledJob{job: item.job, attempt: item.attempt + 1, runAt: runAt})
	d.pendingID[item.job.ID] = struct{}{}
	d.stateMu.Unlock()
	d.notify()
}

func (d *Dispatcher) finishJob(jobID string, success bool) {
	now := time.Now()
	d.stateMu.Lock()
	delete(d.inFlight, jobID)
	d.recent.add(jobID, now)
	d.stateMu.Unlock()

	if success {
		d.metrics.succeeded.Add(1)
	} else {
		d.metrics.failed.Add(1)
	}

	d.notify()
}

func (d *Dispatcher) getHandler(jobType string) (HandlerFunc, bool) {
	d.handlerMu.RLock()
	handler, ok := d.handlers[jobType]
	d.handlerMu.RUnlock()

	return handler, ok
}

func (d *Dispatcher) isDuplicateLocked(jobID string, now time.Time) bool {
	if _, exists := d.pendingID[jobID]; exists {
		return true
	}
	if _, exists := d.inFlight[jobID]; exists {
		return true
	}
	if d.recent.contains(jobID, now) {
		return true
	}

	return false
}

func (d *Dispatcher) notify() {
	select {
	case d.notifyCh <- struct{}{}:
	default:
	}
}

func (d *Dispatcher) computeRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := d.cfg.RetryMinDelay
	for i := 1; i < attempt && delay < d.cfg.RetryMaxDelay; i++ {
		if delay > d.cfg.RetryMaxDelay/2 {
			delay = d.cfg.RetryMaxDelay
			break
		}
		delay *= 2
	}
	if delay > d.cfg.RetryMaxDelay {
		delay = d.cfg.RetryMaxDelay
	}
	if d.cfg.RetryJitter <= 0 {
		return delay
	}

	jitterRange := float64(delay) * d.cfg.RetryJitter
	d.backoffRandMu.Lock()
	randomOffset := (d.backoffRand.Float64()*2 - 1) * jitterRange
	d.backoffRandMu.Unlock()

	adjusted := float64(delay) + randomOffset
	if adjusted < float64(time.Millisecond) {
		adjusted = float64(time.Millisecond)
	}
	maxDelay := float64(d.cfg.RetryMaxDelay)
	if adjusted > maxDelay {
		adjusted = maxDelay
	}

	return time.Duration(math.Round(adjusted))
}

func (d *Dispatcher) cancelRunContext() {
	d.stateMu.Lock()
	cancel := d.cancel
	d.stateMu.Unlock()

	if cancel != nil {
		cancel()
	}
}
