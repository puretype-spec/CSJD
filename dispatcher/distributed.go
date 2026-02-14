package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultDistributedBatchSize     = 64
	defaultDistributedBlock         = time.Second
	defaultDistributedClaimInterval = 2 * time.Second
	defaultDistributedClaimMinIdle  = 10 * time.Second
	distributedFinalizeMaxAttempts  = 3
	distributedFinalizeBaseDelay    = 10 * time.Millisecond
)

type distributedMessage struct {
	ID   string
	Body []byte
}

type distributedBackend interface {
	EnsureGroup(ctx context.Context) error
	Add(ctx context.Context, body []byte, maxPending int) (string, error)
	ReadGroup(ctx context.Context, count int64, block time.Duration) ([]distributedMessage, error)
	AutoClaim(ctx context.Context, minIdle time.Duration, start string, count int64) ([]distributedMessage, string, error)
	Touch(ctx context.Context, id string) error
	Ack(ctx context.Context, ids ...string) error
	Del(ctx context.Context, ids ...string) error
	AddDeadLetter(ctx context.Context, body []byte) (string, error)
	Len(ctx context.Context) (int64, error)
	Close() error
}

// DistributedConfig controls distributed dispatcher behavior.
type DistributedConfig struct {
	Workers             int
	Stream              string
	DeadLetterStream    string
	Group               string
	Consumer            string
	BatchSize           int64
	Block               time.Duration
	ClaimInterval       time.Duration
	ClaimMinIdle        time.Duration
	MaxPendingJobs      int
	RetryMinDelay       time.Duration
	RetryMaxDelay       time.Duration
	RetryJitter         float64
	DefaultMaxAttempts  int
	DefaultJobTimeout   time.Duration
	MaxDetachedHandlers int
}

func (c DistributedConfig) normalize() (DistributedConfig, error) {
	cfg := c
	if cfg.Workers == 0 {
		cfg.Workers = defaultWorkers
	}
	if cfg.Stream == "" {
		return DistributedConfig{}, errors.New("stream is required")
	}
	if cfg.DeadLetterStream == "" {
		cfg.DeadLetterStream = cfg.Stream + ":dlq"
	}
	if cfg.DeadLetterStream == cfg.Stream {
		return DistributedConfig{}, errors.New("dead letter stream must be different from stream")
	}
	if cfg.Group == "" {
		return DistributedConfig{}, errors.New("group is required")
	}
	if cfg.Consumer == "" {
		cfg.Consumer = defaultConsumerName()
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = defaultDistributedBatchSize
	}
	if cfg.Block == 0 {
		cfg.Block = defaultDistributedBlock
	}
	if cfg.ClaimInterval == 0 {
		cfg.ClaimInterval = defaultDistributedClaimInterval
	}
	if cfg.ClaimMinIdle == 0 {
		cfg.ClaimMinIdle = defaultDistributedClaimMinIdle
	}
	if cfg.RetryMinDelay == 0 {
		cfg.RetryMinDelay = defaultRetryMinDelay
	}
	if cfg.RetryMaxDelay == 0 {
		cfg.RetryMaxDelay = defaultRetryMaxDelay
	}
	if cfg.DefaultMaxAttempts == 0 {
		cfg.DefaultMaxAttempts = defaultMaxAttempts
	}
	if cfg.DefaultJobTimeout == 0 {
		cfg.DefaultJobTimeout = defaultJobTimeout
	}

	if cfg.Workers < 1 {
		return DistributedConfig{}, errors.New("workers must be >= 1")
	}
	if cfg.BatchSize < 1 {
		return DistributedConfig{}, errors.New("batch size must be >= 1")
	}
	if cfg.Block < time.Millisecond {
		return DistributedConfig{}, errors.New("block must be >= 1ms")
	}
	if cfg.ClaimInterval < time.Millisecond {
		return DistributedConfig{}, errors.New("claim interval must be >= 1ms")
	}
	if cfg.ClaimMinIdle < time.Millisecond {
		return DistributedConfig{}, errors.New("claim min idle must be >= 1ms")
	}
	if cfg.MaxPendingJobs < 0 {
		return DistributedConfig{}, errors.New("max pending jobs must be >= 0")
	}
	if cfg.RetryMinDelay < time.Millisecond {
		return DistributedConfig{}, errors.New("retry min delay must be >= 1ms")
	}
	if cfg.RetryMaxDelay < cfg.RetryMinDelay {
		return DistributedConfig{}, errors.New("retry max delay must be >= retry min delay")
	}
	if cfg.RetryJitter < 0 || cfg.RetryJitter > 1 {
		return DistributedConfig{}, errors.New("retry jitter must be in range [0,1]")
	}
	if cfg.DefaultMaxAttempts < 1 {
		return DistributedConfig{}, errors.New("default max attempts must be >= 1")
	}
	if cfg.DefaultJobTimeout < time.Millisecond {
		return DistributedConfig{}, errors.New("default job timeout must be >= 1ms")
	}
	if cfg.MaxDetachedHandlers < 0 {
		return DistributedConfig{}, errors.New("max detached handlers must be >= 0")
	}

	return cfg, nil
}

func defaultConsumerName() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "node"
	}
	return fmt.Sprintf("%s-%d-%d", hostname, os.Getpid(), time.Now().UnixNano())
}

type distributedEnvelope struct {
	Job     Job `json:"job"`
	Attempt int `json:"attempt"`
}

// DistributedDispatcher consumes and dispatches jobs from a shared backend.
type DistributedDispatcher struct {
	cfg     DistributedConfig
	backend distributedBackend

	stateMu    sync.Mutex
	handlerMu  sync.RWMutex
	deliveryMu sync.Mutex

	handlers map[string]HandlerFunc

	runCtx context.Context
	cancel context.CancelFunc

	started bool
	closed  bool

	deliveries chan distributedMessage
	inFlight   map[string]struct{}

	wg sync.WaitGroup

	backoffRand   *rand.Rand
	backoffRandMu sync.Mutex

	metrics metricsCounters

	handlerSlots     chan struct{}
	detachedInFlight atomic.Int32
}

// NewDistributedDispatcher creates a distributed dispatcher with an injected backend.
func NewDistributedDispatcher(cfg DistributedConfig, backend distributedBackend) (*DistributedDispatcher, error) {
	if backend == nil {
		return nil, errors.New("distributed backend is nil")
	}

	normalized, err := cfg.normalize()
	if err != nil {
		return nil, err
	}

	return &DistributedDispatcher{
		cfg:          normalized,
		backend:      backend,
		handlers:     make(map[string]HandlerFunc),
		backoffRand:  rand.New(rand.NewSource(time.Now().UnixNano())),
		handlerSlots: newHandlerSlots(normalized.Workers),
	}, nil
}

// RegisterHandler adds or replaces a handler for a distributed job type.
func (d *DistributedDispatcher) RegisterHandler(jobType string, handler HandlerFunc) error {
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

// Start creates/joins consumer group and starts poll/claim/worker loops.
func (d *DistributedDispatcher) Start() error {
	d.stateMu.Lock()
	if d.started {
		if d.closed {
			d.stateMu.Unlock()
			return ErrDispatcherClosed
		}

		d.stateMu.Unlock()
		return nil
	}
	if d.closed {
		d.stateMu.Unlock()
		return ErrDispatcherClosed
	}
	d.stateMu.Unlock()

	if err := d.backend.EnsureGroup(context.Background()); err != nil {
		return fmt.Errorf("ensure consumer group: %w", err)
	}

	d.stateMu.Lock()
	if d.started {
		d.stateMu.Unlock()
		return nil
	}
	d.runCtx, d.cancel = context.WithCancel(context.Background())
	d.deliveries = make(chan distributedMessage, max(1, d.cfg.Workers)*int(d.cfg.BatchSize)*2)
	d.inFlight = make(map[string]struct{})
	d.handlerSlots = newHandlerSlots(d.cfg.Workers)
	d.detachedInFlight.Store(0)
	d.started = true
	d.closed = false
	d.stateMu.Unlock()

	d.wg.Add(1)
	go d.readLoop()

	d.wg.Add(1)
	go d.claimLoop()

	for i := 0; i < d.cfg.Workers; i++ {
		d.wg.Add(1)
		go d.workerLoop()
	}

	return nil
}

// Stop cancels consumers/workers and waits until they exit.
func (d *DistributedDispatcher) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	d.stateMu.Lock()
	if !d.started {
		d.stateMu.Unlock()
		return nil
	}
	d.closed = true
	cancel := d.cancel
	d.stateMu.Unlock()

	if cancel != nil {
		cancel()
	}

	done := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		d.markStopped()
		return nil
	case <-ctx.Done():
		go func() {
			<-done
			d.markStopped()
		}()
		return ctx.Err()
	}
}

// Close stops the dispatcher and closes backend resources.
func (d *DistributedDispatcher) Close(ctx context.Context) error {
	stopErr := d.Stop(ctx)
	closeErr := d.backend.Close()
	if stopErr != nil {
		return stopErr
	}
	if closeErr != nil {
		return closeErr
	}

	return nil
}

// Submit appends one distributed job to the shared stream.
func (d *DistributedDispatcher) Submit(job Job) error {
	if err := job.validate(); err != nil {
		return err
	}

	d.metrics.submitted.Add(1)
	if err := d.ensureSubmitReady(); err != nil {
		return err
	}

	if job.CreatedAt.IsZero() {
		job.CreatedAt = time.Now()
	}

	envelope := distributedEnvelope{
		Job:     cloneJob(job),
		Attempt: 1,
	}

	if err := d.enqueueEnvelope(context.Background(), envelope, true); err != nil {
		return err
	}
	d.metrics.accepted.Add(1)

	return nil
}

// SubmitBatch appends multiple jobs to the shared stream.
func (d *DistributedDispatcher) SubmitBatch(jobs []Job) BatchSubmitReport {
	report := BatchSubmitReport{}
	if len(jobs) == 0 {
		return report
	}

	setError := func(index int, err error) {
		if report.Errors == nil {
			report.Errors = make([]error, len(jobs))
		}
		report.Errors[index] = err
	}

	d.metrics.submitted.Add(uint64(len(jobs)))
	if err := d.ensureSubmitReady(); err != nil {
		report.Errors = make([]error, len(jobs))
		for i := range jobs {
			report.Errors[i] = err
		}
		return report
	}

	for i, job := range jobs {
		if err := job.validate(); err != nil {
			report.Invalid++
			setError(i, err)
			continue
		}

		if job.CreatedAt.IsZero() {
			job.CreatedAt = time.Now()
		}

		envelope := distributedEnvelope{
			Job:     cloneJob(job),
			Attempt: 1,
		}
		if err := d.enqueueEnvelope(context.Background(), envelope, true); err != nil {
			setError(i, err)
			continue
		}

		report.Accepted++
		d.metrics.accepted.Add(1)
	}

	return report
}

// Metrics returns a point-in-time counter snapshot.
func (d *DistributedDispatcher) Metrics() MetricsSnapshot {
	return MetricsSnapshot{
		Submitted:        d.metrics.submitted.Load(),
		Accepted:         d.metrics.accepted.Load(),
		Duplicates:       d.metrics.duplicates.Load(),
		Processed:        d.metrics.processed.Load(),
		Retried:          d.metrics.retried.Load(),
		Succeeded:        d.metrics.succeeded.Load(),
		Failed:           d.metrics.failed.Load(),
		Panics:           d.metrics.panics.Load(),
		Detached:         d.metrics.detached.Load(),
		FinalizeErrors:   d.metrics.finalize.Load(),
		DeadLetterErrors: d.metrics.deadLetter.Load(),
	}
}

func (d *DistributedDispatcher) ensureSubmitReady() error {
	d.stateMu.Lock()
	started := d.started
	closed := d.closed
	d.stateMu.Unlock()

	if !started {
		return ErrDispatcherNotStarted
	}
	if closed {
		return ErrDispatcherClosed
	}

	return nil
}

func (d *DistributedDispatcher) readLoop() {
	defer d.wg.Done()

	for {
		select {
		case <-d.runCtx.Done():
			return
		default:
		}

		messages, err := d.backend.ReadGroup(d.runCtx, d.cfg.BatchSize, d.cfg.Block)
		if err != nil {
			if d.runCtx.Err() != nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if len(messages) == 0 {
			continue
		}

		for _, message := range messages {
			if !d.enqueueDelivery(message) && d.runCtx.Err() != nil {
				return
			}
		}
	}
}

func (d *DistributedDispatcher) claimLoop() {
	defer d.wg.Done()

	ticker := time.NewTicker(d.cfg.ClaimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.runCtx.Done():
			return
		case <-ticker.C:
		}

		start := "0-0"
		for {
			messages, next, err := d.backend.AutoClaim(d.runCtx, d.cfg.ClaimMinIdle, start, d.cfg.BatchSize)
			if err != nil {
				break
			}
			start = next
			if len(messages) == 0 {
				break
			}

			for _, message := range messages {
				if !d.enqueueDelivery(message) && d.runCtx.Err() != nil {
					return
				}
			}

			if len(messages) < int(d.cfg.BatchSize) {
				break
			}
		}
	}
}

func (d *DistributedDispatcher) workerLoop() {
	defer d.wg.Done()

	for {
		select {
		case <-d.runCtx.Done():
			return
		case message := <-d.deliveries:
			d.processMessage(message)
		}
	}
}

func (d *DistributedDispatcher) processMessage(message distributedMessage) {
	defer d.releaseDelivery(message.ID)
	d.metrics.processed.Add(1)

	envelope, err := decodeEnvelope(message.Body)
	if err != nil {
		_ = d.finalizeDeadLetter(message, nil, "decode_error", err)
		return
	}
	stopHeartbeat := d.startClaimHeartbeat(message.ID)
	defer stopHeartbeat()

	err = d.executeEnvelope(envelope)
	if err == nil {
		if finalizeErr := d.finalizeMessageWithRetry(message.ID); finalizeErr != nil {
			d.metrics.finalize.Add(1)
			return
		}
		d.metrics.succeeded.Add(1)
		return
	}

	maxAttempts := envelope.Job.effectiveMaxAttempts(d.cfg.DefaultMaxAttempts)
	if isPermanentError(err) || envelope.Attempt >= maxAttempts {
		reason := "max_attempts_exhausted"
		if isPermanentError(err) {
			reason = "permanent_error"
		}
		_ = d.finalizeDeadLetter(message, &envelope, reason, err)
		return
	}

	delay := d.computeRetryDelay(envelope.Attempt)
	if delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-d.runCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
	}

	envelope.Attempt++
	if enqueueErr := d.enqueueEnvelope(d.runCtx, envelope, false); enqueueErr != nil {
		return
	}

	if finalizeErr := d.finalizeMessageWithRetry(message.ID); finalizeErr != nil {
		d.metrics.finalize.Add(1)
		return
	}
	d.metrics.retried.Add(1)
}

func (d *DistributedDispatcher) executeEnvelope(envelope distributedEnvelope) error {
	handler, ok := d.getHandler(envelope.Job.Type)
	if !ok {
		return MarkPermanent(fmt.Errorf("%w: %s", ErrHandlerNotFound, envelope.Job.Type))
	}

	jobTimeout := envelope.Job.effectiveTimeout(d.cfg.DefaultJobTimeout)
	jobCtx, cancel := context.WithTimeout(d.runCtx, jobTimeout)
	defer cancel()

	acquireErr := d.acquireHandlerSlot(jobCtx, d.handlerSlots)
	if acquireErr != nil {
		if errors.Is(acquireErr, context.DeadlineExceeded) {
			return MarkPermanent(acquireErr)
		}
		if errors.Is(acquireErr, context.Canceled) && d.runCtx.Err() != nil {
			return MarkPermanent(acquireErr)
		}
		return acquireErr
	}

	lease := newHandlerSlotLease(d.handlerSlots, d.releaseHandlerSlot)
	var detached atomic.Bool

	handlerErrCh := make(chan error, 1)
	go func() {
		defer lease.Release()
		defer func() {
			if detached.Load() {
				d.detachedInFlight.Add(-1)
			}
		}()
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}

			d.metrics.panics.Add(1)
			handlerErrCh <- MarkPermanent(fmt.Errorf("panic in distributed handler for job %s: %v", envelope.Job.ID, recovered))
		}()

		handlerErrCh <- handler(jobCtx, envelope.Job)
	}()

	select {
	case handlerErr := <-handlerErrCh:
		if handlerErr != nil && errors.Is(handlerErr, context.DeadlineExceeded) {
			return MarkPermanent(handlerErr)
		}
		if handlerErr != nil && errors.Is(handlerErr, context.Canceled) && d.runCtx.Err() != nil {
			return MarkPermanent(handlerErr)
		}
		return handlerErr
	case <-jobCtx.Done():
		handlerErr := jobCtx.Err()
		if errors.Is(handlerErr, context.DeadlineExceeded) {
			if d.tryDetachTimedOutHandler(&detached) {
				lease.Release()
			}
			return MarkPermanent(handlerErr)
		}
		if errors.Is(handlerErr, context.Canceled) && d.runCtx.Err() != nil {
			return MarkPermanent(handlerErr)
		}
		return handlerErr
	}
}

func (d *DistributedDispatcher) enqueueEnvelope(ctx context.Context, envelope distributedEnvelope, enforceQueueCap bool) error {
	body, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal distributed envelope: %w", err)
	}

	maxPending := 0
	if enforceQueueCap {
		maxPending = d.cfg.MaxPendingJobs
	}
	if _, err = d.backend.Add(ctx, body, maxPending); err != nil {
		if errors.Is(err, ErrQueueFull) {
			return ErrQueueFull
		}
		return fmt.Errorf("enqueue distributed job %s: %w", envelope.Job.ID, err)
	}

	return nil
}

type distributedDeadLetter struct {
	MessageID string    `json:"message_id"`
	FailedAt  time.Time `json:"failed_at"`
	Reason    string    `json:"reason"`
	Error     string    `json:"error"`
	Attempt   int       `json:"attempt"`
	Job       *Job      `json:"job,omitempty"`
	RawBody   []byte    `json:"raw_body,omitempty"`
}

func (d *DistributedDispatcher) enqueueDelivery(message distributedMessage) bool {
	if !d.reserveDelivery(message.ID) {
		return false
	}

	select {
	case d.deliveries <- message:
		return true
	case <-d.runCtx.Done():
		d.releaseDelivery(message.ID)
		return false
	}
}

func (d *DistributedDispatcher) reserveDelivery(id string) bool {
	d.deliveryMu.Lock()
	defer d.deliveryMu.Unlock()
	if d.inFlight == nil {
		d.inFlight = make(map[string]struct{})
	}
	if _, exists := d.inFlight[id]; exists {
		return false
	}
	d.inFlight[id] = struct{}{}

	return true
}

func (d *DistributedDispatcher) releaseDelivery(id string) {
	d.deliveryMu.Lock()
	delete(d.inFlight, id)
	d.deliveryMu.Unlock()
}

func (d *DistributedDispatcher) startClaimHeartbeat(messageID string) func() {
	if messageID == "" {
		return func() {}
	}
	d.stateMu.Lock()
	runCtx := d.runCtx
	d.stateMu.Unlock()
	if runCtx == nil {
		return func() {}
	}

	interval := d.cfg.ClaimMinIdle / 2
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-runCtx.Done():
				return
			case <-ticker.C:
				_ = d.backend.Touch(runCtx, messageID)
			}
		}
	}()

	return func() {
		close(done)
	}
}

func (d *DistributedDispatcher) finalizeDeadLetter(message distributedMessage, envelope *distributedEnvelope, reason string, cause error) error {
	deadLetter := distributedDeadLetter{
		MessageID: message.ID,
		FailedAt:  time.Now().UTC(),
		Reason:    reason,
		Error:     errorText(cause),
		RawBody:   append([]byte(nil), message.Body...),
	}
	if envelope != nil {
		clonedJob := cloneJob(envelope.Job)
		deadLetter.Job = &clonedJob
		deadLetter.Attempt = envelope.Attempt
	}

	body, err := json.Marshal(deadLetter)
	if err != nil {
		d.metrics.deadLetter.Add(1)
		return fmt.Errorf("marshal dead letter %s: %w", message.ID, err)
	}
	if _, err = d.backend.AddDeadLetter(d.runCtx, body); err != nil {
		d.metrics.deadLetter.Add(1)
		return fmt.Errorf("publish dead letter %s: %w", message.ID, err)
	}

	if err = d.finalizeMessageWithRetry(message.ID); err != nil {
		d.metrics.finalize.Add(1)
		d.metrics.deadLetter.Add(1)
		return fmt.Errorf("finalize dead letter %s: %w", message.ID, err)
	}
	d.metrics.failed.Add(1)

	return nil
}

func (d *DistributedDispatcher) finalizeMessageWithRetry(messageID string) error {
	var lastErr error
	delay := distributedFinalizeBaseDelay
	for attempt := 1; attempt <= distributedFinalizeMaxAttempts; attempt++ {
		if ackErr := d.backend.Ack(d.runCtx, messageID); ackErr != nil {
			lastErr = fmt.Errorf("ack distributed message %s: %w", messageID, ackErr)
		} else if delErr := d.backend.Del(d.runCtx, messageID); delErr != nil {
			lastErr = fmt.Errorf("delete distributed message %s: %w", messageID, delErr)
		} else {
			return nil
		}

		if attempt == distributedFinalizeMaxAttempts {
			break
		}

		timer := time.NewTimer(delay)
		select {
		case <-d.runCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return lastErr
		case <-timer.C:
		}
		if delay < 200*time.Millisecond {
			delay *= 2
		}
	}

	return lastErr
}

func errorText(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

func decodeEnvelope(body []byte) (distributedEnvelope, error) {
	var envelope distributedEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return distributedEnvelope{}, err
	}
	if envelope.Attempt < 1 {
		envelope.Attempt = 1
	}
	if err := envelope.Job.validate(); err != nil {
		return distributedEnvelope{}, err
	}

	return envelope, nil
}

func (d *DistributedDispatcher) getHandler(jobType string) (HandlerFunc, bool) {
	d.handlerMu.RLock()
	handler, ok := d.handlers[jobType]
	d.handlerMu.RUnlock()

	return handler, ok
}

func (d *DistributedDispatcher) computeRetryDelay(attempt int) time.Duration {
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

func (d *DistributedDispatcher) acquireHandlerSlot(ctx context.Context, slots chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.runCtx.Done():
		return d.runCtx.Err()
	case <-slots:
		return nil
	}
}

func (d *DistributedDispatcher) releaseHandlerSlot(slots chan struct{}) {
	select {
	case slots <- struct{}{}:
	default:
	}
}

func (d *DistributedDispatcher) tryDetachTimedOutHandler(detached *atomic.Bool) bool {
	limit := d.cfg.MaxDetachedHandlers
	if limit <= 0 {
		return false
	}

	for {
		current := d.detachedInFlight.Load()
		if int(current) >= limit {
			return false
		}
		if d.detachedInFlight.CompareAndSwap(current, current+1) {
			detached.Store(true)
			d.metrics.detached.Add(1)
			return true
		}
	}
}

func (d *DistributedDispatcher) markStopped() {
	d.stateMu.Lock()
	d.started = false
	d.closed = false
	d.runCtx = nil
	d.cancel = nil
	d.deliveries = nil
	d.inFlight = nil
	d.stateMu.Unlock()
}
