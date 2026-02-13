package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrBatchLoaderClosed is returned after Close.
	ErrBatchLoaderClosed = errors.New("batch loader closed")
	// ErrMissingBatchResult indicates fetch did not return a value for key.
	ErrMissingBatchResult = errors.New("batch fetch missing key")
)

const (
	defaultBatchSize = 64
	defaultBatchWait = 5 * time.Millisecond
	enqueueWaitStep  = 1 * time.Millisecond
)

// BatchFetchFunc fetches values for keys in one call.
// Implementations should respect ctx cancellation for fast shutdown.
type BatchFetchFunc[K comparable, V any] func(ctx context.Context, keys []K) (map[K]V, error)

// BatchLoaderConfig controls batching behavior.
type BatchLoaderConfig struct {
	MaxBatch int
	MaxWait  time.Duration
}

func (c BatchLoaderConfig) normalize() (BatchLoaderConfig, error) {
	cfg := c
	if cfg.MaxBatch == 0 {
		cfg.MaxBatch = defaultBatchSize
	}
	if cfg.MaxWait == 0 {
		cfg.MaxWait = defaultBatchWait
	}
	if cfg.MaxBatch < 1 {
		return BatchLoaderConfig{}, errors.New("max batch must be >= 1")
	}
	if cfg.MaxWait < time.Millisecond {
		return BatchLoaderConfig{}, errors.New("max wait must be >= 1ms")
	}

	return cfg, nil
}

type batchRequest[K comparable, V any] struct {
	ctx   context.Context
	key   K
	reply chan batchReply[V]
}

type batchReply[V any] struct {
	value V
	err   error
}

// BatchLoader batches concurrent key lookups to avoid N+1 data access patterns.
type BatchLoader[K comparable, V any] struct {
	cfg   BatchLoaderConfig
	fetch BatchFetchFunc[K, V]

	reqCh  chan batchRequest[K, V]
	stopCh chan struct{}

	runCtx    context.Context
	runCancel context.CancelFunc
	runDone   chan struct{}
	closeOnce sync.Once
	closeMu   sync.RWMutex
	closed    bool
}

// NewBatchLoader starts a batching loop.
func NewBatchLoader[K comparable, V any](cfg BatchLoaderConfig, fetch BatchFetchFunc[K, V]) (*BatchLoader[K, V], error) {
	if fetch == nil {
		return nil, errors.New("fetch function is nil")
	}

	normalized, err := cfg.normalize()
	if err != nil {
		return nil, err
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	loader := &BatchLoader[K, V]{
		cfg:       normalized,
		fetch:     fetch,
		reqCh:     make(chan batchRequest[K, V], normalized.MaxBatch*4),
		stopCh:    make(chan struct{}),
		runCtx:    runCtx,
		runCancel: runCancel,
		runDone:   make(chan struct{}),
	}

	go loader.run()

	return loader, nil
}

// Load resolves one key using batched fetches.
func (l *BatchLoader[K, V]) Load(ctx context.Context, key K) (V, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	replyCh := make(chan batchReply[V], 1)
	request := batchRequest[K, V]{ctx: ctx, key: key, reply: replyCh}

	err := l.enqueueRequest(ctx, request)
	if err != nil {
		var zero V
		return zero, err
	}

	select {
	case <-l.stopCh:
		var zero V
		return zero, ErrBatchLoaderClosed
	case <-ctx.Done():
		var zero V
		return zero, ctx.Err()
	case response := <-replyCh:
		return response.value, response.err
	}
}

// Close stops the loader and unblocks pending requests.
// It does not wait for fetch to return because fetch may ignore cancellation.
func (l *BatchLoader[K, V]) Close() {
	l.closeOnce.Do(func() {
		l.closeMu.Lock()
		l.closed = true
		close(l.stopCh)

		if l.runCancel != nil {
			l.runCancel()
		}
		l.closeMu.Unlock()

		// Best effort: unblock queued requests even when fetch is non-cooperative.
		l.rejectQueuedRequests(ErrBatchLoaderClosed)
	})
}

func (l *BatchLoader[K, V]) run() {
	defer close(l.runDone)

	for {
		select {
		case <-l.stopCh:
			l.rejectQueuedRequests(ErrBatchLoaderClosed)
			return
		case first := <-l.reqCh:
			if !l.collectAndDispatch(first) {
				l.rejectQueuedRequests(ErrBatchLoaderClosed)
				return
			}
		}
	}
}

func (l *BatchLoader[K, V]) collectAndDispatch(first batchRequest[K, V]) bool {
	batch := make([]batchRequest[K, V], 0, l.cfg.MaxBatch)
	batch = append(batch, first)

	timer := time.NewTimer(l.cfg.MaxWait)
	for len(batch) < l.cfg.MaxBatch {
		select {
		case <-l.stopCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			l.respondBatch(batch, batchReply[V]{err: ErrBatchLoaderClosed})
			return false
		case request := <-l.reqCh:
			batch = append(batch, request)
		case <-timer.C:
			goto dispatch
		}
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

dispatch:
	activeKeys := make(map[K][]batchRequest[K, V])
	keys := make([]K, 0, len(batch))
	for _, request := range batch {
		if err := request.ctx.Err(); err != nil {
			l.respondOne(request, batchReply[V]{err: err})
			continue
		}

		requests, exists := activeKeys[request.key]
		if !exists {
			keys = append(keys, request.key)
		}
		activeKeys[request.key] = append(requests, request)
	}
	if len(activeKeys) == 0 {
		return true
	}

	results, err := l.safeFetch(keys)
	if err != nil {
		if errors.Is(err, context.Canceled) && l.isClosed() {
			l.respondMap(activeKeys, batchReply[V]{err: ErrBatchLoaderClosed})
			return false
		}

		l.respondMap(activeKeys, batchReply[V]{err: err})
		return true
	}

	for key, requests := range activeKeys {
		value, exists := results[key]
		if !exists {
			l.respondSlice(requests, batchReply[V]{err: ErrMissingBatchResult})
			continue
		}

		l.respondSlice(requests, batchReply[V]{value: value})
	}

	return true
}

func (l *BatchLoader[K, V]) safeFetch(keys []K) (results map[K]V, err error) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}

		err = fmt.Errorf("batch fetch panic: %v", recovered)
	}()

	return l.fetch(l.runCtx, keys)
}

func (l *BatchLoader[K, V]) rejectQueuedRequests(err error) {
	for {
		select {
		case request := <-l.reqCh:
			l.respondOne(request, batchReply[V]{err: err})
		default:
			return
		}
	}
}

func (l *BatchLoader[K, V]) respondBatch(batch []batchRequest[K, V], reply batchReply[V]) {
	for _, request := range batch {
		l.respondOne(request, reply)
	}
}

func (l *BatchLoader[K, V]) respondMap(active map[K][]batchRequest[K, V], reply batchReply[V]) {
	for _, requests := range active {
		l.respondSlice(requests, reply)
	}
}

func (l *BatchLoader[K, V]) respondSlice(requests []batchRequest[K, V], reply batchReply[V]) {
	for _, request := range requests {
		l.respondOne(request, reply)
	}
}

func (l *BatchLoader[K, V]) respondOne(request batchRequest[K, V], reply batchReply[V]) {
	select {
	case request.reply <- reply:
	default:
	}
}

func (l *BatchLoader[K, V]) isClosed() bool {
	l.closeMu.RLock()
	closed := l.closed
	l.closeMu.RUnlock()

	return closed
}

func (l *BatchLoader[K, V]) enqueueRequest(ctx context.Context, request batchRequest[K, V]) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		l.closeMu.RLock()
		if l.closed {
			l.closeMu.RUnlock()
			return ErrBatchLoaderClosed
		}

		select {
		case l.reqCh <- request:
			l.closeMu.RUnlock()
			return nil
		default:
			l.closeMu.RUnlock()
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-l.stopCh:
			return ErrBatchLoaderClosed
		case <-time.After(enqueueWaitStep):
		}
	}
}
