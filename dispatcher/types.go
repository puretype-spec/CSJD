package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrDuplicateJob is returned when a job with the same ID already exists in pending/in-flight/recent window.
	ErrDuplicateJob = errors.New("duplicate job id")
	// ErrDispatcherClosed is returned when submitting to a stopped dispatcher.
	ErrDispatcherClosed = errors.New("dispatcher is closed")
	// ErrDispatcherNotStarted is returned when dispatcher has not started.
	ErrDispatcherNotStarted = errors.New("dispatcher is not started")
	// ErrHandlerNotFound is returned when no handler is registered for job type.
	ErrHandlerNotFound = errors.New("handler not found")
)

// Job is immutable metadata consumed by the dispatcher.
type Job struct {
	ID          string
	Type        string
	Payload     []byte
	MaxAttempts int
	Timeout     time.Duration
	CreatedAt   time.Time
}

func (j Job) effectiveMaxAttempts(defaultValue int) int {
	if j.MaxAttempts > 0 {
		return j.MaxAttempts
	}

	return defaultValue
}

func (j Job) effectiveTimeout(defaultValue time.Duration) time.Duration {
	if j.Timeout > 0 {
		return j.Timeout
	}

	return defaultValue
}

func (j Job) validate() error {
	if j.ID == "" {
		return errors.New("job id is required")
	}

	if j.Type == "" {
		return errors.New("job type is required")
	}

	return nil
}

// HandlerFunc handles one job execution.
type HandlerFunc func(ctx context.Context, job Job) error

// PermanentError indicates retries should stop immediately.
type PermanentError struct {
	Err error
}

func (e PermanentError) Error() string {
	if e.Err == nil {
		return "permanent error"
	}

	return e.Err.Error()
}

func (e PermanentError) Unwrap() error {
	return e.Err
}

// MarkPermanent wraps an error as non-retriable.
func MarkPermanent(err error) error {
	if err == nil {
		return nil
	}

	return PermanentError{Err: err}
}

func isPermanentError(err error) bool {
	var permanent PermanentError

	return errors.As(err, &permanent)
}

// BatchSubmitReport summarizes SubmitBatch results.
type BatchSubmitReport struct {
	Accepted   int
	Duplicates int
	Invalid    int
	Errors     []error
}

// MetricsSnapshot is an immutable metrics view.
type MetricsSnapshot struct {
	Submitted  uint64
	Accepted   uint64
	Duplicates uint64
	Processed  uint64
	Retried    uint64
	Succeeded  uint64
	Failed     uint64
	Panics     uint64
}

func (m MetricsSnapshot) String() string {
	return fmt.Sprintf(
		"submitted=%d accepted=%d duplicates=%d processed=%d retried=%d succeeded=%d failed=%d panics=%d",
		m.Submitted,
		m.Accepted,
		m.Duplicates,
		m.Processed,
		m.Retried,
		m.Succeeded,
		m.Failed,
		m.Panics,
	)
}
