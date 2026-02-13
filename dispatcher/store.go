package dispatcher

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"syscall"
	"time"
)

var (
	// ErrJobStoreLocked is returned when another process holds the same store lock.
	ErrJobStoreLocked = errors.New("job store is locked by another process")
	// ErrJobStoreClosed is returned after FileJobStore.Close.
	ErrJobStoreClosed = errors.New("job store is closed")
)

const defaultStoreCompactEvery = 256

// PersistedJob is the durable representation used by JobStore.
type PersistedJob struct {
	Job     Job       `json:"job"`
	Attempt int       `json:"attempt"`
	RunAt   time.Time `json:"run_at"`
}

// JobStore persists queued jobs so they can be recovered after restart.
type JobStore interface {
	Save(job PersistedJob) error
	Delete(jobID string) error
	Load() ([]PersistedJob, error)
}

type noopJobStore struct{}

func (noopJobStore) Save(_ PersistedJob) error {
	return nil
}

func (noopJobStore) Delete(_ string) error {
	return nil
}

func (noopJobStore) Load() ([]PersistedJob, error) {
	return nil, nil
}

type fileStoreSnapshot struct {
	Jobs []PersistedJob `json:"jobs"`
}

type walOp struct {
	Type  string       `json:"type"`
	Job   PersistedJob `json:"job,omitempty"`
	JobID string       `json:"job_id,omitempty"`
}

// FileJobStore provides a durable single-node store backed by JSON snapshot + append-only WAL.
type FileJobStore struct {
	path     string
	walPath  string
	lockPath string

	mu              sync.Mutex
	jobs            map[string]PersistedJob
	walFile         *os.File
	lockFile        *os.File
	opsSinceCompact int
	compactEvery    int
	closed          bool
}

// NewFileJobStore creates a store, acquires an exclusive process lock, then loads snapshot and WAL.
func NewFileJobStore(path string) (*FileJobStore, error) {
	if path == "" {
		return nil, errors.New("store path is required")
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}

	store := &FileJobStore{
		path:         path,
		walPath:      path + ".wal",
		lockPath:     path + ".lock",
		jobs:         make(map[string]PersistedJob),
		compactEvery: defaultStoreCompactEvery,
	}

	lockFile, err := os.OpenFile(store.lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open store lock file: %w", err)
	}
	if lockErr := syscall.Flock(int(lockFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); lockErr != nil {
		_ = lockFile.Close()
		return nil, fmt.Errorf("%w: %v", ErrJobStoreLocked, lockErr)
	}
	store.lockFile = lockFile

	cleanupOnErr := true
	defer func() {
		if !cleanupOnErr {
			return
		}
		_ = store.Close()
	}()

	if err = store.loadSnapshotFromDisk(); err != nil {
		return nil, err
	}

	replayed, err := store.replayWALFromDisk()
	if err != nil {
		return nil, err
	}
	store.opsSinceCompact = replayed

	walFile, err := os.OpenFile(store.walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open store wal file: %w", err)
	}
	store.walFile = walFile

	if store.opsSinceCompact >= store.compactEvery {
		if err = store.compactLocked(); err != nil {
			return nil, err
		}
	}

	cleanupOnErr = false
	return store, nil
}

// Save upserts one persisted job.
func (s *FileJobStore) Save(job PersistedJob) error {
	normalized, err := normalizePersistedJob(job, time.Now())
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrJobStoreClosed
	}

	s.jobs[normalized.Job.ID] = clonePersistedJob(normalized)
	if err = s.appendWALOpLocked(walOp{
		Type: "save",
		Job:  clonePersistedJob(normalized),
	}); err != nil {
		return fmt.Errorf("append wal save: %w", err)
	}

	if s.opsSinceCompact >= s.compactEvery {
		if err = s.compactLocked(); err != nil {
			return fmt.Errorf("compact store after save: %w", err)
		}
	}

	return nil
}

// Delete removes one persisted job by ID.
func (s *FileJobStore) Delete(jobID string) error {
	if jobID == "" {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrJobStoreClosed
	}

	delete(s.jobs, jobID)
	if err := s.appendWALOpLocked(walOp{
		Type:  "delete",
		JobID: jobID,
	}); err != nil {
		return fmt.Errorf("append wal delete: %w", err)
	}

	if s.opsSinceCompact >= s.compactEvery {
		if err := s.compactLocked(); err != nil {
			return fmt.Errorf("compact store after delete: %w", err)
		}
	}

	return nil
}

// Load returns a snapshot of persisted jobs.
func (s *FileJobStore) Load() ([]PersistedJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrJobStoreClosed
	}

	items := make([]PersistedJob, 0, len(s.jobs))
	for _, item := range s.jobs {
		items = append(items, clonePersistedJob(item))
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Job.ID < items[j].Job.ID
	})

	return items, nil
}

// Close releases WAL and process lock resources.
func (s *FileJobStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true

	var closeErr error
	if s.walFile != nil {
		if err := s.walFile.Close(); err != nil {
			closeErr = err
		}
		s.walFile = nil
	}
	if s.lockFile != nil {
		if err := syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN); err != nil && closeErr == nil {
			closeErr = err
		}
		if err := s.lockFile.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
		s.lockFile = nil
	}

	if closeErr != nil {
		return fmt.Errorf("close store resources: %w", closeErr)
	}

	return nil
}

func (s *FileJobStore) loadSnapshotFromDisk() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read store snapshot: %w", err)
	}
	if len(data) == 0 {
		return nil
	}

	var snapshot fileStoreSnapshot
	if err = json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("unmarshal store snapshot: %w", err)
	}

	now := time.Now()
	for _, item := range snapshot.Jobs {
		normalized, normalizeErr := normalizePersistedJob(item, now)
		if normalizeErr != nil {
			continue
		}
		s.jobs[normalized.Job.ID] = clonePersistedJob(normalized)
	}

	return nil
}

func (s *FileJobStore) replayWALFromDisk() (int, error) {
	walFile, err := os.Open(s.walPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, fmt.Errorf("open store wal for replay: %w", err)
	}
	defer walFile.Close()

	reader := bufio.NewReader(walFile)
	replayed := 0
	for {
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				if applyErr := s.applyWALLine(trimmed); applyErr == nil {
					replayed++
				} else if !errors.Is(readErr, io.EOF) {
					return replayed, fmt.Errorf("apply wal line: %w", applyErr)
				}
			}
		}

		if readErr == nil {
			continue
		}
		if errors.Is(readErr, io.EOF) {
			return replayed, nil
		}

		return replayed, fmt.Errorf("read store wal: %w", readErr)
	}
}

func (s *FileJobStore) applyWALLine(line []byte) error {
	var op walOp
	if err := json.Unmarshal(line, &op); err != nil {
		return err
	}

	switch op.Type {
	case "save":
		normalized, err := normalizePersistedJob(op.Job, time.Now())
		if err != nil {
			return err
		}
		s.jobs[normalized.Job.ID] = clonePersistedJob(normalized)
	case "delete":
		delete(s.jobs, op.JobID)
	default:
		return fmt.Errorf("unknown wal op: %s", op.Type)
	}

	return nil
}

func (s *FileJobStore) appendWALOpLocked(op walOp) error {
	payload, err := json.Marshal(op)
	if err != nil {
		return fmt.Errorf("marshal wal op: %w", err)
	}

	payload = append(payload, '\n')
	if _, err = s.walFile.Write(payload); err != nil {
		return fmt.Errorf("write wal op: %w", err)
	}
	if err = s.walFile.Sync(); err != nil {
		return fmt.Errorf("sync wal op: %w", err)
	}
	s.opsSinceCompact++

	return nil
}

func (s *FileJobStore) compactLocked() error {
	if err := s.persistSnapshotLocked(); err != nil {
		return err
	}

	if s.walFile != nil {
		if err := s.walFile.Close(); err != nil {
			return fmt.Errorf("close wal before truncate: %w", err)
		}
		s.walFile = nil
	}

	walFile, err := os.OpenFile(s.walPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("recreate wal after compact: %w", err)
	}
	s.walFile = walFile
	s.opsSinceCompact = 0

	return nil
}

func (s *FileJobStore) persistSnapshotLocked() error {
	snapshot := fileStoreSnapshot{
		Jobs: make([]PersistedJob, 0, len(s.jobs)),
	}
	for _, item := range s.jobs {
		snapshot.Jobs = append(snapshot.Jobs, clonePersistedJob(item))
	}

	sort.Slice(snapshot.Jobs, func(i, j int) bool {
		return snapshot.Jobs[i].Job.ID < snapshot.Jobs[j].Job.ID
	})

	content, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal store snapshot: %w", err)
	}

	tempFile, err := os.CreateTemp(filepath.Dir(s.path), ".job-store-*")
	if err != nil {
		return fmt.Errorf("create temp snapshot file: %w", err)
	}

	tempPath := tempFile.Name()
	cleanupTemp := true
	defer func() {
		if cleanupTemp {
			_ = os.Remove(tempPath)
		}
	}()

	if _, err = tempFile.Write(content); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("write temp snapshot file: %w", err)
	}
	if err = tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("sync temp snapshot file: %w", err)
	}
	if err = tempFile.Close(); err != nil {
		return fmt.Errorf("close temp snapshot file: %w", err)
	}
	if err = os.Rename(tempPath, s.path); err != nil {
		return fmt.Errorf("rename temp snapshot file: %w", err)
	}
	cleanupTemp = false

	return nil
}

func normalizePersistedJob(item PersistedJob, now time.Time) (PersistedJob, error) {
	if err := item.Job.validate(); err != nil {
		return PersistedJob{}, fmt.Errorf("invalid persisted job: %w", err)
	}

	normalized := PersistedJob{
		Job: cloneJob(item.Job),
	}
	if normalized.Job.CreatedAt.IsZero() {
		normalized.Job.CreatedAt = now
	}

	normalized.Attempt = item.Attempt
	if normalized.Attempt < 1 {
		normalized.Attempt = 1
	}

	normalized.RunAt = item.RunAt
	if normalized.RunAt.IsZero() {
		normalized.RunAt = now
	}

	return normalized, nil
}

func clonePersistedJob(item PersistedJob) PersistedJob {
	return PersistedJob{
		Job:     cloneJob(item.Job),
		Attempt: item.Attempt,
		RunAt:   item.RunAt,
	}
}

func cloneJob(job Job) Job {
	job.Payload = cloneBytes(job.Payload)
	return job
}
