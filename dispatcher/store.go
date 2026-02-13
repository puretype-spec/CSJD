package dispatcher

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

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

// FileJobStore provides a simple single-node durable store backed by one JSON file.
type FileJobStore struct {
	path string

	mu   sync.Mutex
	jobs map[string]PersistedJob
}

// NewFileJobStore creates a file-based store and loads existing entries when present.
func NewFileJobStore(path string) (*FileJobStore, error) {
	if path == "" {
		return nil, errors.New("store path is required")
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create store dir: %w", err)
	}

	store := &FileJobStore{
		path: path,
		jobs: make(map[string]PersistedJob),
	}

	if err := store.loadFromDisk(); err != nil {
		return nil, err
	}

	return store, nil
}

// Save upserts one persisted job.
func (s *FileJobStore) Save(job PersistedJob) error {
	normalized, err := normalizePersistedJob(job, time.Now())
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.jobs[normalized.Job.ID] = clonePersistedJob(normalized)
	err = s.persistLocked()
	s.mu.Unlock()

	if err != nil {
		return fmt.Errorf("persist store save: %w", err)
	}

	return nil
}

// Delete removes one persisted job by ID.
func (s *FileJobStore) Delete(jobID string) error {
	if jobID == "" {
		return nil
	}

	s.mu.Lock()
	delete(s.jobs, jobID)
	err := s.persistLocked()
	s.mu.Unlock()

	if err != nil {
		return fmt.Errorf("persist store delete: %w", err)
	}

	return nil
}

// Load returns a snapshot of persisted jobs.
func (s *FileJobStore) Load() ([]PersistedJob, error) {
	s.mu.Lock()
	items := make([]PersistedJob, 0, len(s.jobs))
	for _, item := range s.jobs {
		items = append(items, clonePersistedJob(item))
	}
	s.mu.Unlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].Job.ID < items[j].Job.ID
	})

	return items, nil
}

func (s *FileJobStore) loadFromDisk() error {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read store file: %w", err)
	}

	if len(data) == 0 {
		return nil
	}

	var snapshot fileStoreSnapshot
	if err = json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("unmarshal store file: %w", err)
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

func (s *FileJobStore) persistLocked() error {
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
		return fmt.Errorf("create temp store file: %w", err)
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
		return fmt.Errorf("write temp store file: %w", err)
	}
	if err = tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("sync temp store file: %w", err)
	}
	if err = tempFile.Close(); err != nil {
		return fmt.Errorf("close temp store file: %w", err)
	}
	if err = os.Rename(tempPath, s.path); err != nil {
		return fmt.Errorf("rename temp store file: %w", err)
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
