package dispatcher

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestFileJobStoreRoundTrip(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "jobs.json")
	store, err := NewFileJobStore(storePath)
	if err != nil {
		t.Fatalf("new file job store: %v", err)
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Fatalf("close file job store: %v", closeErr)
		}
	}()

	first := PersistedJob{
		Job: Job{
			ID:      "job-1",
			Type:    "email",
			Payload: []byte("alpha"),
		},
		Attempt: 1,
		RunAt:   time.Now(),
	}
	second := PersistedJob{
		Job: Job{
			ID:      "job-2",
			Type:    "email",
			Payload: []byte("beta"),
		},
		Attempt: 2,
		RunAt:   time.Now().Add(time.Second),
	}

	if err = store.Save(first); err != nil {
		t.Fatalf("save first job: %v", err)
	}
	if err = store.Save(second); err != nil {
		t.Fatalf("save second job: %v", err)
	}

	items, err := store.Load()
	if err != nil {
		t.Fatalf("load jobs: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 jobs, got %d", len(items))
	}

	if err = store.Delete("job-1"); err != nil {
		t.Fatalf("delete job-1: %v", err)
	}

	items, err = store.Load()
	if err != nil {
		t.Fatalf("load after delete: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 job after delete, got %d", len(items))
	}
	if items[0].Job.ID != "job-2" {
		t.Fatalf("unexpected remaining job id: %s", items[0].Job.ID)
	}

	if err = store.Close(); err != nil {
		t.Fatalf("close file job store before reopen: %v", err)
	}

	reopened, err := NewFileJobStore(storePath)
	if err != nil {
		t.Fatalf("reopen file job store: %v", err)
	}
	defer func() {
		if closeErr := reopened.Close(); closeErr != nil {
			t.Fatalf("close reopened file job store: %v", closeErr)
		}
	}()

	items, err = reopened.Load()
	if err != nil {
		t.Fatalf("load from reopened store: %v", err)
	}
	if len(items) != 1 || items[0].Job.ID != "job-2" {
		t.Fatalf("unexpected reopened items: %+v", items)
	}
}

func TestFileJobStoreRejectsSecondProcessLock(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "jobs.json")

	first, err := NewFileJobStore(storePath)
	if err != nil {
		t.Fatalf("new first file job store: %v", err)
	}
	defer func() {
		if closeErr := first.Close(); closeErr != nil {
			t.Fatalf("close first file job store: %v", closeErr)
		}
	}()

	_, err = NewFileJobStore(storePath)
	if !errors.Is(err, ErrJobStoreLocked) {
		t.Fatalf("expected ErrJobStoreLocked, got %v", err)
	}
}

func TestDispatcherRestoresPersistedJobsOnStart(t *testing.T) {
	store, err := NewFileJobStore(filepath.Join(t.TempDir(), "jobs.json"))
	if err != nil {
		t.Fatalf("new file job store: %v", err)
	}
	defer func() {
		if closeErr := store.Close(); closeErr != nil {
			t.Fatalf("close file job store: %v", closeErr)
		}
	}()

	if err = store.Save(PersistedJob{
		Job: Job{
			ID:   "restore-1",
			Type: "restore",
		},
		Attempt: 1,
		RunAt:   time.Now().Add(-time.Millisecond),
	}); err != nil {
		t.Fatalf("seed persisted job: %v", err)
	}

	d, err := New(Config{
		Workers:            1,
		RetryJitter:        0,
		RecentDuplicateTTL: 10 * time.Millisecond,
		Store:              store,
	})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	var processed atomic.Int32
	err = d.RegisterHandler("restore", func(_ context.Context, _ Job) error {
		processed.Add(1)
		return nil
	})
	if err != nil {
		t.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		t.Fatalf("start dispatcher: %v", err)
	}

	waitForCondition(t, 2*time.Second, func() bool {
		return processed.Load() == 1
	})

	if err = d.Stop(context.Background()); err != nil {
		t.Fatalf("stop dispatcher: %v", err)
	}

	remaining, err := store.Load()
	if err != nil {
		t.Fatalf("load remaining store jobs: %v", err)
	}
	if len(remaining) != 0 {
		t.Fatalf("expected no remaining persisted jobs, got %d", len(remaining))
	}
}

func TestDispatcherStartReturnsLoadErrorFromStore(t *testing.T) {
	expected := errors.New("load failed")

	d, err := New(Config{
		Workers:     1,
		RetryJitter: 0,
		Store: &errorStore{
			loadErr: expected,
		},
	})
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}

	if err = d.Start(); err == nil {
		t.Fatal("expected start to fail when store load fails")
	}
}

type errorStore struct {
	loadErr error
}

func (s *errorStore) Save(_ PersistedJob) error {
	return nil
}

func (s *errorStore) Delete(_ string) error {
	return nil
}

func (s *errorStore) Load() ([]PersistedJob, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}

	return nil, nil
}
