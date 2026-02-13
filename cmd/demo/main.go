package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"csjd/dispatcher"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	d, err := dispatcher.New(dispatcher.Config{
		Workers:            4,
		RetryMinDelay:      100 * time.Millisecond,
		RetryMaxDelay:      2 * time.Second,
		RetryJitter:        0.10,
		DefaultMaxAttempts: 3,
		RecentDuplicateTTL: 2 * time.Second,
	})
	if err != nil {
		logger.Error("new dispatcher failed", "component", "bootstrap", "error", err)
		os.Exit(1)
	}

	err = d.RegisterHandler("print", func(_ context.Context, job dispatcher.Job) error {
		logger.Info(
			"job handled",
			"component", "worker",
			"job_id", job.ID,
			"job_type", job.Type,
			"payload_size", len(job.Payload),
		)

		return nil
	})
	if err != nil {
		logger.Error("register handler failed", "component", "bootstrap", "job_type", "print", "error", err)
		os.Exit(1)
	}

	if err = d.Start(); err != nil {
		logger.Error("start dispatcher failed", "component", "bootstrap", "error", err)
		os.Exit(1)
	}

	jobs := []dispatcher.Job{
		{ID: "demo-1", Type: "print", Payload: []byte("hello")},
		{ID: "demo-2", Type: "print", Payload: []byte("concurrency-safe")},
		{ID: "demo-3", Type: "print", Payload: []byte("job dispatcher")},
	}
	report := d.SubmitBatch(jobs)
	logger.Info(
		"submit batch result",
		"component", "ingest",
		"accepted", report.Accepted,
		"duplicates", report.Duplicates,
		"invalid", report.Invalid,
	)

	time.Sleep(300 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = d.Stop(stopCtx); err != nil {
		logger.Error("stop dispatcher failed", "component", "shutdown", "error", err)
		os.Exit(1)
	}

	metrics := d.Metrics()
	logger.Info(
		"dispatcher metrics",
		"component", "metrics",
		"submitted", metrics.Submitted,
		"accepted", metrics.Accepted,
		"processed", metrics.Processed,
		"retried", metrics.Retried,
		"succeeded", metrics.Succeeded,
		"failed", metrics.Failed,
		"panics", metrics.Panics,
	)
}
