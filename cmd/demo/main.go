package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"csjd/dispatcher"
)

func main() {
	d, err := dispatcher.New(dispatcher.Config{
		Workers:            4,
		RetryMinDelay:      100 * time.Millisecond,
		RetryMaxDelay:      2 * time.Second,
		RetryJitter:        0.10,
		DefaultMaxAttempts: 3,
		RecentDuplicateTTL: 2 * time.Second,
	})
	if err != nil {
		log.Fatalf("new dispatcher: %v", err)
	}

	err = d.RegisterHandler("print", func(_ context.Context, job dispatcher.Job) error {
		fmt.Printf("[worker] job=%s type=%s payload=%s\n", job.ID, job.Type, string(job.Payload))
		return nil
	})
	if err != nil {
		log.Fatalf("register handler: %v", err)
	}

	if err = d.Start(); err != nil {
		log.Fatalf("start dispatcher: %v", err)
	}

	jobs := []dispatcher.Job{
		{ID: "demo-1", Type: "print", Payload: []byte("hello")},
		{ID: "demo-2", Type: "print", Payload: []byte("concurrency-safe")},
		{ID: "demo-3", Type: "print", Payload: []byte("job dispatcher")},
	}
	report := d.SubmitBatch(jobs)
	if report.Accepted != len(jobs) {
		log.Printf("batch report: %+v", report)
	}

	time.Sleep(300 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err = d.Stop(stopCtx); err != nil {
		log.Fatalf("stop dispatcher: %v", err)
	}

	fmt.Printf("metrics: %s\n", d.Metrics().String())
}
