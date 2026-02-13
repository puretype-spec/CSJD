package dispatcher

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func BenchmarkDispatcherEndToEnd(b *testing.B) {
	workerOptions := []int{1, 4, 8}
	for _, workers := range workerOptions {
		b.Run(fmt.Sprintf("workers=%d", workers), func(b *testing.B) {
			d, err := New(Config{
				Workers:            workers,
				RetryJitter:        0,
				RecentDuplicateTTL: 0,
				DefaultJobTimeout:  2 * time.Second,
			})
			if err != nil {
				b.Fatalf("new dispatcher: %v", err)
			}

			const batchSize = 64
			done := make(chan struct{}, batchSize)
			err = d.RegisterHandler("bench", func(_ context.Context, _ Job) error {
				done <- struct{}{}
				return nil
			})
			if err != nil {
				b.Fatalf("register handler: %v", err)
			}

			if err = d.Start(); err != nil {
				b.Fatalf("start dispatcher: %v", err)
			}
			defer func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if stopErr := d.Stop(stopCtx); stopErr != nil {
					b.Fatalf("stop dispatcher: %v", stopErr)
				}
			}()

			jobSeq := 0
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				jobs := make([]Job, batchSize)
				for j := 0; j < batchSize; j++ {
					jobSeq++
					jobs[j] = Job{
						ID:   fmt.Sprintf("bench-%d", jobSeq),
						Type: "bench",
					}
				}

				report := d.SubmitBatch(jobs)
				if report.Accepted != batchSize {
					b.Fatalf("batch submit accepted=%d want=%d", report.Accepted, batchSize)
				}

				for j := 0; j < batchSize; j++ {
					select {
					case <-done:
					case <-time.After(2 * time.Second):
						b.Fatal("timeout waiting benchmark batch completion")
					}
				}
			}

			b.StopTimer()
		})
	}
}
