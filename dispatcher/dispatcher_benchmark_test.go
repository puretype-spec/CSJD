package dispatcher

import (
	"context"
	"fmt"
	"strconv"
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
				RecentDuplicateTTL: time.Nanosecond,
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

			jobs := make([]Job, batchSize)
			for i := 0; i < batchSize; i++ {
				jobs[i] = Job{
					ID:   "bench-" + strconv.Itoa(i),
					Type: "bench",
				}
			}

			b.ResetTimer()
			completed := 0

			for i := 0; i < b.N; i++ {
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

				completed += batchSize
				deadline := time.Now().Add(2 * time.Second)
				for d.Metrics().Succeeded < uint64(completed) {
					if time.Now().After(deadline) {
						b.Fatal("timeout waiting in-flight completion")
					}
					time.Sleep(50 * time.Microsecond)
				}
			}

			b.StopTimer()
		})
	}
}
