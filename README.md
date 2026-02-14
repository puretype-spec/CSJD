# CSJD - Production-Safe Async Jobs in Go

CSJD is a reliability-first job dispatcher for Go.

It supports two modes:
- in-process dispatcher (single service, low operational overhead)
- distributed dispatcher on Redis Streams (multi-node consumer group)

## Why This Exists

Background jobs are easy to start and hard to run safely under real traffic.

Common failure modes:
- unbounded goroutines during retries
- duplicate execution during restart/failover
- retry storms on downstream dependencies
- lost jobs during shutdown
- silent handler panics

CSJD focuses on bounded execution, controlled retries, explicit failure semantics, and recoverability.

## Current Capabilities

- Worker pool (`Config.Workers` / `DistributedConfig.Workers`)
- `Submit` / `SubmitBatch`
- Exponential retry with jitter
- Permanent error shortcut (`MarkPermanent`)
- Queue cap (`MaxPendingJobs`)
- Timeout detach budget (`MaxDetachedHandlers`, opt-in)
- Panic capture
- Metrics snapshot (`submitted`, `accepted`, `processed`, `retried`, `succeeded`, `failed`, `panics`, `detached`)
- `TTLCache` with stampede protection (`GetOrLoad`)
- `BatchLoader` for N+1 batching
- File durable store (`FileJobStore`) with snapshot + WAL + process lock
- Redis Streams distributed dispatcher with consumer group + autoclaim

## Quick Start

```bash
go test ./...
go run ./cmd/demo
```

## In-Process Usage

```go
package main

import (
    "context"
    "time"

    "csjd/dispatcher"
)

func main() {
    store, _ := dispatcher.NewFileJobStore("./data/jobs.json")
    defer store.Close()

    d, _ := dispatcher.New(dispatcher.Config{
        Workers:            4,
        MaxPendingJobs:     10000,
        MaxDetachedHandlers: 0, // keep strict non-overlap default
        Store:              store,
    })

    _ = d.RegisterHandler("email", func(ctx context.Context, job dispatcher.Job) error {
        return nil
    })

    _ = d.Start()
    _ = d.Submit(dispatcher.Job{ID: "job-1", Type: "email"})

    stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    _ = d.Stop(stopCtx)
}
```

## Distributed Usage (Redis Streams)

```go
dist, _ := dispatcher.NewRedisDistributedDispatcher(
    dispatcher.RedisConfig{
        Addr: "127.0.0.1:6379",
    },
    dispatcher.DistributedConfig{
        Workers:            8,
        Stream:             "csjd:jobs",
        Group:              "csjd-workers",
        Consumer:           "node-a",
        MaxPendingJobs:     20000,
        DefaultMaxAttempts: 3,
    },
)

defer dist.Close(context.Background())

_ = dist.RegisterHandler("email", func(ctx context.Context, job dispatcher.Job) error {
    return nil
})

_ = dist.Start()
_ = dist.Submit(dispatcher.Job{ID: "dist-1", Type: "email"})
```

## Delivery Semantics

- In-process mode: local process guarantees only.
- Distributed mode: `at-least-once` delivery.
- Consumer recovery uses `XAUTOCLAIM` for idle pending messages.
- Handlers must be idempotent (or protected by external dedupe keys).

## Reliability Guardrails

- `MaxPendingJobs`: reject overload with `ErrQueueFull`.
- `MaxDetachedHandlers`: optional throughput protection when non-cooperative handlers ignore timeout.
- File store locking: second process opening same store path gets `ErrJobStoreLocked`.

## Testing

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
```

Benchmark:

```bash
go test -run '^$' -bench BenchmarkDispatcherEndToEnd -benchmem ./dispatcher
```

## Tradeoffs and Limits

- No exactly-once guarantee in distributed mode.
- Non-cooperative handlers/fetchers cannot be force-killed in Go.
- Redis Streams mode currently targets one stream/group per dispatcher instance.

