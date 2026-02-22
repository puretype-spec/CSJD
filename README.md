# CSJD — Production-Safe Async Jobs in Go

> Most Go worker pools work perfectly…
> until real traffic hits.

This repository exists because background jobs are deceptively simple in development, but fail in production.

Typical failures we encountered:

* Goroutine leaks after retries
* Duplicate jobs after service restart
* Retry storms killing the database
* Cache stampede during traffic spikes
* Lost tasks during shutdown
* Panics silently skipping jobs
* "Exactly once" turning into "at least 10 times"

Most job queue examples don't handle these.

CSJD demonstrates a **production-oriented async job execution model** — not just concurrency.

---

## What problem does this solve?

A typical Go worker pool:

```go
for job := range jobs {
    go handle(job)
}
```

Looks fine — until:

* context cancellation doesn't propagate
* retry creates unbounded goroutines
* multiple instances process same job
* graceful shutdown drops tasks
* DB / API meltdown due to retry bursts

CSJD implements defensive patterns used in high-traffic backend systems.

---

## Design Goals

Instead of maximizing throughput, we optimize for **system survival**.

| Concern         | Typical Worker Pool     | CSJD                     |
| --------------- | ----------------------- | ------------------------ |
| Retry control   | ❌ none                  | exponential backoff      |
| Idempotency     | ❌ caller responsibility | built-in protection      |
| Duplicate jobs  | ❌ possible              | prevented                |
| Cache stampede  | ❌                       | singleflight + TTL cache |
| Shutdown safety | ❌ drop tasks            | drain + handoff          |
| Panic safety    | ❌ lost jobs             | captured + retry         |
| Backpressure    | ❌ unlimited goroutines  | bounded concurrency      |

---

## Core Concepts

### 1) Bounded Concurrency

Prevent goroutine explosions during spikes.

### 2) Idempotent Execution

A job may run multiple times — your system must behave as once.

### 3) Retry With Control

Retries must not become a denial-of-service attack on your own database.

### 4) Graceful Shutdown Safety

Server restarts must not corrupt system state.

---

## Example

```go
dispatcher := csjd.NewDispatcher(10)

dispatcher.Submit("send_email", userID, func(ctx context.Context) error {
    return emailService.Send(ctx, userID)
})
```

The dispatcher guarantees:

* bounded execution
* retry safety
* panic recovery
* duplicate suppression
* controlled shutdown

---

## Why this project exists

After running high-traffic backend services, we discovered:

Most outages were not caused by business logic —
they were caused by background jobs behaving badly under load.

This repo documents the patterns we now consider mandatory.

---

## Roadmap

Planned improvements:

* Distributed coordinator (multi-instance safety)
* Redis storage backend
* Dead letter queue
* Metrics exporter (Prometheus)
* Cron scheduler
* Circuit breaker integration

---

## Who is this for?

Backend engineers building systems where failure matters:

* payment systems
* notification services
* event processing
* external API integrations
* high traffic platforms

If your system must survive restarts, spikes, and partial outages — this is relevant.

---

## Non-Goals

This is **not** a high-throughput message queue replacement.
This is a **reliability-focused execution model**.

---

## Inspiration

Real production incidents.

---

If this project saved you from debugging a 3AM outage,
please consider ⭐ starring the repository.
