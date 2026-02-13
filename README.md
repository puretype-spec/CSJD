# CSJD - Concurrency-Safe Job Dispatcher (Go)

這個專案提供一個完整、可測試、可直接落地的 Golang 非同步工作派發器（job dispatcher）。

## 目標
- concurrency-safe：所有共享狀態都有明確同步保護。
- readable：程式結構小而清楚，函式責任單一。
- production-oriented：支援 worker pool、retry/backoff、duplicate 防護、graceful shutdown、metrics。

## 功能清單
- Worker pool (`Config.Workers`)
- `Submit` / `SubmitBatch`（批次送入可降低 lock 開銷）
- 可選單機持久化（`Config.Store` + `FileJobStore`）
- Queue 上限保護（`Config.MaxPendingJobs`）
- Job retry（指數退避 + jitter）
- Non-retriable error（`dispatcher.MarkPermanent(err)`）
- Duplicate 保護（pending + in-flight + recent TTL window）
- Timeout detach budget（`Config.MaxDetachedHandlers`，預設關閉）
- Graceful stop（`Stop(ctx)`）
- Stop 後可重啟（`Start()`，前提是 handler 在 timeout 內返回或能響應 `ctx`）
- Metrics snapshot
- `TTLCache`（stampede 防護，`GetOrLoad` 單飛）
- `BatchLoader` 合併並行請求
- Benchmark（可比較 worker 規模下的端到端吞吐）
- 結構化日誌示例（`slog` + metrics tags）

## 專案結構
- `dispatcher/dispatcher.go`：dispatcher 核心流程
- `dispatcher/config.go`：設定與驗證
- `dispatcher/types.go`：Job/Handler/Error/Metrics
- `dispatcher/cache.go`：TTL cache + single-flight loader
- `dispatcher/batch_loader.go`：批次載入器
- `dispatcher/store.go`：持久化介面與 `FileJobStore`
- `dispatcher/*_test.go`：單元測試
- `dispatcher/dispatcher_benchmark_test.go`：效能基準測試
- `cmd/demo/main.go`：最小可執行範例

## 快速開始
```bash
go test ./...
go run ./cmd/demo
go test -run '^$' -bench BenchmarkDispatcherEndToEnd -benchmem ./dispatcher
```

## 使用範例
```go
package main

import (
    "context"
    "time"

    "csjd/dispatcher"
)

func example() error {
    d, err := dispatcher.New(dispatcher.Config{Workers: 4})
    if err != nil {
        return err
    }

    if err = d.RegisterHandler("email", func(ctx context.Context, job dispatcher.Job) error {
        // do work
        return nil
    }); err != nil {
        return err
    }

    if err = d.Start(); err != nil {
        return err
    }

    _ = d.Submit(dispatcher.Job{ID: "email-1", Type: "email"})

    stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    return d.Stop(stopCtx)
}
```

## 單機持久化（選配）
```go
store, _ := dispatcher.NewFileJobStore("./data/jobs.json")

d, _ := dispatcher.New(dispatcher.Config{
    Workers:        4,
    MaxPendingJobs: 10000,
    Store:          store,
})
```
- 行為：`Submit/SubmitBatch` 寫入 store，`Start()` 會載入未完成 job，`finish` 後刪除。
- 用途：單機重啟後可恢復未完成工作（不是分散式調度）。
- `FileJobStore` 內部採「snapshot + append-only WAL」降低每次寫入全量重寫成本。
- 同一路徑會做跨進程鎖保護（第二個進程會拿到 `ErrJobStoreLocked`）。

## 如何避免 N+1 / Cache / Async 問題

### 1) 避免 N+1
- 使用 `SubmitBatch` 一次送入多筆工作，減少多次鎖競爭。
- 對於 handler 內的資料讀取，使用 `BatchLoader` 合併同時間多個 key 請求：
```go
loader, _ := dispatcher.NewBatchLoader[int, User](dispatcher.BatchLoaderConfig{}, fetchUsersByIDs)
user, err := loader.Load(ctx, userID)
```

### 2) 避免 cache stampede 與髒資料
- 使用 `TTLCache.GetOrLoad`，同 key 只會有一個 loader 執行，其餘請求等待同一結果。
- 使用 TTL 控制資料新鮮度，並可用 `Cleanup()` 主動清理過期資料。
```go
cache := dispatcher.NewTTLCache[string, Profile](30 * time.Second)
profile, err := cache.GetOrLoad(ctx, "u-1", loadProfile)
```

### 3) 避免 async race / goroutine leak
- Dispatcher 內部共享狀態受鎖保護，排程流程單點管理（scheduler loop）。
- `Stop(ctx)` 有明確超時控制；worker 使用 context timeout。
- handler panic 會被 recover，不會讓 worker goroutine 整批崩潰。
- duplicate TTL cleanup 採節流策略，降低高頻事件下的掃描負擔。

## SonarLint 對齊重點
- 函式短小、責任單一
- 早返回降低巢狀深度
- 明確錯誤語義（permanent vs retryable）
- 無全域可變狀態
- 有測試覆蓋關鍵併發行為

## 測試覆蓋
- Dispatcher 批次併發處理
- Retry/backoff 成功路徑
- Duplicate + recent TTL 行為
- Queue 上限拒絕策略
- Timeout detach budget 行為
- 停機後拒絕新工作
- Stop 後重啟與 timeout 後最終重啟
- Cache 單飛與 TTL 過期
- Batch loader 併批與關閉行為
- File store round-trip / lock / restart restore

## Benchmark（面試展示版）
- Benchmark 檔案：`dispatcher/dispatcher_benchmark_test.go`
- 目的：比較不同 `Workers` 設定下，`SubmitBatch` 到 handler 完成的端到端吞吐。
- 指令：
```bash
go test -run '^$' -bench BenchmarkDispatcherEndToEnd -benchmem ./dispatcher
```
- 讀法：
- `workers=1/4/8`：觀察 worker 擴展性
- `ns/op`：每個 benchmark iteration（一個 batch）耗時
- `B/op`、`allocs/op`：記憶體配置與 GC 壓力
- 目前也包含 hot path 優化：`SubmitBatch` 採 lazy `Errors` 配置、scheduler 改成值回傳避免額外逃逸配置。

## 設計取捨與限制（Interview 可 defend）
- 這是 in-process dispatcher，預設記憶體模式；若需要重啟恢復可接 `FileJobStore` 做單機持久化。
- 不含分散式排程與跨節點協調，不是 XXL-JOB 類完整平台替代品。
- 對不配合 context 的 handler/fetch，Go 無法強制 kill goroutine，只能以 timeout + 隔離策略控風險。
- 可用 `MaxDetachedHandlers`（建議明確設定）在 timeout 後釋放 slot 以維持吞吐，但會接受受限的暫時重疊執行。
- 可用 `MaxPendingJobs` 限制待處理量，避免極端流量下記憶體無上限膨脹。
- `TTLCache` 單飛會共享同一個 in-flight load；每個 caller 可依自己的 context 提前返回，但背景 load 可能繼續到完成。
- 設計優先順序是簡潔、可測試、可讀與單服務可落地，而非重型分散式特性。

## 可觀測性示例（structured log + metrics tags）
- `cmd/demo/main.go` 已示範 `slog` JSON 日誌，包含可篩選欄位：
- `component`、`job_id`、`job_type`、`payload_size`
- 關機後輸出 dispatcher metrics 標籤：
- `submitted`、`accepted`、`processed`、`retried`、`succeeded`、`failed`、`panics`
- 實務上可直接把這些欄位導入 ELK/Loki/Cloud Logging，或轉成 Prometheus 指標維度。
