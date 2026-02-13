# CSJD - Concurrency-Safe Job Dispatcher (Go)

這個專案提供一個完整、可測試、可直接落地的 Golang 非同步工作派發器（job dispatcher）。

## 目標
- concurrency-safe：所有共享狀態都有明確同步保護。
- readable：程式結構小而清楚，函式責任單一。
- production-oriented：支援 worker pool、retry/backoff、duplicate 防護、graceful shutdown、metrics。

## 功能清單
- Worker pool (`Config.Workers`)
- `Submit` / `SubmitBatch`（批次送入可降低 lock 開銷）
- Job retry（指數退避 + jitter）
- Non-retriable error（`dispatcher.MarkPermanent(err)`）
- Duplicate 保護（pending + in-flight + recent TTL window）
- Graceful stop（`Stop(ctx)`）
- Stop 後可重啟（`Start()`）
- Metrics snapshot
- `TTLCache`（stampede 防護，`GetOrLoad` 單飛）
- `BatchLoader` 合併並行請求

## 專案結構
- `dispatcher/dispatcher.go`：dispatcher 核心流程
- `dispatcher/config.go`：設定與驗證
- `dispatcher/types.go`：Job/Handler/Error/Metrics
- `dispatcher/cache.go`：TTL cache + single-flight loader
- `dispatcher/batch_loader.go`：批次載入器
- `dispatcher/*_test.go`：單元測試
- `cmd/demo/main.go`：最小可執行範例

## 快速開始
```bash
go test ./...
go run ./cmd/demo
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
- 停機後拒絕新工作
- Stop 後重啟與 timeout 後最終重啟
- Cache 單飛與 TTL 過期
- Batch loader 併批與關閉行為
