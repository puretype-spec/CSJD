package dispatcher

import (
	"errors"
	"time"
)

const (
	defaultWorkers            = 4
	defaultReadyQueueSize     = 256
	defaultRetryMinDelay      = 100 * time.Millisecond
	defaultRetryMaxDelay      = 5 * time.Second
	defaultMaxAttempts        = 3
	defaultJobTimeout         = 10 * time.Second
	defaultRecentDuplicateTTL = 2 * time.Second
)

// Config controls dispatcher behavior.
type Config struct {
	Workers            int
	ReadyQueueSize     int
	RetryMinDelay      time.Duration
	RetryMaxDelay      time.Duration
	RetryJitter        float64
	DefaultMaxAttempts int
	DefaultJobTimeout  time.Duration
	RecentDuplicateTTL time.Duration
	Store              JobStore
}

func (c Config) normalize() (Config, error) {
	cfg := c

	if cfg.Workers == 0 {
		cfg.Workers = defaultWorkers
	}
	if cfg.ReadyQueueSize == 0 {
		cfg.ReadyQueueSize = defaultReadyQueueSize
	}
	if cfg.RetryMinDelay == 0 {
		cfg.RetryMinDelay = defaultRetryMinDelay
	}
	if cfg.RetryMaxDelay == 0 {
		cfg.RetryMaxDelay = defaultRetryMaxDelay
	}
	if cfg.DefaultMaxAttempts == 0 {
		cfg.DefaultMaxAttempts = defaultMaxAttempts
	}
	if cfg.DefaultJobTimeout == 0 {
		cfg.DefaultJobTimeout = defaultJobTimeout
	}
	if cfg.RecentDuplicateTTL == 0 {
		cfg.RecentDuplicateTTL = defaultRecentDuplicateTTL
	}

	if cfg.Workers < 1 {
		return Config{}, errors.New("workers must be >= 1")
	}
	if cfg.ReadyQueueSize < 1 {
		return Config{}, errors.New("ready queue size must be >= 1")
	}
	if cfg.RetryMinDelay < time.Millisecond {
		return Config{}, errors.New("retry min delay must be >= 1ms")
	}
	if cfg.RetryMaxDelay < cfg.RetryMinDelay {
		return Config{}, errors.New("retry max delay must be >= retry min delay")
	}
	if cfg.RetryJitter < 0 || cfg.RetryJitter > 1 {
		return Config{}, errors.New("retry jitter must be in range [0,1]")
	}
	if cfg.DefaultMaxAttempts < 1 {
		return Config{}, errors.New("default max attempts must be >= 1")
	}
	if cfg.DefaultJobTimeout < time.Millisecond {
		return Config{}, errors.New("default job timeout must be >= 1ms")
	}
	if cfg.RecentDuplicateTTL < 0 {
		return Config{}, errors.New("recent duplicate ttl must be >= 0")
	}

	return cfg, nil
}
