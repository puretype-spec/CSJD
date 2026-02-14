package dispatcher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisConfig controls how distributed dispatcher connects to Redis.
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type redisStreamBackend struct {
	client   *redis.Client
	stream   string
	group    string
	consumer string
}

// NewRedisDistributedDispatcher creates a distributed dispatcher backed by Redis Streams.
func NewRedisDistributedDispatcher(redisCfg RedisConfig, cfg DistributedConfig) (*DistributedDispatcher, error) {
	if redisCfg.Addr == "" {
		return nil, errors.New("redis addr is required")
	}

	normalized, err := cfg.normalize()
	if err != nil {
		return nil, err
	}

	client := redis.NewClient(&redis.Options{
		Addr:     redisCfg.Addr,
		Password: redisCfg.Password,
		DB:       redisCfg.DB,
	})

	if pingErr := client.Ping(context.Background()).Err(); pingErr != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis ping failed: %w", pingErr)
	}

	backend := &redisStreamBackend{
		client:   client,
		stream:   normalized.Stream,
		group:    normalized.Group,
		consumer: normalized.Consumer,
	}

	dispatcher, err := NewDistributedDispatcher(normalized, backend)
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	return dispatcher, nil
}

func (b *redisStreamBackend) EnsureGroup(ctx context.Context) error {
	err := b.client.XGroupCreateMkStream(ctx, b.stream, b.group, "$").Err()
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}

	return err
}

func (b *redisStreamBackend) Add(ctx context.Context, body []byte) (string, error) {
	return b.client.XAdd(ctx, &redis.XAddArgs{
		Stream: b.stream,
		Values: map[string]any{"body": string(body)},
	}).Result()
}

func (b *redisStreamBackend) ReadGroup(ctx context.Context, count int64, block time.Duration) ([]distributedMessage, error) {
	streams, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    b.group,
		Consumer: b.consumer,
		Streams:  []string{b.stream, ">"},
		Count:    count,
		Block:    block,
		NoAck:    false,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	return flattenXStreams(streams), nil
}

func (b *redisStreamBackend) AutoClaim(ctx context.Context, minIdle time.Duration, start string, count int64) ([]distributedMessage, string, error) {
	messages, next, err := b.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   b.stream,
		Group:    b.group,
		Consumer: b.consumer,
		MinIdle:  minIdle,
		Start:    start,
		Count:    count,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, start, nil
		}
		return nil, start, err
	}

	out := make([]distributedMessage, 0, len(messages))
	for _, message := range messages {
		body, ok := getStreamBody(message.Values)
		if !ok {
			continue
		}
		out = append(out, distributedMessage{
			ID:   message.ID,
			Body: body,
		})
	}

	return out, next, nil
}

func (b *redisStreamBackend) Ack(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	return b.client.XAck(ctx, b.stream, b.group, ids...).Err()
}

func (b *redisStreamBackend) Del(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	return b.client.XDel(ctx, b.stream, ids...).Err()
}

func (b *redisStreamBackend) Len(ctx context.Context) (int64, error) {
	return b.client.XLen(ctx, b.stream).Result()
}

func (b *redisStreamBackend) Close() error {
	return b.client.Close()
}

func flattenXStreams(streams []redis.XStream) []distributedMessage {
	total := 0
	for _, stream := range streams {
		total += len(stream.Messages)
	}

	out := make([]distributedMessage, 0, total)
	for _, stream := range streams {
		for _, message := range stream.Messages {
			body, ok := getStreamBody(message.Values)
			if !ok {
				continue
			}
			out = append(out, distributedMessage{
				ID:   message.ID,
				Body: body,
			})
		}
	}

	return out
}

func getStreamBody(values map[string]any) ([]byte, bool) {
	raw, ok := values["body"]
	if !ok {
		return nil, false
	}

	switch value := raw.(type) {
	case string:
		return []byte(value), true
	case []byte:
		return append([]byte(nil), value...), true
	default:
		return []byte(fmt.Sprintf("%v", value)), true
	}
}
