package dlq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/rbaliyan/event/v3/health"
	"github.com/redis/go-redis/v9"
)

// storeScript atomically stores a DLQ message in Redis.
//
// KEYS[1] = message hash key (dlq:msg:{id})
// KEYS[2] = time index key (dlq:by_time)
// KEYS[3] = event index key (dlq:by_event:{event_name})
//
// ARGV[1]  = number of hash field-value pairs (N)
// ARGV[2..2*N+1] = hash field-value pairs
// ARGV[2*N+2] = score (created_at unix timestamp)
// ARGV[2*N+3] = message ID
var storeScript = redis.NewScript(`
local hashKey = KEYS[1]
local timeKey = KEYS[2]
local eventKey = KEYS[3]

local numFields = tonumber(ARGV[1])
local score = tonumber(ARGV[numFields * 2 + 2])
local msgID = ARGV[numFields * 2 + 3]

-- HSet message hash
local hashArgs = {}
for i = 1, numFields * 2 do
    hashArgs[i] = ARGV[i + 1]
end
redis.call('HSET', hashKey, unpack(hashArgs))

-- Add to time index (primary sorted set for time-range queries)
redis.call('ZADD', timeKey, score, msgID)

-- Add to event index (per-event sorted set for event+time queries)
redis.call('ZADD', eventKey, score, msgID)

return 1
`)

/*
Redis Schema:

Key layout:
  dlq:by_time             sorted set  score=created_at_unix  member=msgID
  dlq:by_event:{name}     sorted set  score=created_at_unix  member=msgID
  dlq:msg:{id}            hash        message fields
  dlq:retried             set         retried msgIDs
  dlq:by_original:{id}    string      → msgID
*/

// RedisStore is a Redis-based DLQ store.
//
// All messages are indexed by creation time in a sorted set (dlq:by_time),
// enabling O(log N + M) time-range queries via ZRANGEBYSCORE rather than
// O(N) stream scans. Per-event sorted sets (dlq:by_event:{name}) allow
// combined event+time queries at the same complexity.
//
// Filters that cannot be pushed to Redis (Source, Error contains, MaxRetries,
// ExcludeRetried) are applied in-memory after the initial sorted-set lookup.
type RedisStore struct {
	client         redis.Cmdable
	logger         *slog.Logger
	timeKey        string // dlq:by_time — primary sorted set
	msgPrefix      string // dlq:msg:
	eventPrefix    string // dlq:by_event:
	retriedKey     string // dlq:retried
	originalPrefix string // dlq:by_original:
}

// RedisStoreOption configures a RedisStore.
type RedisStoreOption func(*redisStoreOptions)

type redisStoreOptions struct {
	keyPrefix string
	logger    *slog.Logger
}

// WithKeyPrefix sets a custom key prefix for all Redis keys.
func WithKeyPrefix(prefix string) RedisStoreOption {
	return func(o *redisStoreOptions) {
		if prefix != "" {
			o.keyPrefix = prefix
		}
	}
}

// WithRedisLogger sets the logger for the Redis store.
func WithRedisLogger(logger *slog.Logger) RedisStoreOption {
	return func(o *redisStoreOptions) {
		if logger != nil {
			o.logger = logger
		}
	}
}

// NewRedisStore creates a new Redis DLQ store.
func NewRedisStore(client redis.Cmdable, opts ...RedisStoreOption) (*RedisStore, error) {
	if client == nil {
		return nil, errors.New("redis: client is required")
	}

	o := &redisStoreOptions{
		keyPrefix: "dlq:",
	}
	for _, opt := range opts {
		opt(o)
	}

	logger := o.logger
	if logger == nil {
		logger = slog.Default()
	}

	return &RedisStore{
		client:         client,
		logger:         logger,
		timeKey:        o.keyPrefix + "by_time",
		msgPrefix:      o.keyPrefix + "msg:",
		eventPrefix:    o.keyPrefix + "by_event:",
		retriedKey:     o.keyPrefix + "retried",
		originalPrefix: o.keyPrefix + "by_original:",
	}, nil
}

// Store adds a message to the DLQ.
// The core operations (hash write, time index, event index) are executed
// atomically via a Lua script. The optional reverse-lookup index by original
// ID is written separately.
func (s *RedisStore) Store(ctx context.Context, msg *Message) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	if msg.ID == "" {
		return fmt.Errorf("message ID is required")
	}

	msgKey := s.msgPrefix + msg.ID
	eventKey := s.eventPrefix + msg.EventName
	metadata, _ := json.Marshal(msg.Metadata)
	score := msg.CreatedAt.Unix()

	fieldPairs := []interface{}{
		"id", msg.ID,
		"event_name", msg.EventName,
		"original_id", msg.OriginalID,
		"payload", msg.Payload,
		"metadata", metadata,
		"error", msg.Error,
		"retry_count", msg.RetryCount,
		"source", msg.Source,
		"created_at", score,
	}

	const numFields = 9

	argv := make([]interface{}, 0, numFields*2+3)
	argv = append(argv, numFields)
	argv = append(argv, fieldPairs...)
	argv = append(argv, score)
	argv = append(argv, msg.ID)

	_, err := storeScript.Run(ctx, s.client, []string{msgKey, s.timeKey, eventKey}, argv...).Result()
	if err != nil {
		return fmt.Errorf("store script: %w", err)
	}

	if msg.OriginalID != "" {
		if err := s.client.Set(ctx, s.originalPrefix+msg.OriginalID, msg.ID, 0).Err(); err != nil {
			s.logger.Warn("failed to set original ID index",
				"original_id", msg.OriginalID,
				"message_id", msg.ID,
				"error", err)
		}
	}

	return nil
}

// Get retrieves a single message by ID.
func (s *RedisStore) Get(ctx context.Context, id string) (*Message, error) {
	fields, err := s.client.HGetAll(ctx, s.msgPrefix+id).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall: %w", err)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	return s.parseMessage(fields)
}

// parseMessage converts hash fields to Message.
func (s *RedisStore) parseMessage(fields map[string]string) (*Message, error) {
	msg := &Message{
		ID:         fields["id"],
		EventName:  fields["event_name"],
		OriginalID: fields["original_id"],
		Payload:    []byte(fields["payload"]),
		Error:      fields["error"],
		Source:     fields["source"],
	}

	if metadata := fields["metadata"]; metadata != "" {
		if err := json.Unmarshal([]byte(metadata), &msg.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}

	if rc := fields["retry_count"]; rc != "" {
		var err error
		msg.RetryCount, err = strconv.Atoi(rc)
		if err != nil {
			return nil, fmt.Errorf("parse retry_count: %w", err)
		}
	}

	if ts := fields["created_at"]; ts != "" {
		unix, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse created_at: %w", err)
		}
		msg.CreatedAt = time.Unix(unix, 0)
	}

	if ts := fields["retried_at"]; ts != "" {
		unix, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse retried_at: %w", err)
		}
		t := time.Unix(unix, 0)
		msg.RetriedAt = &t
	}

	return msg, nil
}

// timeRange builds ZRangeBy min/max from filter time bounds.
func timeRange(filter Filter) *redis.ZRangeBy {
	r := &redis.ZRangeBy{Min: "-inf", Max: "+inf"}
	if !filter.StartTime.IsZero() {
		r.Min = strconv.FormatInt(filter.StartTime.Unix(), 10)
	}
	if !filter.EndTime.IsZero() {
		r.Max = strconv.FormatInt(filter.EndTime.Unix(), 10)
	}
	return r
}

// List returns messages matching the filter.
//
// Time bounds (StartTime, EndTime) are pushed to Redis via ZRANGEBYSCORE for
// O(log N + M) retrieval. EventName selects the per-event sorted set.
// Remaining filters (Source, Error, MaxRetries, ExcludeRetried) are applied
// in-memory after the bulk fetch.
func (s *RedisStore) List(ctx context.Context, filter Filter) ([]*Message, error) {
	key := s.timeKey
	if filter.EventName != "" {
		key = s.eventPrefix + filter.EventName
	}

	ids, err := s.client.ZRangeByScore(ctx, key, timeRange(filter)).Result()
	if err != nil {
		return nil, fmt.Errorf("zrangebyscore: %w", err)
	}

	if len(ids) == 0 {
		return nil, nil
	}

	// Batch-fetch all message hashes in one pipeline.
	pipe := s.client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, s.msgPrefix+id)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("pipeline hgetall: %w", err)
	}

	// Parse and apply in-memory filters.
	var messages []*Message
	for _, cmd := range cmds {
		fields, err := cmd.Result()
		if err != nil || len(fields) == 0 {
			continue
		}
		msg, err := s.parseMessage(fields)
		if err != nil {
			continue
		}
		if filter.ExcludeRetried && msg.RetriedAt != nil {
			continue
		}
		if filter.MaxRetries > 0 && msg.RetryCount > filter.MaxRetries {
			continue
		}
		if filter.Source != "" && msg.Source != filter.Source {
			continue
		}
		if filter.Error != "" && !strings.Contains(strings.ToLower(msg.Error), strings.ToLower(filter.Error)) {
			continue
		}
		messages = append(messages, msg)
	}

	if filter.Offset > 0 {
		if filter.Offset >= len(messages) {
			return nil, nil
		}
		messages = messages[filter.Offset:]
	}
	if filter.Limit > 0 && len(messages) > filter.Limit {
		messages = messages[:filter.Limit]
	}

	return messages, nil
}

// Count returns the number of messages matching the filter.
//
// When no in-memory-only filters are active, uses ZCOUNT for O(log N) counting.
// Falls back to listing and counting when Source, Error, MaxRetries, or
// ExcludeRetried filters are present.
func (s *RedisStore) Count(ctx context.Context, filter Filter) (int64, error) {
	hasComplexFilters := filter.ExcludeRetried ||
		filter.MaxRetries > 0 ||
		filter.Source != "" ||
		filter.Error != ""

	if !hasComplexFilters {
		r := timeRange(filter)
		key := s.timeKey
		if filter.EventName != "" {
			key = s.eventPrefix + filter.EventName
		}
		return s.client.ZCount(ctx, key, r.Min, r.Max).Result()
	}

	countFilter := filter
	countFilter.Offset = 0
	countFilter.Limit = 0
	messages, err := s.List(ctx, countFilter)
	if err != nil {
		return 0, err
	}
	return int64(len(messages)), nil
}

// markRetriedScript atomically marks a DLQ message as retried.
//
// KEYS[1] = message hash key (dlq:msg:{id})
// KEYS[2] = retried set key (dlq:retried)
//
// ARGV[1] = message ID
// ARGV[2] = retried_at unix timestamp
//
// Returns 1 on success, 0 if message not found.
var markRetriedScript = redis.NewScript(`
local msgKey = KEYS[1]
local retriedSetKey = KEYS[2]
local msgID = ARGV[1]
local retriedAt = ARGV[2]

if redis.call('EXISTS', msgKey) == 0 then
    return 0
end

redis.call('HSET', msgKey, 'retried_at', retriedAt)
redis.call('SADD', retriedSetKey, msgID)

return 1
`)

// MarkRetried atomically marks a message as replayed.
func (s *RedisStore) MarkRetried(ctx context.Context, id string) error {
	result, err := markRetriedScript.Run(ctx, s.client,
		[]string{s.msgPrefix + id, s.retriedKey},
		id, time.Now().Unix(),
	).Int64()
	if err != nil {
		return fmt.Errorf("mark retried script: %w", err)
	}
	if result == 0 {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}
	return nil
}

// Delete removes a message from the DLQ and all its index entries.
func (s *RedisStore) Delete(ctx context.Context, id string) error {
	msgKey := s.msgPrefix + id

	fields, err := s.client.HGetAll(ctx, msgKey).Result()
	if err != nil {
		return fmt.Errorf("hgetall: %w", err)
	}
	if len(fields) == 0 {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	if err := s.client.Del(ctx, msgKey).Err(); err != nil {
		return fmt.Errorf("del: %w", err)
	}

	if err := s.client.ZRem(ctx, s.timeKey, id).Err(); err != nil {
		s.logger.Warn("failed to remove from time index during delete", "id", id, "error", err)
	}

	if eventName := fields["event_name"]; eventName != "" {
		if err := s.client.ZRem(ctx, s.eventPrefix+eventName, id).Err(); err != nil {
			s.logger.Warn("failed to remove event index during delete", "id", id, "event_name", eventName, "error", err)
		}
	}

	if err := s.client.SRem(ctx, s.retriedKey, id).Err(); err != nil {
		s.logger.Warn("failed to remove from retried set during delete", "id", id, "error", err)
	}

	if originalID := fields["original_id"]; originalID != "" {
		if err := s.client.Del(ctx, s.originalPrefix+originalID).Err(); err != nil {
			s.logger.Warn("failed to remove original ID index during delete", "id", id, "original_id", originalID, "error", err)
		}
	}

	return nil
}

// DeleteOlderThan removes messages older than the specified age.
// Uses ZRANGEBYSCORE on the time index for O(log N + M) retrieval instead of
// a full SCAN over all message keys.
func (s *RedisStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	cutoff := strconv.FormatInt(time.Now().Add(-age).Unix(), 10)

	ids, err := s.client.ZRangeByScore(ctx, s.timeKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: cutoff,
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("zrangebyscore: %w", err)
	}

	var deleted int64
	for _, id := range ids {
		if err := s.Delete(ctx, id); err == nil {
			deleted++
		}
	}
	return deleted, nil
}

// DeleteByFilter removes messages matching the filter.
func (s *RedisStore) DeleteByFilter(ctx context.Context, filter Filter) (int64, error) {
	messages, err := s.List(ctx, filter)
	if err != nil {
		return 0, err
	}

	var deleted int64
	for _, msg := range messages {
		if err := s.Delete(ctx, msg.ID); err == nil {
			deleted++
		}
	}
	return deleted, nil
}

// Stats returns DLQ statistics.
func (s *RedisStore) Stats(ctx context.Context) (*Stats, error) {
	stats := &Stats{
		MessagesByEvent: make(map[string]int64),
		MessagesByError: make(map[string]int64),
	}

	total, _ := s.client.ZCard(ctx, s.timeKey).Result()
	stats.TotalMessages = total

	retried, _ := s.client.SCard(ctx, s.retriedKey).Result()
	stats.RetriedMessages = retried
	stats.PendingMessages = total - retried

	// Enumerate event names via SCAN; ZCard each event sorted set.
	var cursor uint64
	for {
		keys, nextCursor, err := s.client.Scan(ctx, cursor, s.eventPrefix+"*", 100).Result()
		if err != nil {
			break
		}
		for _, key := range keys {
			eventName := key[len(s.eventPrefix):]
			count, _ := s.client.ZCard(ctx, key).Result()
			stats.MessagesByEvent[eventName] = count
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return stats, nil
}

// GetByOriginalID retrieves a message by its original event message ID.
func (s *RedisStore) GetByOriginalID(ctx context.Context, originalID string) (*Message, error) {
	dlqID, err := s.client.Get(ctx, s.originalPrefix+originalID).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("original_id %s: %w", originalID, ErrNotFound)
		}
		return nil, fmt.Errorf("get original index: %w", err)
	}
	return s.Get(ctx, dlqID)
}

// Health performs a health check by pinging the Redis server.
func (s *RedisStore) Health(ctx context.Context) *health.Result {
	start := time.Now()

	if err := s.client.Ping(ctx).Err(); err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("ping failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
			Details: map[string]any{
				"time_key": s.timeKey,
			},
		}
	}

	count, err := s.client.ZCard(ctx, s.timeKey).Result()
	if err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("zcard failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
			Details: map[string]any{
				"time_key": s.timeKey,
			},
		}
	}

	return &health.Result{
		Status:    health.StatusHealthy,
		Latency:   time.Since(start),
		CheckedAt: start,
		Details: map[string]any{
			"time_key":      s.timeKey,
			"message_count": count,
		},
	}
}

// Compile-time checks
var _ Store = (*RedisStore)(nil)
var _ StatsProvider = (*RedisStore)(nil)
var _ health.Checker = (*RedisStore)(nil)
