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

// storeScript atomically stores a DLQ message in Redis using a Lua script.
// This ensures the hash, stream entry, stream_id back-reference, and event index
// are all written together or not at all.
//
// KEYS[1] = message hash key (dlq:msg:{id})
// KEYS[2] = stream key (dlq:messages)
// KEYS[3] = event index key (dlq:by_event:{event_name})
//
// ARGV[1]  = number of hash field-value pairs (N)
// ARGV[2..2*N+1] = hash field-value pairs
// ARGV[2*N+2] = maxLen (0 means no limit)
// ARGV[2*N+3] = message ID (for the stream entry value)
var storeScript = redis.NewScript(`
local hashKey = KEYS[1]
local streamKey = KEYS[2]
local eventIndexKey = KEYS[3]

local numFields = tonumber(ARGV[1])
local maxLen = tonumber(ARGV[numFields * 2 + 2])
local msgID = ARGV[numFields * 2 + 3]

-- HSet message hash
local hashArgs = {}
for i = 1, numFields * 2 do
    hashArgs[i] = ARGV[i + 1]
end
redis.call('HSET', hashKey, unpack(hashArgs))

-- XAdd to stream
local streamID
if maxLen > 0 then
    streamID = redis.call('XADD', streamKey, 'MAXLEN', '~', tostring(maxLen), '*', 'id', msgID)
else
    streamID = redis.call('XADD', streamKey, '*', 'id', msgID)
end

-- Store stream entry ID back in hash
redis.call('HSET', hashKey, 'stream_id', streamID)

-- Add to event index
redis.call('SADD', eventIndexKey, msgID)

return streamID
`)

/*
Redis Schema:

Uses Redis Streams and Hashes for DLQ:
- Stream: dlq:messages - all DLQ messages
- Hash: dlq:msg:{id} - individual message details
- Set: dlq:by_event:{event_name} - message IDs by event
- Set: dlq:retried - IDs of retried messages
*/

// RedisStore is a Redis-based DLQ store
type RedisStore struct {
	client         redis.Cmdable
	logger         *slog.Logger
	streamKey      string
	msgPrefix      string
	eventPrefix    string
	retriedKey     string
	originalPrefix string
	maxLen         int64
}

// RedisStoreOption configures a RedisStore.
type RedisStoreOption func(*redisStoreOptions)

type redisStoreOptions struct {
	keyPrefix string
	maxLen    int64
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

// WithMaxLen sets the maximum stream length.
func WithMaxLen(maxLen int64) RedisStoreOption {
	return func(o *redisStoreOptions) {
		if maxLen > 0 {
			o.maxLen = maxLen
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
		streamKey:      o.keyPrefix + "messages",
		msgPrefix:      o.keyPrefix + "msg:",
		eventPrefix:    o.keyPrefix + "by_event:",
		retriedKey:     o.keyPrefix + "retried",
		originalPrefix: o.keyPrefix + "by_original:",
		maxLen:         o.maxLen,
	}, nil
}

// Store adds a message to the DLQ.
// The core operations (hash, stream, stream_id back-reference, event index)
// are executed atomically via a Lua script. The optional reverse-lookup index
// by original ID is written separately since its failure should not roll back
// the main store operation.
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

	// Build flat field-value pairs for the Lua script
	fieldPairs := []interface{}{
		"id", msg.ID,
		"event_name", msg.EventName,
		"original_id", msg.OriginalID,
		"payload", msg.Payload,
		"metadata", metadata,
		"error", msg.Error,
		"retry_count", msg.RetryCount,
		"source", msg.Source,
		"created_at", msg.CreatedAt.Unix(),
	}

	const numFields = 9 // number of key-value pairs in fieldPairs

	// ARGV: [numFields, field1, val1, field2, val2, ..., maxLen, msgID]
	argv := make([]interface{}, 0, numFields*2+3)
	argv = append(argv, numFields)
	argv = append(argv, fieldPairs...)
	argv = append(argv, s.maxLen)
	argv = append(argv, msg.ID)

	keys := []string{msgKey, s.streamKey, eventKey}

	_, err := storeScript.Run(ctx, s.client, keys, argv...).Result()
	if err != nil {
		return fmt.Errorf("store script: %w", err)
	}

	// Add reverse-lookup index by original ID (separate concern).
	// The core data is already committed via the Lua script above,
	// so we log a warning instead of returning an error.
	if msg.OriginalID != "" {
		originalKey := s.originalPrefix + msg.OriginalID
		if err := s.client.Set(ctx, originalKey, msg.ID, 0).Err(); err != nil {
			s.logger.Warn("failed to set original ID index",
				"original_id", msg.OriginalID,
				"message_id", msg.ID,
				"error", err)
		}
	}

	return nil
}

// Get retrieves a single message by ID
func (s *RedisStore) Get(ctx context.Context, id string) (*Message, error) {
	msgKey := s.msgPrefix + id

	fields, err := s.client.HGetAll(ctx, msgKey).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall: %w", err)
	}

	if len(fields) == 0 {
		return nil, fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	return s.parseMessage(fields)
}

// parseMessage converts hash fields to Message
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

// List returns messages matching the filter
func (s *RedisStore) List(ctx context.Context, filter Filter) ([]*Message, error) {
	var ids []string

	if filter.EventName != "" {
		// Get IDs from event index
		eventKey := s.eventPrefix + filter.EventName
		var err error
		ids, err = s.client.SMembers(ctx, eventKey).Result()
		if err != nil {
			return nil, fmt.Errorf("smembers: %w", err)
		}
	} else {
		// Get all IDs from stream
		results, err := s.client.XRange(ctx, s.streamKey, "-", "+").Result()
		if err != nil {
			return nil, fmt.Errorf("xrange: %w", err)
		}
		for _, r := range results {
			if id, ok := r.Values["id"].(string); ok {
				ids = append(ids, id)
			}
		}
	}

	// Batch-fetch all messages using pipeline to avoid N round-trips
	if len(ids) == 0 {
		return nil, nil
	}

	pipe := s.client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HGetAll(ctx, s.msgPrefix+id)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("pipeline hgetall: %w", err)
	}

	// Parse results and apply filters
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

		// Apply filters
		if filter.ExcludeRetried && msg.RetriedAt != nil {
			continue
		}
		if !filter.StartTime.IsZero() && msg.CreatedAt.Before(filter.StartTime) {
			continue
		}
		if !filter.EndTime.IsZero() && msg.CreatedAt.After(filter.EndTime) {
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

	// Apply offset after filtering
	if filter.Offset > 0 {
		if filter.Offset >= len(messages) {
			return nil, nil
		}
		messages = messages[filter.Offset:]
	}

	// Apply limit after filtering
	if filter.Limit > 0 && len(messages) > filter.Limit {
		messages = messages[:filter.Limit]
	}

	return messages, nil
}

// Count returns the number of messages matching the filter.
// When only EventName (or no filter) is set, uses efficient Redis counting.
// For complex filters, falls back to listing and counting matched messages.
func (s *RedisStore) Count(ctx context.Context, filter Filter) (int64, error) {
	hasComplexFilters := filter.ExcludeRetried ||
		!filter.StartTime.IsZero() ||
		!filter.EndTime.IsZero() ||
		filter.MaxRetries > 0 ||
		filter.Source != "" ||
		filter.Error != ""

	if !hasComplexFilters {
		if filter.EventName != "" {
			eventKey := s.eventPrefix + filter.EventName
			return s.client.SCard(ctx, eventKey).Result()
		}
		return s.client.XLen(ctx, s.streamKey).Result()
	}

	// For complex filters, list and count all matching messages
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
// It checks the message exists, sets retried_at, and adds to the retried set.
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

-- Check message exists
if redis.call('EXISTS', msgKey) == 0 then
    return 0
end

-- Set retried_at field
redis.call('HSET', msgKey, 'retried_at', retriedAt)

-- Add to retried set
redis.call('SADD', retriedSetKey, msgID)

return 1
`)

// MarkRetried atomically marks a message as replayed using a Lua script.
// The script checks existence, sets retried_at, and adds to the retried set
// in a single atomic operation.
func (s *RedisStore) MarkRetried(ctx context.Context, id string) error {
	msgKey := s.msgPrefix + id

	keys := []string{msgKey, s.retriedKey}
	result, err := markRetriedScript.Run(ctx, s.client, keys, id, time.Now().Unix()).Int64()
	if err != nil {
		return fmt.Errorf("mark retried script: %w", err)
	}

	if result == 0 {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	return nil
}

// Delete removes a message from the DLQ
func (s *RedisStore) Delete(ctx context.Context, id string) error {
	msgKey := s.msgPrefix + id

	// Get message details needed for cleanup
	fields, err := s.client.HGetAll(ctx, msgKey).Result()
	if err != nil {
		return fmt.Errorf("hgetall: %w", err)
	}
	if len(fields) == 0 {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	// Delete hash
	if err := s.client.Del(ctx, msgKey).Err(); err != nil {
		return fmt.Errorf("del: %w", err)
	}

	// Remove from stream using stored stream entry ID
	if streamID := fields["stream_id"]; streamID != "" {
		if err := s.client.XDel(ctx, s.streamKey, streamID).Err(); err != nil {
			s.logger.Warn("failed to remove stream entry during delete", "id", id, "stream_id", streamID, "error", err)
		}
	}

	// Remove from event index
	if eventName := fields["event_name"]; eventName != "" {
		eventKey := s.eventPrefix + eventName
		if err := s.client.SRem(ctx, eventKey, id).Err(); err != nil {
			s.logger.Warn("failed to remove event index during delete", "id", id, "event_name", eventName, "error", err)
		}
	}

	// Remove from retried set
	if err := s.client.SRem(ctx, s.retriedKey, id).Err(); err != nil {
		s.logger.Warn("failed to remove from retried set during delete", "id", id, "error", err)
	}

	// Remove reverse-lookup index by original ID
	if originalID := fields["original_id"]; originalID != "" {
		if err := s.client.Del(ctx, s.originalPrefix+originalID).Err(); err != nil {
			s.logger.Warn("failed to remove original ID index during delete", "id", id, "original_id", originalID, "error", err)
		}
	}

	return nil
}

// DeleteOlderThan removes messages older than the specified age
func (s *RedisStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	cutoff := time.Now().Add(-age).Unix()

	// Scan all message keys
	var cursor uint64
	var deleted int64

	for {
		keys, nextCursor, err := s.client.Scan(ctx, cursor, s.msgPrefix+"*", 100).Result()
		if err != nil {
			return deleted, fmt.Errorf("scan: %w", err)
		}

		for _, key := range keys {
			createdAt, err := s.client.HGet(ctx, key, "created_at").Int64()
			if err != nil {
				continue
			}

			if createdAt < cutoff {
				id := key[len(s.msgPrefix):]
				if err := s.Delete(ctx, id); err == nil {
					deleted++
				}
			}
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return deleted, nil
}

// DeleteByFilter removes messages matching the filter
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

// Stats returns DLQ statistics
func (s *RedisStore) Stats(ctx context.Context) (*Stats, error) {
	stats := &Stats{
		MessagesByEvent: make(map[string]int64),
		MessagesByError: make(map[string]int64),
	}

	// Total count
	total, _ := s.client.XLen(ctx, s.streamKey).Result()
	stats.TotalMessages = total

	// Retried count
	retried, _ := s.client.SCard(ctx, s.retriedKey).Result()
	stats.RetriedMessages = retried
	stats.PendingMessages = total - retried

	// Count by event - scan event index keys
	var cursor uint64
	for {
		keys, nextCursor, err := s.client.Scan(ctx, cursor, s.eventPrefix+"*", 100).Result()
		if err != nil {
			break
		}

		for _, key := range keys {
			eventName := key[len(s.eventPrefix):]
			count, _ := s.client.SCard(ctx, key).Result()
			stats.MessagesByEvent[eventName] = count
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return stats, nil
}

// GetByOriginalID retrieves a message by its original event message ID
func (s *RedisStore) GetByOriginalID(ctx context.Context, originalID string) (*Message, error) {
	// Look up the DLQ message ID from the reverse index
	originalKey := s.originalPrefix + originalID
	dlqID, err := s.client.Get(ctx, originalKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("original_id %s: %w", originalID, ErrNotFound)
		}
		return nil, fmt.Errorf("get original index: %w", err)
	}

	return s.Get(ctx, dlqID)
}

// Health performs a health check by pinging the Redis server.
// Returns healthy if the ping succeeds, unhealthy otherwise.
func (s *RedisStore) Health(ctx context.Context) *health.Result {
	start := time.Now()

	err := s.client.Ping(ctx).Err()
	if err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("ping failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
			Details: map[string]any{
				"stream_key": s.streamKey,
			},
		}
	}

	// Get message count from stream length
	count, err := s.client.XLen(ctx, s.streamKey).Result()
	if err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("xlen failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
			Details: map[string]any{
				"stream_key": s.streamKey,
			},
		}
	}

	return &health.Result{
		Status:    health.StatusHealthy,
		Latency:   time.Since(start),
		CheckedAt: start,
		Details: map[string]any{
			"stream_key":    s.streamKey,
			"message_count": count,
		},
	}
}

// Compile-time checks
var _ Store = (*RedisStore)(nil)
var _ StatsProvider = (*RedisStore)(nil)
var _ health.Checker = (*RedisStore)(nil)
