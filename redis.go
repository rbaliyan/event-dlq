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

// storeDedupScript atomically upserts a DLQ message when dedup is enabled.
//
// KEYS[1] = composite dedup key (dlq:by_eventorig:{event}:{original})
//
// ARGV positional layout:
//
//	1  msgPrefix      (e.g. "dlq:msg:")
//	2  timeKey        (e.g. "dlq:by_time")
//	3  eventKey       (e.g. "dlq:by_event:{event_name}")
//	4  retriedKey     (e.g. "dlq:retried")
//	5  originalKey    (e.g. "dlq:by_original:{original_id}")
//	6  score          (created_at unix seconds, numeric)
//	7  msgID
//	8  event_name
//	9  original_id
//	10 error
//	11 payload
//	12 metadata
//	13 source
//	14 retry_count    (initial, as string; only used on fresh insert)
//
// NOTE: This script is NOT Redis-Cluster-safe. It builds hash keys dynamically
// from ARGV-supplied prefixes (msgPrefix + existingID / msgID), which means
// those keys may hash to different slots than KEYS[1]. The existing RedisStore
// already has the same limitation for its side-index SET writes, so this is an
// accepted constraint: the store is designed for single-node / Sentinel Redis.
var storeDedupScript = redis.NewScript(`
local compositeKey = KEYS[1]
local msgPrefix    = ARGV[1]
local timeKey      = ARGV[2]
local eventKey     = ARGV[3]
local retriedKey   = ARGV[4]
local originalKey  = ARGV[5]
local score        = tonumber(ARGV[6])
local msgID        = ARGV[7]
local event_name   = ARGV[8]
local original_id  = ARGV[9]
local err          = ARGV[10]
local payload      = ARGV[11]
local metadata     = ARGV[12]
local source       = ARGV[13]
local retry_count  = ARGV[14]

local existing = redis.call('GET', compositeKey)
if existing then
  -- Upsert path: update existing message in-place.
  local hk = msgPrefix .. existing
  redis.call('HINCRBY', hk, 'retry_count', 1)
  redis.call('HSET', hk, 'error', err, 'payload', payload, 'metadata', metadata, 'retried_at', '')
  redis.call('SREM', retriedKey, existing)
  return existing
else
  -- Insert path: brand-new message.
  local hk = msgPrefix .. msgID
  redis.call('HSET', hk,
    'id',          msgID,
    'event_name',  event_name,
    'original_id', original_id,
    'payload',     payload,
    'metadata',    metadata,
    'error',       err,
    'retry_count', retry_count,
    'source',      source,
    'created_at',  score)
  redis.call('ZADD', timeKey,  score, msgID)
  redis.call('ZADD', eventKey, score, msgID)
  redis.call('SET', compositeKey, msgID)
  redis.call('SET', originalKey,  msgID)
  return msgID
end
`)

/*
Redis Schema:

Key layout:
  dlq:by_time                      sorted set  score=created_at_unix  member=msgID
  dlq:by_event:{name}              sorted set  score=created_at_unix  member=msgID
  dlq:msg:{id}                     hash        message fields
  dlq:retried                      set         retried msgIDs
  dlq:quarantined                  set         quarantined msgIDs
  dlq:by_original:{id}             string      → msgID
  dlq:by_eventorig:{event}:{orig}  string      → msgID  (dedup composite index)
*/

// RedisStore is a Redis-based DLQ store.
//
// All messages are indexed by creation time in a sorted set (dlq:by_time),
// enabling O(log N + M) time-range queries via ZRANGEBYSCORE rather than
// O(N) stream scans. Per-event sorted sets (dlq:by_event:{name}) allow
// combined event+time queries at the same complexity.
//
// Filters that cannot be pushed to Redis (Source, Error contains, MaxRetries,
// ExcludeRetried, ExcludeQuarantined) are applied in-memory after the initial
// sorted-set lookup.
type RedisStore struct {
	client          redis.Cmdable
	logger          *slog.Logger
	timeKey         string // dlq:by_time — primary sorted set
	msgPrefix       string // dlq:msg:
	eventPrefix     string // dlq:by_event:
	retriedKey      string // dlq:retried
	quarantinedKey  string // dlq:quarantined
	originalPrefix  string // dlq:by_original:
	eventOrigPrefix string // dlq:by_eventorig: — composite (event,original) dedup index
	dedup           bool   // when true, Store upserts on (event_name, original_id)
}

// RedisStoreOption configures a RedisStore.
type RedisStoreOption func(*redisStoreOptions)

type redisStoreOptions struct {
	keyPrefix string
	logger    *slog.Logger
	dedup     bool
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

// WithRedisDedup enables opt-in deduplication for the Redis store.
//
// When enabled, storing a message whose OriginalID is non-empty and whose
// (EventName, OriginalID) pair already exists in the store will upsert
// rather than insert: the retry count is incremented, the error/payload/
// metadata fields are updated to the latest values, any retried state is
// cleared, and the original created_at timestamp is preserved. Messages
// with an empty OriginalID are always inserted as distinct entries.
//
// Dedup is OFF by default.
func WithRedisDedup() RedisStoreOption {
	return func(o *redisStoreOptions) {
		o.dedup = true
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
		client:          client,
		logger:          logger,
		timeKey:         o.keyPrefix + "by_time",
		msgPrefix:       o.keyPrefix + "msg:",
		eventPrefix:     o.keyPrefix + "by_event:",
		retriedKey:      o.keyPrefix + "retried",
		quarantinedKey:  o.keyPrefix + "quarantined",
		originalPrefix:  o.keyPrefix + "by_original:",
		eventOrigPrefix: o.keyPrefix + "by_eventorig:",
		dedup:           o.dedup,
	}, nil
}

// Store adds a message to the DLQ.
//
// When dedup is enabled and msg.OriginalID is non-empty, the operation is
// routed through storeDedupScript which atomically upserts on the composite
// (event_name, original_id) key. Otherwise the existing storeScript path is
// used unchanged.
//
// The core non-dedup operations (hash write, time index, event index) are
// executed atomically via storeScript. The optional reverse-lookup index by
// original ID is written separately on the non-dedup path only (the dedup
// script writes both indexes atomically).
func (s *RedisStore) Store(ctx context.Context, msg *Message) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	if msg.ID == "" {
		return fmt.Errorf("message ID is required")
	}

	metadata, _ := json.Marshal(msg.Metadata)
	score := msg.CreatedAt.Unix()
	eventKey := s.eventPrefix + msg.EventName

	// Dedup path: route through atomic upsert script.
	if s.dedup && msg.OriginalID != "" {
		compositeKey := s.eventOrigPrefix + msg.EventName + ":" + msg.OriginalID
		argv := []interface{}{
			s.msgPrefix,
			s.timeKey,
			eventKey,
			s.retriedKey,
			s.originalPrefix + msg.OriginalID,
			score,
			msg.ID,
			msg.EventName,
			msg.OriginalID,
			msg.Error,
			string(msg.Payload),
			string(metadata),
			msg.Source,
			msg.RetryCount,
		}
		if _, err := storeDedupScript.Run(ctx, s.client, []string{compositeKey}, argv...).Result(); err != nil {
			return fmt.Errorf("store dedup script: %w", err)
		}
		return nil
	}

	// Non-dedup path (existing behaviour — unchanged).
	msgKey := s.msgPrefix + msg.ID

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

	if _, err := storeScript.Run(ctx, s.client, []string{msgKey, s.timeKey, eventKey}, argv...).Result(); err != nil {
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

	if ts := fields["quarantined_at"]; ts != "" {
		unix, err := strconv.ParseInt(ts, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse quarantined_at: %w", err)
		}
		t := time.Unix(unix, 0)
		msg.QuarantinedAt = &t
	}

	return msg, nil
}

// timeRange builds ZRangeBy min/max from filter time bounds.
func timeRange(filter Filter) *redis.ZRangeBy {
	r := &redis.ZRangeBy{Min: "-inf", Max: "+inf"}
	if !filter.After.IsZero() {
		r.Min = strconv.FormatInt(filter.After.Unix(), 10)
	}
	if !filter.Before.IsZero() {
		r.Max = strconv.FormatInt(filter.Before.Unix(), 10)
	}
	return r
}

// List returns messages matching the filter.
//
// Time bounds (After, Before) are pushed to Redis via ZRANGEBYSCORE for
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
		if filter.ExcludeQuarantined && msg.QuarantinedAt != nil {
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
		filter.ExcludeQuarantined ||
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

// quarantineScript atomically marks a DLQ message as quarantined.
//
// KEYS[1] = message hash key (dlq:msg:{id})
// KEYS[2] = quarantined set key (dlq:quarantined)
//
// ARGV[1] = message ID
// ARGV[2] = quarantined_at unix timestamp
//
// Returns 1 on success, 0 if message not found.
var quarantineScript = redis.NewScript(`
local msgKey = KEYS[1]
local quarantinedSetKey = KEYS[2]
local msgID = ARGV[1]
local quarantinedAt = ARGV[2]

if redis.call('EXISTS', msgKey) == 0 then
    return 0
end

redis.call('HSET', msgKey, 'quarantined_at', quarantinedAt)
redis.call('SADD', quarantinedSetKey, msgID)

return 1
`)

// Quarantine marks a message as a terminal, non-retryable failure.
func (s *RedisStore) Quarantine(ctx context.Context, id string) error {
	result, err := quarantineScript.Run(ctx, s.client,
		[]string{s.msgPrefix + id, s.quarantinedKey},
		id, time.Now().Unix(),
	).Int64()
	if err != nil {
		return fmt.Errorf("quarantine script: %w", err)
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

	if err := s.client.SRem(ctx, s.quarantinedKey, id).Err(); err != nil {
		s.logger.Warn("failed to remove from quarantined set during delete", "id", id, "error", err)
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

	total, err := s.client.ZCard(ctx, s.timeKey).Result()
	if err != nil {
		return nil, fmt.Errorf("stats: total count: %w", err)
	}
	stats.TotalMessages = total

	retried, err := s.client.SCard(ctx, s.retriedKey).Result()
	if err != nil {
		return nil, fmt.Errorf("stats: retried count: %w", err)
	}
	stats.RetriedMessages = retried

	quarantined, err := s.client.SCard(ctx, s.quarantinedKey).Result()
	if err != nil {
		return nil, fmt.Errorf("stats: quarantined count: %w", err)
	}
	stats.QuarantinedMessages = quarantined

	// Pending = total - |retried ∪ quarantined|: a message is pending iff it is
	// neither retried nor quarantined. Computing the union cardinality avoids
	// double-counting messages that are in both sets.
	unionMembers, err := s.client.SUnion(ctx, s.retriedKey, s.quarantinedKey).Result()
	if err != nil {
		return nil, fmt.Errorf("stats: union of retried and quarantined: %w", err)
	}
	nonPending := int64(len(unionMembers))
	if pending := total - nonPending; pending > 0 {
		stats.PendingMessages = pending
	}

	// Enumerate event names via SCAN; ZCard each event sorted set.
	var cursor uint64
	for {
		keys, nextCursor, err := s.client.Scan(ctx, cursor, s.eventPrefix+"*", 100).Result()
		if err != nil {
			return nil, fmt.Errorf("stats: scan event keys: %w", err)
		}
		for _, key := range keys {
			eventName := key[len(s.eventPrefix):]
			count, err := s.client.ZCard(ctx, key).Result()
			if err != nil {
				return nil, fmt.Errorf("stats: count for event %q: %w", eventName, err)
			}
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
// Backend returns the store's backend name for metric labelling.
func (s *RedisStore) Backend() string { return "redis" }

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

// MigrateDedup collapses historical duplicate (event_name, original_id) groups
// into a single entry each: keeps the newest by CreatedAt (tiebreak by ID),
// sums RetryCount across the group, sets CreatedAt to the oldest in the group,
// and sets QuarantinedAt to the earliest non-nil QuarantinedAt (if any).
// The composite dedup index (eventOrigPrefix) and original-ID index are updated
// to point to the survivor so that subsequent dedup-on writes collapse correctly.
//
// MigrateDedup refuses to delete more than 50% of all messages unless WithForce
// is passed. Returns the number of non-survivor entries removed. Idempotent.
func (s *RedisStore) MigrateDedup(ctx context.Context, opts ...MigrateDedupOption) (int64, error) {
	o := applyMigrateDedupOptions(opts)

	// 1. Enumerate all message IDs from the primary time index.
	allIDs, err := s.client.ZRange(ctx, s.timeKey, 0, -1).Result()
	if err != nil {
		return 0, fmt.Errorf("zrange: %w", err)
	}
	total := int64(len(allIDs))

	// 2. Fetch all messages.
	msgs := make([]*Message, 0, len(allIDs))
	for _, id := range allIDs {
		fields, ferr := s.client.HGetAll(ctx, s.msgPrefix+id).Result()
		if ferr != nil || len(fields) == 0 {
			continue
		}
		m, perr := s.parseMessage(fields)
		if perr != nil {
			continue
		}
		msgs = append(msgs, m)
	}

	// 3. Group by (EventName, OriginalID) for messages with a non-empty OriginalID.
	type groupKey struct{ event, orig string }
	groups := make(map[groupKey][]*Message)
	for _, m := range msgs {
		if m.OriginalID == "" {
			continue
		}
		k := groupKey{m.EventName, m.OriginalID}
		groups[k] = append(groups[k], m)
	}

	// 4. Compute toDelete across multi-member groups only.
	var toDelete int64
	for _, g := range groups {
		if len(g) > 1 {
			toDelete += int64(len(g) - 1)
		}
	}

	// 5. Safety guard.
	if !o.force && total > 0 && toDelete*2 > total {
		return 0, fmt.Errorf(
			"dlq: MigrateDedup would delete %d of %d messages (>50%%); pass WithForce to proceed",
			toDelete, total,
		)
	}

	// 6. Collapse each multi-member group.
	var removed int64
	for k, group := range groups {
		if len(group) <= 1 {
			continue
		}

		// Identify the survivor: newest CreatedAt, tiebreak by ID (lexicographic).
		survivor := group[0]
		for _, m := range group[1:] {
			if m.CreatedAt.After(survivor.CreatedAt) ||
				(m.CreatedAt.Equal(survivor.CreatedAt) && m.ID > survivor.ID) {
				survivor = m
			}
		}

		// Compute aggregates across all group members.
		var sumRetry int
		minCreated := survivor.CreatedAt
		var minQuarantined *time.Time
		for _, m := range group {
			sumRetry += m.RetryCount
			if m.CreatedAt.Before(minCreated) {
				minCreated = m.CreatedAt
			}
			if m.QuarantinedAt != nil {
				if minQuarantined == nil || m.QuarantinedAt.Before(*minQuarantined) {
					qt := *m.QuarantinedAt
					minQuarantined = &qt
				}
			}
		}

		// Delete each non-survivor (reuse Delete which cleans all indexes).
		for _, m := range group {
			if m.ID == survivor.ID {
				continue
			}
			// Delete removes originalPrefix+m.OriginalID too, but since all
			// duplicates share the same OriginalID and we re-point it below,
			// that is harmless.
			if derr := s.Delete(ctx, m.ID); derr == nil {
				removed++
			}
		}

		// Update survivor hash with aggregated values.
		survivorKey := s.msgPrefix + survivor.ID
		hsetArgs := []interface{}{
			"retry_count", sumRetry,
			"created_at", minCreated.Unix(),
		}
		if minQuarantined != nil {
			hsetArgs = append(hsetArgs, "quarantined_at", minQuarantined.Unix())
			if aerr := s.client.SAdd(ctx, s.quarantinedKey, survivor.ID).Err(); aerr != nil {
				s.logger.Warn("failed to add survivor to quarantined set", "id", survivor.ID, "error", aerr)
			}
		}
		if herr := s.client.HSet(ctx, survivorKey, hsetArgs...).Err(); herr != nil {
			return removed, fmt.Errorf("hset survivor %s: %w", survivor.ID, herr)
		}

		// Update primary time-index score to minCreated so ordering reflects first-seen.
		if zerr := s.client.ZAdd(ctx, s.timeKey, redis.Z{
			Score:  float64(minCreated.Unix()),
			Member: survivor.ID,
		}).Err(); zerr != nil {
			s.logger.Warn("failed to update time index score for survivor", "id", survivor.ID, "error", zerr)
		}
		// Update per-event index score as well.
		if zerr := s.client.ZAdd(ctx, s.eventPrefix+k.event, redis.Z{
			Score:  float64(minCreated.Unix()),
			Member: survivor.ID,
		}).Err(); zerr != nil {
			s.logger.Warn("failed to update event index score for survivor", "id", survivor.ID, "error", zerr)
		}

		// Point both composite and original indexes at the survivor.
		compositeKey := s.eventOrigPrefix + k.event + ":" + k.orig
		if serr := s.client.Set(ctx, compositeKey, survivor.ID, 0).Err(); serr != nil {
			return removed, fmt.Errorf("set composite key %s: %w", compositeKey, serr)
		}
		if serr := s.client.Set(ctx, s.originalPrefix+k.orig, survivor.ID, 0).Err(); serr != nil {
			return removed, fmt.Errorf("set original key %s: %w", k.orig, serr)
		}
	}

	return removed, nil
}

// Compile-time checks
var _ Store = (*RedisStore)(nil)
var _ StatsProvider = (*RedisStore)(nil)
var _ Quarantiner = (*RedisStore)(nil)
var _ health.Checker = (*RedisStore)(nil)
