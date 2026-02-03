package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

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
	client       redis.Cmdable
	streamKey    string
	msgPrefix    string
	eventPrefix  string
	retriedKey   string
	maxLen       int64
}

// NewRedisStore creates a new Redis DLQ store
func NewRedisStore(client redis.Cmdable) *RedisStore {
	return &RedisStore{
		client:      client,
		streamKey:   "dlq:messages",
		msgPrefix:   "dlq:msg:",
		eventPrefix: "dlq:by_event:",
		retriedKey:  "dlq:retried",
		maxLen:      0,
	}
}

// WithKeyPrefix sets a custom key prefix
func (s *RedisStore) WithKeyPrefix(prefix string) *RedisStore {
	s.streamKey = prefix + "messages"
	s.msgPrefix = prefix + "msg:"
	s.eventPrefix = prefix + "by_event:"
	s.retriedKey = prefix + "retried"
	return s
}

// WithMaxLen sets the maximum stream length
func (s *RedisStore) WithMaxLen(maxLen int64) *RedisStore {
	s.maxLen = maxLen
	return s
}

// Store adds a message to the DLQ
func (s *RedisStore) Store(ctx context.Context, msg *Message) error {
	// Store message details in hash
	msgKey := s.msgPrefix + msg.ID
	metadata, _ := json.Marshal(msg.Metadata)

	fields := map[string]interface{}{
		"id":          msg.ID,
		"event_name":  msg.EventName,
		"original_id": msg.OriginalID,
		"payload":     msg.Payload,
		"metadata":    metadata,
		"error":       msg.Error,
		"retry_count": msg.RetryCount,
		"source":      msg.Source,
		"created_at":  msg.CreatedAt.Unix(),
	}

	if err := s.client.HSet(ctx, msgKey, fields).Err(); err != nil {
		return fmt.Errorf("hset: %w", err)
	}

	// Add to stream for ordering
	args := &redis.XAddArgs{
		Stream: s.streamKey,
		Values: map[string]interface{}{
			"id": msg.ID,
		},
	}
	if s.maxLen > 0 {
		args.MaxLen = s.maxLen
		args.Approx = true
	}

	if err := s.client.XAdd(ctx, args).Err(); err != nil {
		return fmt.Errorf("xadd: %w", err)
	}

	// Add to event index
	eventKey := s.eventPrefix + msg.EventName
	if err := s.client.SAdd(ctx, eventKey, msg.ID).Err(); err != nil {
		return fmt.Errorf("sadd event index: %w", err)
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
		return nil, fmt.Errorf("message not found: %s", id)
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

	// Fetch and filter messages before applying offset/limit
	var messages []*Message
	for _, id := range ids {
		msg, err := s.Get(ctx, id)
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
		filter.Source != ""

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

// MarkRetried marks a message as replayed
func (s *RedisStore) MarkRetried(ctx context.Context, id string) error {
	msgKey := s.msgPrefix + id

	// Update retried_at
	if err := s.client.HSet(ctx, msgKey, "retried_at", time.Now().Unix()).Err(); err != nil {
		return fmt.Errorf("hset: %w", err)
	}

	// Add to retried set
	if err := s.client.SAdd(ctx, s.retriedKey, id).Err(); err != nil {
		return fmt.Errorf("sadd retried: %w", err)
	}

	return nil
}

// Delete removes a message from the DLQ
func (s *RedisStore) Delete(ctx context.Context, id string) error {
	msg, err := s.Get(ctx, id)
	if err != nil {
		return err
	}

	// Delete hash
	msgKey := s.msgPrefix + id
	if err := s.client.Del(ctx, msgKey).Err(); err != nil {
		return fmt.Errorf("del: %w", err)
	}

	// Remove from event index
	eventKey := s.eventPrefix + msg.EventName
	if err := s.client.SRem(ctx, eventKey, id).Err(); err != nil {
		return fmt.Errorf("srem event index: %w", err)
	}

	// Remove from retried set
	if err := s.client.SRem(ctx, s.retriedKey, id).Err(); err != nil {
		return fmt.Errorf("srem retried: %w", err)
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

// Compile-time checks
var _ Store = (*RedisStore)(nil)
var _ StatsProvider = (*RedisStore)(nil)
