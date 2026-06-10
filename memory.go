package dlq

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rbaliyan/event/v3/health"
)

// MemoryStore is an in-memory DLQ store for testing
type MemoryStore struct {
	mu       sync.RWMutex
	messages map[string]*Message
	byKey    map[string]string // dedupKey(eventName, originalID) -> message ID
	dedup    bool
}

// MemoryStoreOption configures a MemoryStore.
type MemoryStoreOption func(*MemoryStore)

// WithMemoryDedup enables upsert-on-(EventName, OriginalID): re-storing a message
// with the same event name and (non-empty) original ID increments retry_count on
// the existing row instead of inserting a new one. Default: off.
func WithMemoryDedup() MemoryStoreOption {
	return func(s *MemoryStore) { s.dedup = true }
}

// dedupKey returns the composite dedup index key for a (eventName, originalID) pair.
func dedupKey(eventName, originalID string) string {
	return eventName + "\x00" + originalID
}

// NewMemoryStore creates a new in-memory DLQ store.
func NewMemoryStore(opts ...MemoryStoreOption) *MemoryStore {
	s := &MemoryStore{
		messages: make(map[string]*Message),
		byKey:    make(map[string]string),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Store adds a message to the DLQ. When dedup is enabled and msg.OriginalID is
// non-empty, a duplicate (EventName, OriginalID) pair upserts the existing row:
// RetryCount is incremented, Error/Payload/Metadata are updated to the latest
// values, RetriedAt is cleared, and CreatedAt is preserved as first-seen.
func (s *MemoryStore) Store(ctx context.Context, msg *Message) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	if msg.ID == "" {
		return fmt.Errorf("message ID is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.dedup && msg.OriginalID != "" {
		key := dedupKey(msg.EventName, msg.OriginalID)
		if existingID, ok := s.byKey[key]; ok {
			if existing, ok2 := s.messages[existingID]; ok2 {
				existing.RetryCount++
				existing.Error = msg.Error
				existing.Payload = msg.Payload
				existing.RetriedAt = nil
				if msg.Metadata != nil {
					existing.Metadata = make(map[string]string, len(msg.Metadata))
					for k, v := range msg.Metadata {
						existing.Metadata[k] = v
					}
				} else {
					existing.Metadata = nil
				}
				// CreatedAt and QuarantinedAt are intentionally preserved (first-seen).
				return nil
			}
		}
		s.byKey[key] = msg.ID
	}

	// Make a copy
	stored := *msg
	if msg.Metadata != nil {
		stored.Metadata = make(map[string]string)
		for k, v := range msg.Metadata {
			stored.Metadata[k] = v
		}
	}

	s.messages[msg.ID] = &stored
	return nil
}

// Get retrieves a single message by ID
func (s *MemoryStore) Get(ctx context.Context, id string) (*Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msg, ok := s.messages[id]
	if !ok {
		return nil, fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	// Return a copy
	result := *msg
	if msg.Metadata != nil {
		result.Metadata = make(map[string]string, len(msg.Metadata))
		for k, v := range msg.Metadata {
			result.Metadata[k] = v
		}
	}
	return &result, nil
}

// List returns messages matching the filter
func (s *MemoryStore) List(ctx context.Context, filter Filter) ([]*Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var messages []*Message

	for _, msg := range s.messages {
		if s.matchesFilter(msg, filter) {
			result := *msg
			if msg.Metadata != nil {
				result.Metadata = make(map[string]string, len(msg.Metadata))
				for k, v := range msg.Metadata {
					result.Metadata[k] = v
				}
			}
			messages = append(messages, &result)
		}
	}

	// Apply pagination
	start := filter.Offset
	if start >= len(messages) {
		return nil, nil
	}

	end := len(messages)
	if filter.Limit > 0 && start+filter.Limit < end {
		end = start + filter.Limit
	}

	return messages[start:end], nil
}

// Count returns the number of messages matching the filter
func (s *MemoryStore) Count(ctx context.Context, filter Filter) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var count int64
	for _, msg := range s.messages {
		if s.matchesFilter(msg, filter) {
			count++
		}
	}

	return count, nil
}

// matchesFilter checks if a message matches the filter criteria
func (s *MemoryStore) matchesFilter(msg *Message, filter Filter) bool {
	if filter.EventName != "" && msg.EventName != filter.EventName {
		return false
	}

	if !filter.After.IsZero() && msg.CreatedAt.Before(filter.After) {
		return false
	}

	if !filter.Before.IsZero() && msg.CreatedAt.After(filter.Before) {
		return false
	}

	if filter.Error != "" && !strings.Contains(strings.ToLower(msg.Error), strings.ToLower(filter.Error)) {
		return false
	}

	if filter.MaxRetries > 0 && msg.RetryCount > filter.MaxRetries {
		return false
	}

	if filter.Source != "" && msg.Source != filter.Source {
		return false
	}

	if filter.ExcludeRetried && msg.RetriedAt != nil {
		return false
	}

	if filter.ExcludeQuarantined && msg.QuarantinedAt != nil {
		return false
	}

	return true
}

// MarkRetried marks a message as replayed
func (s *MemoryStore) MarkRetried(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	msg, ok := s.messages[id]
	if !ok {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	now := time.Now()
	msg.RetriedAt = &now
	return nil
}

// Quarantine marks a message as a terminal, non-retryable failure.
func (s *MemoryStore) Quarantine(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	msg, ok := s.messages[id]
	if !ok {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}
	now := time.Now()
	msg.QuarantinedAt = &now
	return nil
}

// Delete removes a message from the DLQ
func (s *MemoryStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	msg, ok := s.messages[id]
	if !ok {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	if s.dedup && msg.OriginalID != "" {
		delete(s.byKey, dedupKey(msg.EventName, msg.OriginalID))
	}
	delete(s.messages, id)
	return nil
}

// DeleteOlderThan removes messages older than the specified age
func (s *MemoryStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-age)
	var deleted int64

	for id, msg := range s.messages {
		if msg.CreatedAt.Before(cutoff) {
			if s.dedup && msg.OriginalID != "" {
				delete(s.byKey, dedupKey(msg.EventName, msg.OriginalID))
			}
			delete(s.messages, id)
			deleted++
		}
	}

	return deleted, nil
}

// DeleteByFilter removes messages matching the filter
func (s *MemoryStore) DeleteByFilter(ctx context.Context, filter Filter) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var deleted int64

	for id, msg := range s.messages {
		if s.matchesFilter(msg, filter) {
			if s.dedup && msg.OriginalID != "" {
				delete(s.byKey, dedupKey(msg.EventName, msg.OriginalID))
			}
			delete(s.messages, id)
			deleted++
		}
	}

	return deleted, nil
}

// Stats returns DLQ statistics
func (s *MemoryStore) Stats(ctx context.Context) (*Stats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := &Stats{
		MessagesByEvent: make(map[string]int64),
		MessagesByError: make(map[string]int64),
	}

	var oldest, newest *time.Time

	for _, msg := range s.messages {
		stats.TotalMessages++

		if msg.RetriedAt != nil {
			stats.RetriedMessages++
		} else {
			stats.PendingMessages++
		}

		stats.MessagesByEvent[msg.EventName]++

		// Track error types (simplified - just use first word)
		errorType := msg.Error
		if idx := strings.Index(errorType, ":"); idx > 0 {
			errorType = errorType[:idx]
		}
		stats.MessagesByError[errorType]++

		if oldest == nil || msg.CreatedAt.Before(*oldest) {
			t := msg.CreatedAt
			oldest = &t
		}
		if newest == nil || msg.CreatedAt.After(*newest) {
			t := msg.CreatedAt
			newest = &t
		}
	}

	stats.OldestMessage = oldest
	stats.NewestMessage = newest

	return stats, nil
}

// GetByOriginalID retrieves a message by its original event message ID
func (s *MemoryStore) GetByOriginalID(ctx context.Context, originalID string) (*Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, msg := range s.messages {
		if msg.OriginalID == originalID {
			result := *msg
			if msg.Metadata != nil {
				result.Metadata = make(map[string]string, len(msg.Metadata))
				for k, v := range msg.Metadata {
					result.Metadata[k] = v
				}
			}
			return &result, nil
		}
	}

	return nil, fmt.Errorf("original_id %s: %w", originalID, ErrNotFound)
}

// Health performs a health check on the in-memory store.
// Always returns healthy with the current message count.
func (s *MemoryStore) Health(ctx context.Context) *health.Result {
	s.mu.RLock()
	count := len(s.messages)
	s.mu.RUnlock()

	return &health.Result{
		Status:    health.StatusHealthy,
		CheckedAt: time.Now(),
		Details: map[string]any{
			"message_count": count,
		},
	}
}

// Compile-time checks
var _ Store = (*MemoryStore)(nil)
var _ StatsProvider = (*MemoryStore)(nil)
var _ Quarantiner = (*MemoryStore)(nil)
var _ health.Checker = (*MemoryStore)(nil)
