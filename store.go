// Package dlq provides dead-letter queue management and reprocessing capabilities.
//
// Dead-letter queues store messages that failed processing after all retries.
// This package provides:
//   - Storage of failed messages with error details
//   - Filtering and listing of DLQ messages
//   - Replay functionality to reprocess messages
//   - Cleanup of old messages
//
// # Overview
//
// The DLQ pattern is essential for handling message processing failures:
//   - Prevent message loss when handlers fail permanently
//   - Enable manual intervention and debugging
//   - Support automated replay after fixing issues
//   - Track error patterns for monitoring
//
// The package provides:
//   - Store interface for DLQ persistence
//   - Manager for DLQ operations and replay
//   - Multiple store implementations (PostgreSQL, Redis, MongoDB, Memory)
//
// # Basic Usage
//
//	// Create store and manager
//	store := dlq.NewPostgresStore(db)
//	manager := dlq.NewManager(store, transport)
//
//	// Store a failed message (typically in handler error path)
//	if err := handler(ctx, msg); err != nil {
//	    if retryCount >= maxRetries {
//	        manager.Store(ctx, event.Name, msg.ID, payload, metadata, err, retryCount, "order-service")
//	    }
//	}
//
//	// Later: Replay failed messages after fixing the issue
//	replayed, err := manager.Replay(ctx, dlq.Filter{
//	    EventName: "orders.created",
//	    StartTime: time.Now().Add(-24 * time.Hour),
//	})
//
// # Monitoring
//
//	stats, err := manager.Stats(ctx)
//	fmt.Printf("Pending messages: %d\n", stats.PendingMessages)
//	for event, count := range stats.MessagesByEvent {
//	    fmt.Printf("  %s: %d\n", event, count)
//	}
//
// # Cleanup
//
//	// Remove messages older than 30 days
//	deleted, err := manager.Cleanup(ctx, 30*24*time.Hour)
//
// # Best Practices
//
//   - Store messages before exhausting retries
//   - Add context (source, metadata) for debugging
//   - Monitor DLQ size and set up alerts
//   - Implement automated replay for recoverable errors
//   - Clean up old messages periodically
package dlq

import (
	"context"
	"fmt"
	"time"

	eventerrors "github.com/rbaliyan/event/v3/errors"
)

// ErrNotFound is returned when a DLQ message cannot be found.
// Use errors.Is(err, ErrNotFound) to check for this condition.
//
// This error wraps the shared event errors package error, so both
// errors.Is(err, dlq.ErrNotFound) and errors.Is(err, eventerrors.ErrNotFound)
// will work for error checking.
var ErrNotFound = fmt.Errorf("dlq message %w", eventerrors.ErrNotFound)

// NewNotFoundError creates a detailed not found error for a DLQ message.
func NewNotFoundError(id string) error {
	return eventerrors.NewNotFoundError("dlq message", id)
}

// Message represents a message in the dead-letter queue.
//
// A DLQ message contains the original message data along with metadata
// about the failure, enabling debugging and replay.
type Message struct {
	ID         string            // Unique DLQ message ID (generated)
	EventName  string            // Original event name/topic
	OriginalID string            // Original message ID for correlation
	Payload    []byte            // Original message payload
	Metadata   map[string]string // Original message metadata
	Error      string            // Error that caused the failure
	RetryCount int               // Number of retries attempted before DLQ
	CreatedAt  time.Time         // When the message was added to DLQ
	RetriedAt  *time.Time        // When the message was last replayed (nil if never)
	Source     string            // Source system/service that produced the error
}

// Filter specifies criteria for listing DLQ messages.
//
// All fields are optional. Empty filter returns all messages.
//
// Example:
//
//	// Find recent payment failures
//	filter := dlq.Filter{
//	    EventName:      "payment.process",
//	    StartTime:      time.Now().Add(-24 * time.Hour),
//	    ExcludeRetried: true,
//	    Limit:          100,
//	}
type Filter struct {
	EventName      string    // Filter by event name (empty = all events)
	StartTime      time.Time // Filter messages after this time (zero = no minimum)
	EndTime        time.Time // Filter messages before this time (zero = no maximum)
	Error          string    // Filter by error message (contains match)
	MaxRetries     int       // Filter by retry count (0 = no limit)
	Source         string    // Filter by source service (empty = all sources)
	ExcludeRetried bool      // Exclude already replayed messages
	Limit          int       // Maximum results (0 = no limit)
	Offset         int       // Offset for pagination
}

// FilterBuilder provides a fluent API for constructing Filter queries.
//
// Example:
//
//	filter := dlq.NewFilterBuilder().
//	    ForEvent("payment.process").
//	    Since(time.Now().Add(-24 * time.Hour)).
//	    OnlyPending().
//	    WithLimit(100).
//	    Build()
type FilterBuilder struct {
	filter Filter
}

// NewFilterBuilder creates a new filter builder.
func NewFilterBuilder() *FilterBuilder {
	return &FilterBuilder{}
}

// ForEvent filters by event name.
func (b *FilterBuilder) ForEvent(name string) *FilterBuilder {
	b.filter.EventName = name
	return b
}

// Since filters messages created after the given time.
func (b *FilterBuilder) Since(t time.Time) *FilterBuilder {
	b.filter.StartTime = t
	return b
}

// Until filters messages created before the given time.
func (b *FilterBuilder) Until(t time.Time) *FilterBuilder {
	b.filter.EndTime = t
	return b
}

// InTimeRange filters messages created between start and end times.
func (b *FilterBuilder) InTimeRange(start, end time.Time) *FilterBuilder {
	b.filter.StartTime = start
	b.filter.EndTime = end
	return b
}

// Last filters messages from the last duration.
func (b *FilterBuilder) Last(d time.Duration) *FilterBuilder {
	b.filter.StartTime = time.Now().Add(-d)
	return b
}

// WithErrorContaining filters by error message containing the given text.
func (b *FilterBuilder) WithErrorContaining(text string) *FilterBuilder {
	b.filter.Error = text
	return b
}

// WithMaxRetries filters by messages with at most this many retries.
func (b *FilterBuilder) WithMaxRetries(count int) *FilterBuilder {
	b.filter.MaxRetries = count
	return b
}

// FromSource filters by source service.
func (b *FilterBuilder) FromSource(source string) *FilterBuilder {
	b.filter.Source = source
	return b
}

// OnlyPending excludes already retried messages.
func (b *FilterBuilder) OnlyPending() *FilterBuilder {
	b.filter.ExcludeRetried = true
	return b
}

// WithLimit sets the maximum number of results.
func (b *FilterBuilder) WithLimit(limit int) *FilterBuilder {
	b.filter.Limit = limit
	return b
}

// WithOffset sets the offset for pagination.
func (b *FilterBuilder) WithOffset(offset int) *FilterBuilder {
	b.filter.Offset = offset
	return b
}

// Page sets both limit and offset for pagination.
// Page numbers are 1-indexed.
func (b *FilterBuilder) Page(pageNum, pageSize int) *FilterBuilder {
	if pageNum < 1 {
		pageNum = 1
	}
	b.filter.Limit = pageSize
	b.filter.Offset = (pageNum - 1) * pageSize
	return b
}

// Build returns the constructed filter.
func (b *FilterBuilder) Build() Filter {
	return b.filter
}

// Store defines the interface for DLQ storage.
//
// Implementations must be safe for concurrent use. The store persists
// failed messages and supports filtering, replay, and cleanup.
//
// Implementations:
//   - PostgresStore: For PostgreSQL databases
//   - RedisStore: For Redis (see redis.go)
//   - MongoStore: For MongoDB (see mongodb.go)
//   - MemoryStore: For testing (see memory.go)
type Store interface {
	// Store adds a message to the DLQ.
	// The message ID should be pre-generated.
	Store(ctx context.Context, msg *Message) error

	// Get retrieves a single message by ID.
	// Returns error if not found.
	Get(ctx context.Context, id string) (*Message, error)

	// List returns messages matching the filter.
	// Returns empty slice if no matches.
	List(ctx context.Context, filter Filter) ([]*Message, error)

	// Count returns the number of messages matching the filter.
	Count(ctx context.Context, filter Filter) (int64, error)

	// MarkRetried marks a message as replayed.
	// Sets RetriedAt to current time.
	MarkRetried(ctx context.Context, id string) error

	// Delete removes a message from the DLQ.
	Delete(ctx context.Context, id string) error

	// DeleteOlderThan removes messages older than the specified age.
	// Returns the number of deleted messages.
	DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error)

	// DeleteByFilter removes messages matching the filter.
	// Returns the number of deleted messages.
	DeleteByFilter(ctx context.Context, filter Filter) (int64, error)

	// GetByOriginalID retrieves a DLQ message by the original event message ID.
	// Returns ErrNotFound if no message exists with that original ID.
	GetByOriginalID(ctx context.Context, originalID string) (*Message, error)
}

// Stats provides DLQ statistics.
//
// Used for monitoring and alerting on DLQ health.
type Stats struct {
	TotalMessages   int64            // Total messages in DLQ
	MessagesByEvent map[string]int64 // Count per event type
	MessagesByError map[string]int64 // Count per error type
	OldestMessage   *time.Time       // Timestamp of oldest message
	NewestMessage   *time.Time       // Timestamp of newest message
	RetriedMessages int64            // Messages that have been replayed
	PendingMessages int64            // Messages awaiting replay
}

// StatsProvider is an optional interface for stores that support statistics.
//
// Stores implementing this interface provide detailed statistics about
// DLQ contents, useful for monitoring dashboards.
type StatsProvider interface {
	// Stats returns DLQ statistics.
	Stats(ctx context.Context) (*Stats, error)
}
