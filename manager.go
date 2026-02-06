package dlq

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/rbaliyan/event/v3/backoff"
	"github.com/rbaliyan/event/v3/health"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel/trace"
)

// Manager handles DLQ operations including replay.
//
// The Manager provides a high-level API for:
//   - Storing failed messages in the DLQ
//   - Listing and filtering DLQ messages
//   - Replaying messages back to their original events with optional retry and backoff
//   - Cleaning up old messages
//   - Getting DLQ statistics
//
// Example:
//
//	store := dlq.NewPostgresStore(db)
//	manager := dlq.NewManager(store, transport)
//
//	// Store a failed message
//	manager.Store(ctx, "order.process", msg.ID, payload, metadata, err, 3, "order-service")
//
//	// Replay failed messages
//	replayed, err := manager.Replay(ctx, dlq.Filter{
//	    EventName:      "order.process",
//	    ExcludeRetried: true,
//	})
//
//	// Get statistics
//	stats, _ := manager.Stats(ctx)
//	fmt.Printf("Pending: %d\n", stats.PendingMessages)
type Manager struct {
	store      Store
	transport  transport.Transport
	logger     *slog.Logger
	metrics    *Metrics
	backoff    BackoffStrategy
	maxRetries int
}

// BackoffStrategy is an alias for backoff.Strategy from the main event library.
// All implementations from github.com/rbaliyan/event/v3/backoff can be used directly.
//
// Implementations must be safe for concurrent use.
type BackoffStrategy = backoff.Strategy

// Resetter is an optional interface for backoff strategies that maintain state.
// If a BackoffStrategy also implements Resetter, Reset() will be called before
// retry attempts to clear any accumulated state.
type Resetter interface {
	Reset()
}

// Storer defines the interface for storing messages in a dead-letter queue.
// This interface is satisfied by Manager and can be used by other packages
// (like event-scheduler) to store failed messages without depending on
// the full DLQ package.
//
// Example usage in other packages:
//
//	type DeadLetterQueue interface {
//	    Store(ctx context.Context, eventName, originalID string, payload []byte,
//	          metadata map[string]string, err error, retryCount int, source string) error
//	}
//
// The Manager type from this package satisfies this interface.
type Storer interface {
	Store(ctx context.Context, eventName, originalID string, payload []byte, metadata map[string]string, err error, retryCount int, source string) error
}

// Compile-time check that Manager satisfies Storer
var _ Storer = (*Manager)(nil)

// managerOptions holds configuration for Manager (unexported)
type managerOptions struct {
	logger     *slog.Logger
	metrics    *Metrics
	backoff    BackoffStrategy
	maxRetries int
}

// ManagerOption is a functional option for configuring Manager
type ManagerOption func(*managerOptions)

// WithLogger sets a custom logger for the manager.
//
// The logger is used for info and error messages during DLQ operations.
func WithLogger(l *slog.Logger) ManagerOption {
	return func(o *managerOptions) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithMetrics enables OpenTelemetry metrics for the manager.
//
// When metrics are enabled, the manager records:
//   - dlq_messages_total: Messages sent to DLQ
//   - dlq_messages_replayed_total: Messages replayed
//   - dlq_messages_deleted_total: Messages deleted
//   - dlq_messages_pending: Current pending messages
//   - dlq_replay_success_total: Successful replays
//   - dlq_replay_failure_total: Failed replays
//
// Example:
//
//	metrics, _ := dlq.NewMetrics()
//	manager := dlq.NewManager(store, transport, dlq.WithMetrics(metrics))
func WithMetrics(m *Metrics) ManagerOption {
	return func(o *managerOptions) {
		o.metrics = m
	}
}

// WithBackoff sets a backoff strategy for replay retries.
//
// When a replay fails, this strategy determines how long to wait before
// retrying. Combined with WithMaxRetries, this enables automatic retry
// of transient failures during replay.
//
// If not set, failed replays are not retried automatically.
//
// Example:
//
//	// Using the event library's backoff package
//	import "github.com/rbaliyan/event/v3/backoff"
//
//	manager := dlq.NewManager(store, transport,
//	    dlq.WithBackoff(&backoff.Exponential{
//	        Initial:    time.Second,
//	        Multiplier: 2.0,
//	        Max:        30 * time.Second,
//	        Jitter:     0.1,
//	    }),
//	    dlq.WithMaxRetries(3),
//	)
func WithBackoff(strategy BackoffStrategy) ManagerOption {
	return func(o *managerOptions) {
		o.backoff = strategy
	}
}

// WithMaxRetries sets the maximum number of retry attempts for replay.
//
// When a replay fails, it will be retried up to this many times before
// being skipped. Use this in combination with WithBackoff to configure
// the delay between retries.
//
// If set to 0 (default), failed replays are not retried.
//
// Example:
//
//	manager := dlq.NewManager(store, transport,
//	    dlq.WithBackoff(backoffStrategy),
//	    dlq.WithMaxRetries(3),
//	)
func WithMaxRetries(max int) ManagerOption {
	return func(o *managerOptions) {
		if max >= 0 {
			o.maxRetries = max
		}
	}
}

// NewManager creates a new DLQ manager.
//
// The manager requires a store for persistence and a transport for replaying
// messages back to their original events.
//
// Parameters:
//   - store: DLQ storage implementation
//   - t: Transport for publishing replayed messages
//   - opts: Optional configuration (logger, metrics, backoff, etc.)
//
// Example:
//
//	store := dlq.NewRedisStore(redisClient)
//	manager := dlq.NewManager(store, transport)
//
//	// With metrics and retry backoff
//	metrics, _ := dlq.NewMetrics()
//	manager := dlq.NewManager(store, transport,
//	    dlq.WithMetrics(metrics),
//	    dlq.WithBackoff(backoffStrategy),
//	    dlq.WithMaxRetries(3),
//	)
func NewManager(store Store, t transport.Transport, opts ...ManagerOption) *Manager {
	o := &managerOptions{
		logger: slog.Default().With("component", "dlq.manager"),
	}
	for _, opt := range opts {
		opt(o)
	}

	return &Manager{
		store:      store,
		transport:  t,
		logger:     o.logger,
		metrics:    o.metrics,
		backoff:    o.backoff,
		maxRetries: o.maxRetries,
	}
}

// Store adds a failed message to the DLQ.
//
// Call this when a message has exhausted all retries and needs to be
// preserved for later investigation or replay.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - eventName: The original event name/topic
//   - originalID: The original message ID for correlation
//   - payload: The message payload (typically JSON)
//   - metadata: Original message metadata
//   - err: The error that caused the failure
//   - retryCount: Number of retries attempted
//   - source: Source service identifier for debugging
//
// Example:
//
//	func handleWithDLQ(ctx context.Context, msg Message) error {
//	    for attempt := 0; attempt < maxRetries; attempt++ {
//	        if err := process(ctx, msg); err == nil {
//	            return nil
//	        }
//	    }
//	    return dlqManager.Store(ctx, event.Name, msg.ID, msg.Payload,
//	        msg.Metadata, err, maxRetries, "order-service")
//	}
func (m *Manager) Store(ctx context.Context, eventName, originalID string, payload []byte, metadata map[string]string, err error, retryCount int, source string) error {
	var errMsg string
	if err != nil {
		errMsg = err.Error()
	} else {
		errMsg = "unknown error"
	}
	msg := &Message{
		ID:         uuid.New().String(),
		EventName:  eventName,
		OriginalID: originalID,
		Payload:    payload,
		Metadata:   metadata,
		Error:      errMsg,
		RetryCount: retryCount,
		CreatedAt:  time.Now(),
		Source:     source,
	}

	if storeErr := m.store.Store(ctx, msg); storeErr != nil {
		m.logger.Error("failed to store DLQ message",
			"event", eventName,
			"original_id", originalID,
			"error", storeErr)
		return fmt.Errorf("store dlq message: %w", storeErr)
	}

	// Record metrics
	m.metrics.RecordMessageStored(ctx, eventName, errMsg)

	m.logger.Info("stored message in DLQ",
		"id", msg.ID,
		"event", eventName,
		"original_id", originalID,
		"retry_count", retryCount)

	return nil
}

// Get retrieves a single DLQ message
func (m *Manager) Get(ctx context.Context, id string) (*Message, error) {
	return m.store.Get(ctx, id)
}

// List returns DLQ messages matching the filter
func (m *Manager) List(ctx context.Context, filter Filter) ([]*Message, error) {
	return m.store.List(ctx, filter)
}

// Count returns the number of messages matching the filter
func (m *Manager) Count(ctx context.Context, filter Filter) (int64, error) {
	return m.store.Count(ctx, filter)
}

// Replay replays all DLQ messages matching the filter back to their original events.
//
// Messages are republished to their original event topics with additional
// metadata indicating they are DLQ replays. Successfully replayed messages
// are marked as retried.
//
// If backoff and maxRetries are configured, each message replay will be
// retried with increasing delays before being skipped.
//
// The replay continues even if some messages fail to replay, logging errors
// for failed messages.
//
// Parameters:
//   - ctx: Context for cancellation and deadlines
//   - filter: Criteria for selecting messages to replay
//
// Returns the number of successfully replayed messages.
//
// Example:
//
//	// Replay all unretried payment failures from the last day
//	replayed, err := manager.Replay(ctx, dlq.Filter{
//	    EventName:      "payment.process",
//	    StartTime:      time.Now().Add(-24 * time.Hour),
//	    ExcludeRetried: true,
//	})
//	log.Info("replayed messages", "count", replayed)
func (m *Manager) Replay(ctx context.Context, filter Filter) (int, error) {
	messages, err := m.store.List(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("list messages: %w", err)
	}

	replayed := 0
	for _, msg := range messages {
		if err := m.replayMessageWithRetry(ctx, msg); err != nil {
			m.logger.Error("failed to replay message",
				"id", msg.ID,
				"event", msg.EventName,
				"error", err)
			// Record replay failure metric
			m.metrics.RecordReplayFailure(ctx, msg.EventName, err.Error())
			continue
		}

		if err := m.store.MarkRetried(ctx, msg.ID); err != nil {
			m.logger.Error("failed to mark message as retried",
				"id", msg.ID,
				"error", err)
		}

		// Record replay success metrics
		m.metrics.RecordReplaySuccess(ctx, msg.EventName)
		m.metrics.RecordMessageReplayed(ctx, msg.EventName)

		replayed++
	}

	m.logger.Info("replayed DLQ messages",
		"total", len(messages),
		"replayed", replayed)

	return replayed, nil
}

// ReplaySingle replays a single DLQ message by ID.
//
// If backoff and maxRetries are configured, the replay will be retried
// with increasing delays before returning an error.
func (m *Manager) ReplaySingle(ctx context.Context, id string) error {
	msg, err := m.store.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("get message: %w", err)
	}

	if err := m.replayMessageWithRetry(ctx, msg); err != nil {
		// Record replay failure metric
		m.metrics.RecordReplayFailure(ctx, msg.EventName, err.Error())
		return fmt.Errorf("replay message: %w", err)
	}

	if err := m.store.MarkRetried(ctx, id); err != nil {
		return fmt.Errorf("mark retried: %w", err)
	}

	// Record replay success metrics
	m.metrics.RecordReplaySuccess(ctx, msg.EventName)
	m.metrics.RecordMessageReplayed(ctx, msg.EventName)

	m.logger.Info("replayed single DLQ message",
		"id", id,
		"event", msg.EventName,
		"original_id", msg.OriginalID)

	return nil
}

// replayMessageWithRetry replays a message with optional retry and backoff.
// Each invocation computes delays based on the attempt index passed to
// NextDelay, avoiding shared mutable backoff state between concurrent calls.
func (m *Manager) replayMessageWithRetry(ctx context.Context, msg *Message) error {
	maxAttempts := m.maxRetries + 1 // +1 for the initial attempt
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		// Wait for backoff delay on retry (not on first attempt)
		if attempt > 0 && m.backoff != nil {
			backoffDelay := m.backoff.NextDelay(attempt - 1)
			m.logger.Info("retrying replay after backoff",
				"id", msg.ID,
				"event", msg.EventName,
				"attempt", attempt+1,
				"backoff_delay", backoffDelay)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoffDelay):
				// Continue with retry
			}
		}

		lastErr = m.replayMessage(ctx, msg)
		if lastErr == nil {
			return nil // Success
		}

		// Log retry attempt
		if attempt < maxAttempts-1 {
			m.logger.Warn("replay failed, will retry",
				"id", msg.ID,
				"event", msg.EventName,
				"attempt", attempt+1,
				"max_attempts", maxAttempts,
				"error", lastErr)
		}
	}

	return lastErr
}

// replayMessage publishes a message back to its original event
func (m *Manager) replayMessage(ctx context.Context, msg *Message) error {
	// Add replay metadata
	metadata := make(map[string]string)
	for k, v := range msg.Metadata {
		metadata[k] = v
	}
	metadata["dlq_replay"] = "true"
	metadata["dlq_message_id"] = msg.ID
	metadata["dlq_original_error"] = msg.Error

	// Propagate trace context from the current span
	var opts []message.Option
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		opts = append(opts, message.WithSpanContext(sc))
	}

	// Create transport message
	transportMsg := message.New(
		msg.OriginalID,
		"dlq-replay",
		msg.Payload, // Send raw payload
		metadata,
		opts...,
	)

	return m.transport.Publish(ctx, msg.EventName, transportMsg)
}

// Delete removes a message from the DLQ
func (m *Manager) Delete(ctx context.Context, id string) error {
	// Get message first to record metrics with event name
	msg, err := m.store.Get(ctx, id)
	if err != nil {
		return err
	}

	if err := m.store.Delete(ctx, id); err != nil {
		return err
	}

	// Record delete metric
	m.metrics.RecordMessageDeleted(ctx, msg.EventName)
	return nil
}

// DeleteByFilter removes messages matching the filter
func (m *Manager) DeleteByFilter(ctx context.Context, filter Filter) (int64, error) {
	deleted, err := m.store.DeleteByFilter(ctx, filter)
	if err != nil {
		return 0, err
	}

	// Record bulk delete metric
	m.metrics.RecordBulkDeleted(ctx, deleted)
	return deleted, nil
}

// Cleanup removes messages older than the specified age
func (m *Manager) Cleanup(ctx context.Context, age time.Duration) (int64, error) {
	deleted, err := m.store.DeleteOlderThan(ctx, age)
	if err != nil {
		return 0, err
	}

	if deleted > 0 {
		// Record bulk delete metric
		m.metrics.RecordBulkDeleted(ctx, deleted)

		m.logger.Info("cleaned up old DLQ messages",
			"deleted", deleted,
			"older_than", age)
	}

	return deleted, nil
}

// Stats returns DLQ statistics if the store supports it
func (m *Manager) Stats(ctx context.Context) (*Stats, error) {
	if sp, ok := m.store.(StatsProvider); ok {
		return sp.Stats(ctx)
	}

	// Fallback: compute basic stats
	total, err := m.store.Count(ctx, Filter{})
	if err != nil {
		return nil, err
	}

	pending, err := m.store.Count(ctx, Filter{ExcludeRetried: true})
	if err != nil {
		return nil, err
	}

	return &Stats{
		TotalMessages:   total,
		PendingMessages: pending,
		RetriedMessages: total - pending,
	}, nil
}

// Health performs a health check on the DLQ manager.
//
// The health check:
//   - Verifies store connectivity by counting messages
//   - Returns pending and total message counts
//
// Returns health.StatusHealthy if the store is responsive.
// Returns health.StatusDegraded if there are pending messages (potential backlog).
// Returns health.StatusUnhealthy if the store is not responsive.
func (m *Manager) Health(ctx context.Context) *health.Result {
	start := time.Now()

	// Try to count messages to verify store connectivity
	total, err := m.store.Count(ctx, Filter{})
	if err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("store connectivity failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
		}
	}

	pending, err := m.store.Count(ctx, Filter{ExcludeRetried: true})
	if err != nil {
		return &health.Result{
			Status:    health.StatusDegraded,
			Message:   fmt.Sprintf("failed to count pending: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
			Details: map[string]any{
				"total_messages": total,
			},
		}
	}

	status := health.StatusHealthy
	message := ""

	// Degraded if there are pending messages (potential backlog)
	if pending > 0 {
		status = health.StatusDegraded
		message = fmt.Sprintf("%d messages pending in DLQ", pending)
	}

	return &health.Result{
		Status:    status,
		Message:   message,
		Latency:   time.Since(start),
		CheckedAt: start,
		Details: map[string]any{
			"total_messages":   total,
			"pending_messages": pending,
			"retried_messages": total - pending,
		},
	}
}

// Compile-time check that Manager implements health.Checker
var _ health.Checker = (*Manager)(nil)
