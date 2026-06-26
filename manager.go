package dlq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	event "github.com/rbaliyan/event/v3"
	"github.com/rbaliyan/event/v3/backoff"
	"github.com/rbaliyan/event/v3/health"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
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
//	store, err := dlq.NewPostgresStore(db)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	manager, err := dlq.NewManager(store, bus)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Store a failed message
//	manager.Store(ctx, dlq.StoreParams{
//	    EventName:  "order.process",
//	    OriginalID: msg.ID,
//	    Payload:    payload,
//	    Metadata:   metadata,
//	    Err:        err,
//	    RetryCount: 3,
//	    Source:     "order-service",
//	})
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
//
// Republisher sends events for DLQ replay.
// This is an alias for event.Sender.
type Republisher = event.Sender

// transportRepublisher wraps transport.Transport to satisfy Republisher.
type transportRepublisher struct {
	t transport.Transport
}

func (r *transportRepublisher) Send(ctx context.Context, eventName string, eventID string, payload []byte, metadata map[string]string) error {
	msg := message.New(eventID, "dlq-replay", payload, metadata)
	return r.t.Publish(ctx, eventName, msg)
}

type Manager struct {
	store             Store
	republisher       Republisher
	logger            *slog.Logger
	metrics           *Metrics
	backoff           BackoffStrategy
	maxRetries        int
	terminalError     func(*Message) bool
	maxReplayAttempts int
}

// BackoffStrategy is an alias for backoff.Strategy from the main event library.
// All implementations from github.com/rbaliyan/event/v3/backoff can be used directly.
//
// Implementations must be safe for concurrent use.
type BackoffStrategy = backoff.Strategy

// StoreParams contains the parameters for storing a failed message in the DLQ.
type StoreParams struct {
	// EventName is the name of the event that failed.
	EventName string
	// OriginalID is the original message ID.
	OriginalID string
	// Payload is the message payload.
	Payload []byte
	// Metadata contains additional message metadata.
	//
	// IMPORTANT for WithMaxReplayAttempts: the replay-attempt counter
	// (MetadataReplayCount / "dlq_replay_count") rides in this metadata and is
	// what bounds the replay loop. When a replayed message fails and is re-DLQ'd,
	// the caller MUST carry the delivered message's metadata through into this
	// field so the counter accumulates across the republish->re-DLQ cycle. If the
	// metadata is regenerated or stripped on re-DLQ, the counter resets to 0 and
	// the WithMaxReplayAttempts cap will never fire. (event.Bus.sendToDLQ does
	// this correctly out of the box; custom DLQ-store wiring must preserve it.)
	Metadata map[string]string
	// Err is the error that caused the failure.
	Err error
	// RetryCount is the number of retries attempted.
	RetryCount int
	// Source is the source service identifier.
	Source string
}

// Storer defines the interface for storing messages in a dead-letter queue.
// This interface is satisfied by Manager and can be used by other packages
// (like event-scheduler) to store failed messages without depending on
// the full DLQ package.
//
// The Manager type from this package satisfies this interface.
type Storer interface {
	Store(ctx context.Context, params StoreParams) error
}

// Compile-time check that Manager satisfies Storer
var _ Storer = (*Manager)(nil)

// managerOptions holds configuration for Manager (unexported)
type managerOptions struct {
	logger            *slog.Logger
	metrics           *Metrics
	backoff           BackoffStrategy
	maxRetries        int
	terminalError     func(*Message) bool
	maxReplayAttempts int
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
//	manager, err := dlq.NewManager(store, bus, dlq.WithMetrics(metrics))
//	if err != nil {
//	    log.Fatal(err)
//	}
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
//	manager, err := dlq.NewManager(store, bus,
//	    dlq.WithBackoff(&backoff.Exponential{
//	        Initial:    time.Second,
//	        Multiplier: 2.0,
//	        Max:        30 * time.Second,
//	        Jitter:     0.1,
//	    }),
//	    dlq.WithMaxRetries(3),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
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
//	manager, err := dlq.NewManager(store, bus,
//	    dlq.WithBackoff(backoffStrategy),
//	    dlq.WithMaxRetries(3),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
func WithMaxRetries(max int) ManagerOption {
	return func(o *managerOptions) {
		if max >= 0 {
			o.maxRetries = max
		}
	}
}

// WithTerminalError sets a predicate identifying non-retryable ("terminal")
// messages. During Replay, a message for which the predicate returns true is
// quarantined (if the store supports Quarantiner) and never republished,
// preventing poison-message replay loops.
//
// Default is nil: when unset, Replay behaves exactly as before — every message
// is replayed regardless of error.
func WithTerminalError(pred func(*Message) bool) ManagerOption {
	return func(o *managerOptions) {
		o.terminalError = pred
	}
}

// MetadataReplayCount is the message-metadata key tracking how many times a
// message has been replayed. It rides in the published message's metadata and
// survives the republish -> re-DLQ round trip, so it accumulates across replay
// cycles even when each cycle produces a fresh DLQ row (no dedup required).
const MetadataReplayCount = "dlq_replay_count"

// WithMaxReplayAttempts caps how many times a single message may be replayed
// before it is quarantined (stop replaying, keep the row for inspection),
// regardless of why it fails. This is the error-agnostic backstop against a
// permanently-failing message looping forever: republish -> decode/handler fails
// -> re-DLQ -> republish ... The count is carried in message metadata
// (MetadataReplayCount), so it bounds the loop without dedup or a stable row.
//
// n <= 0 (the default) means unlimited replays — the prior behavior. A small
// value (e.g. 3-5) is recommended for any auto-replay loop.
func WithMaxReplayAttempts(n int) ManagerOption {
	return func(o *managerOptions) {
		o.maxReplayAttempts = n
	}
}

// replayCount returns how many times the message has already been replayed,
// read from its metadata. Missing/invalid values count as 0.
func replayCount(msg *Message) int {
	if msg == nil || msg.Metadata == nil {
		return 0
	}
	n, err := strconv.Atoi(msg.Metadata[MetadataReplayCount])
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// exceededReplayCap reports whether the message has hit the configured
// max-replay-attempts cap. Always false when the cap is disabled (n <= 0).
func (m *Manager) exceededReplayCap(msg *Message) bool {
	return m.maxReplayAttempts > 0 && replayCount(msg) >= m.maxReplayAttempts
}

// TerminalErrorMatching returns a predicate that reports a message as terminal
// when its Error contains any of the given (case-sensitive) substrings. An
// empty Error never matches.
func TerminalErrorMatching(patterns ...string) func(*Message) bool {
	return func(msg *Message) bool {
		if msg == nil || msg.Error == "" {
			return false
		}
		for _, p := range patterns {
			if p != "" && strings.Contains(msg.Error, p) {
				return true
			}
		}
		return false
	}
}

// NewManager creates a new DLQ manager.
//
// The manager requires a store for persistence and a republisher for replaying
// messages back to their original events. The republisher can be:
//   - An *event.Bus (recommended — works with all transports including MongoDB)
//   - A transport.Transport (wrapped automatically via NewManagerWithTransport)
//   - Any custom Republisher implementation
//
// Parameters:
//   - store: DLQ storage implementation
//   - r: Republisher for replaying messages (typically *event.Bus)
//   - opts: Optional configuration (logger, metrics, backoff, etc.)
//
// Example:
//
//	store, err := dlq.NewRedisStore(redisClient)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Using a bus (recommended for MongoDB and other non-publishable transports)
//	manager, err := dlq.NewManager(store, bus)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// With metrics and retry backoff
//	metrics, _ := dlq.NewMetrics()
//	manager, err = dlq.NewManager(store, bus,
//	    dlq.WithMetrics(metrics),
//	    dlq.WithBackoff(backoffStrategy),
//	    dlq.WithMaxRetries(3),
//	)
func NewManager(store Store, r Republisher, opts ...ManagerOption) (*Manager, error) {
	if store == nil {
		return nil, errors.New("manager: store is required")
	}
	if r == nil {
		return nil, errors.New("manager: republisher is required")
	}

	o := &managerOptions{
		logger: slog.Default().With("component", "dlq.manager"),
	}
	for _, opt := range opts {
		opt(o)
	}

	return &Manager{
		store:             store,
		republisher:       r,
		logger:            o.logger,
		metrics:           o.metrics,
		backoff:           o.backoff,
		maxRetries:        o.maxRetries,
		terminalError:     o.terminalError,
		maxReplayAttempts: o.maxReplayAttempts,
	}, nil
}

// NewManagerWithTransport creates a DLQ manager using a transport for replay.
// This wraps the transport in a Republisher adapter. For transports that don't
// support Publish (e.g., MongoDB), use NewManager with a *event.Bus instead.
func NewManagerWithTransport(store Store, t transport.Transport, opts ...ManagerOption) (*Manager, error) {
	if t == nil {
		return nil, errors.New("manager: transport is required")
	}
	return NewManager(store, &transportRepublisher{t: t}, opts...)
}

// Store adds a failed message to the DLQ.
//
// Call this when a message has exhausted all retries and needs to be
// preserved for later investigation or replay.
//
// Example:
//
//	func handleWithDLQ(ctx context.Context, msg Message) error {
//	    for attempt := 0; attempt < maxRetries; attempt++ {
//	        if err := process(ctx, msg); err == nil {
//	            return nil
//	        }
//	    }
//	    return dlqManager.Store(ctx, dlq.StoreParams{
//	        EventName:  event.Name,
//	        OriginalID: msg.ID,
//	        Payload:    msg.Payload,
//	        Metadata:   msg.Metadata,
//	        Err:        err,
//	        RetryCount: maxRetries,
//	        Source:     "order-service",
//	    })
//	}
// backend returns the underlying store's backend name for metric labelling, or
// "unknown" if the store does not expose one.
func (m *Manager) backend() string {
	if b, ok := m.store.(interface{ Backend() string }); ok {
		return b.Backend()
	}
	return "unknown"
}

// timeStoreOp starts a timer for a store operation and returns a function that
// records its duration when called. Use as: defer m.timeStoreOp(ctx, "list")().
func (m *Manager) timeStoreOp(ctx context.Context, op string) func() {
	start := time.Now()
	return func() {
		m.metrics.RecordStoreOpDuration(ctx, op, m.backend(), time.Since(start))
	}
}

func (m *Manager) Store(ctx context.Context, params StoreParams) error {
	defer m.timeStoreOp(ctx, "store")()
	var errMsg string
	if params.Err != nil {
		errMsg = params.Err.Error()
	} else {
		errMsg = "unknown error"
	}
	msg := &Message{
		ID:         uuid.New().String(),
		EventName:  params.EventName,
		OriginalID: params.OriginalID,
		Payload:    params.Payload,
		Metadata:   params.Metadata,
		Error:      errMsg,
		RetryCount: params.RetryCount,
		CreatedAt:  time.Now(),
		Source:     params.Source,
	}

	if storeErr := m.store.Store(ctx, msg); storeErr != nil {
		m.metrics.RecordStoreError(ctx, "store", m.backend())
		m.logger.Error("failed to store DLQ message",
			"event", params.EventName,
			"original_id", params.OriginalID,
			"error", storeErr)
		return fmt.Errorf("store dlq message: %w", storeErr)
	}

	// Record metrics
	m.metrics.RecordMessageStored(ctx, params.EventName, errMsg)

	m.logger.Info("stored message in DLQ",
		"id", msg.ID,
		"event", params.EventName,
		"original_id", params.OriginalID,
		"retry_count", params.RetryCount)

	return nil
}

// Get retrieves a single DLQ message
func (m *Manager) Get(ctx context.Context, id string) (*Message, error) {
	defer m.timeStoreOp(ctx, "get")()
	return m.store.Get(ctx, id)
}

// GetByOriginalID retrieves a single DLQ message by the original event message
// ID (the ID of the message that originally failed), rather than by the
// generated DLQ ID. Returns ErrNotFound if no message has that original ID.
func (m *Manager) GetByOriginalID(ctx context.Context, originalID string) (*Message, error) {
	defer m.timeStoreOp(ctx, "get_by_original_id")()
	return m.store.GetByOriginalID(ctx, originalID)
}

// List returns DLQ messages matching the filter
func (m *Manager) List(ctx context.Context, filter Filter) ([]*Message, error) {
	defer m.timeStoreOp(ctx, "list")()
	return m.store.List(ctx, filter)
}

// Count returns the number of messages matching the filter
func (m *Manager) Count(ctx context.Context, filter Filter) (int64, error) {
	defer m.timeStoreOp(ctx, "count")()
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
//	    After:          time.Now().Add(-24 * time.Hour),
//	    ExcludeRetried: true,
//	})
//	log.Info("replayed messages", "count", replayed)
func (m *Manager) Replay(ctx context.Context, filter Filter) (int, error) {
	filter.ExcludeQuarantined = true // never re-evaluate quarantined messages
	messages, err := m.store.List(ctx, filter)
	if err != nil {
		m.metrics.RecordStoreError(ctx, "list", m.backend())
		return 0, fmt.Errorf("list messages: %w", err)
	}

	replayed := 0
	warnedNoQuarantine := false
	for _, msg := range messages {
		// Generic, error-agnostic backstop: a message that has been replayed too
		// many times is quarantined regardless of why it keeps failing.
		if m.exceededReplayCap(msg) {
			m.quarantineDuringReplay(ctx, msg, "max_replay_attempts", &warnedNoQuarantine)
			continue
		}
		if m.isTerminal(msg) {
			m.quarantineDuringReplay(ctx, msg, "terminal_error", &warnedNoQuarantine)
			continue
		}

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
			m.metrics.RecordStoreError(ctx, "mark_retried", m.backend())
			m.logger.Error("failed to mark message as retried",
				"id", msg.ID,
				"error", err)
		}

		// Record replay success metrics
		m.metrics.RecordReplaySuccess(ctx, msg.EventName)
		m.metrics.RecordMessageReplayed(ctx, msg.EventName)
		m.metrics.RecordMessageAge(ctx, msg.EventName, msg.CreatedAt)

		replayed++
	}

	m.logger.Info("replayed DLQ messages",
		"total", len(messages),
		"replayed", replayed)

	return replayed, nil
}

// errStoreNotQuarantiner indicates the configured store does not implement
// Quarantiner, so terminal messages cannot be persistently quarantined.
var errStoreNotQuarantiner = errors.New("dlq: store does not support quarantine")

// isTerminal reports whether the message is a non-retryable failure per the
// configured WithTerminalError predicate. Always false when no predicate is set.
func (m *Manager) isTerminal(msg *Message) bool {
	return m.terminalError != nil && m.terminalError(msg)
}

// quarantineTerminal quarantines a terminal message. It returns
// errStoreNotQuarantiner if the store does not implement Quarantiner, or the
// underlying store error if the quarantine write fails.
func (m *Manager) quarantineMessage(ctx context.Context, msg *Message, reason string) error {
	q, ok := m.store.(Quarantiner)
	if !ok {
		return errStoreNotQuarantiner
	}
	if err := q.Quarantine(ctx, msg.ID); err != nil {
		return fmt.Errorf("quarantine %s: %w", msg.ID, err)
	}
	m.metrics.RecordQuarantined(ctx, msg.EventName, reason)
	m.metrics.RecordMessageAge(ctx, msg.EventName, msg.CreatedAt)
	m.logger.Info("quarantined DLQ message instead of replaying",
		"id", msg.ID, "event", msg.EventName, "reason", reason, "error", msg.Error)
	return nil
}

// quarantineDuringReplay quarantines msg for the given reason while a Replay
// sweep is running, handling the store-not-quarantiner and error cases. The
// warnedNoQuarantine pointer suppresses repeated warnings to once per sweep.
func (m *Manager) quarantineDuringReplay(ctx context.Context, msg *Message, reason string, warnedNoQuarantine *bool) {
	if err := m.quarantineMessage(ctx, msg, reason); err != nil {
		if errors.Is(err, errStoreNotQuarantiner) {
			if !*warnedNoQuarantine {
				m.logger.Warn("DLQ message must be quarantined but store does not support quarantine; skipping replay",
					"id", msg.ID, "event", msg.EventName, "reason", reason)
				*warnedNoQuarantine = true
			}
		} else {
			m.logger.Error("failed to quarantine DLQ message", "id", msg.ID, "reason", reason, "error", err)
		}
	}
}

// ReplaySingle replays a single DLQ message by ID.
//
// If backoff and maxRetries are configured, the replay will be retried
// with increasing delays before returning an error.
func (m *Manager) ReplaySingle(ctx context.Context, id string) error {
	msg, err := m.store.Get(ctx, id)
	if err != nil {
		m.metrics.RecordStoreError(ctx, "get", m.backend())
		return fmt.Errorf("get message: %w", err)
	}

	if m.exceededReplayCap(msg) {
		return m.quarantineMessage(ctx, msg, "max_replay_attempts")
	}
	if m.isTerminal(msg) {
		return m.quarantineMessage(ctx, msg, "terminal_error")
	}

	if err := m.replayMessageWithRetry(ctx, msg); err != nil {
		// Record replay failure metric
		m.metrics.RecordReplayFailure(ctx, msg.EventName, err.Error())
		return fmt.Errorf("replay message: %w", err)
	}

	if err := m.store.MarkRetried(ctx, id); err != nil {
		m.metrics.RecordStoreError(ctx, "mark_retried", m.backend())
		return fmt.Errorf("mark retried: %w", err)
	}

	// Record replay success metrics
	m.metrics.RecordReplaySuccess(ctx, msg.EventName)
	m.metrics.RecordMessageReplayed(ctx, msg.EventName)
	m.metrics.RecordMessageAge(ctx, msg.EventName, msg.CreatedAt)

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

	// Record the (retry-inclusive) duration and the attempt count on every exit
	// path — success, give-up, or context cancellation.
	start := time.Now()
	attemptsMade := 0
	defer func() {
		m.metrics.RecordReplayDuration(ctx, msg.EventName, time.Since(start))
		m.metrics.RecordReplayAttempts(ctx, msg.EventName, attemptsMade)
	}()

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		attemptsMade = attempt + 1
		// Wait for backoff delay on retry (not on first attempt)
		if attempt > 0 && m.backoff != nil {
			backoffDelay := m.backoff.NextDelay(attempt - 1)
			m.logger.Info("retrying replay after backoff",
				"id", msg.ID,
				"event", msg.EventName,
				"attempt", attempt+1,
				"backoff_delay", backoffDelay)

			timer := time.NewTimer(backoffDelay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
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
	// Increment the replay counter so it accumulates across republish -> re-DLQ
	// cycles, enabling the WithMaxReplayAttempts cap without dedup.
	metadata[MetadataReplayCount] = strconv.Itoa(replayCount(msg) + 1)

	return m.republisher.Send(ctx, msg.EventName, msg.OriginalID, msg.Payload, metadata)
}

// Delete removes a message from the DLQ
func (m *Manager) Delete(ctx context.Context, id string) error {
	defer m.timeStoreOp(ctx, "delete")()
	// Get message first to record metrics with event name
	msg, err := m.store.Get(ctx, id)
	if err != nil {
		m.metrics.RecordStoreError(ctx, "get", m.backend())
		return err
	}

	if err := m.store.Delete(ctx, id); err != nil {
		m.metrics.RecordStoreError(ctx, "delete", m.backend())
		return err
	}

	// Record delete metric
	m.metrics.RecordMessageDeleted(ctx, msg.EventName)
	m.metrics.RecordMessageAge(ctx, msg.EventName, msg.CreatedAt)
	return nil
}

// DeleteByFilter removes messages matching the filter
func (m *Manager) DeleteByFilter(ctx context.Context, filter Filter) (int64, error) {
	defer m.timeStoreOp(ctx, "delete_by_filter")()
	deleted, err := m.store.DeleteByFilter(ctx, filter)
	if err != nil {
		m.metrics.RecordStoreError(ctx, "delete_by_filter", m.backend())
		return 0, err
	}

	// Record bulk delete metric
	m.metrics.RecordBulkDeleted(ctx, deleted)
	return deleted, nil
}

// Cleanup removes messages older than the specified age
func (m *Manager) Cleanup(ctx context.Context, age time.Duration) (int64, error) {
	defer m.timeStoreOp(ctx, "delete_older_than")()
	deleted, err := m.store.DeleteOlderThan(ctx, age)
	if err != nil {
		m.metrics.RecordStoreError(ctx, "delete_older_than", m.backend())
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
