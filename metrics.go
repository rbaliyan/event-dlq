package dlq

import (
	"context"
	"sync"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// meterName is the OpenTelemetry meter name for DLQ metrics
	meterName = "github.com/rbaliyan/event-dlq"
)

// Metrics provides OpenTelemetry metrics for DLQ operations.
//
// All metrics are optional - if Metrics is nil, no metrics are recorded.
// Use NewMetrics() to create a Metrics instance with the global meter provider,
// or NewMetricsWithProvider() for a custom provider.
//
// Available metrics:
//   - dlq_messages_total: Counter of messages sent to DLQ (by event, error_type)
//   - dlq_messages_replayed_total: Counter of messages replayed
//   - dlq_messages_deleted_total: Counter of messages deleted/cleaned up
//   - dlq_messages_pending: Best-effort per-event gauge of pending messages (in-process)
//   - dlq_messages_pending_actual: Authoritative pending count from the store (observable; see RegisterPendingProvider)
//   - dlq_replay_success_total: Counter of successful replays
//   - dlq_replay_failure_total: Counter of failed replays (by event, error_type)
//   - dlq_messages_quarantined_total: Counter of quarantines (by event, reason)
//   - dlq_messages_quarantined: Gauge of currently-quarantined messages
//   - dlq_store_errors_total: Counter of store operation failures (by op, backend)
//   - dlq_store_op_duration_seconds: Histogram of store operation duration (by op, backend)
//   - dlq_replay_duration_seconds: Histogram of replay attempt duration
//   - dlq_replay_attempts: Histogram of attempts per replay
//   - dlq_message_age_seconds: Histogram of message age when it leaves the DLQ
type Metrics struct {
	messagesTotal            metric.Int64Counter
	messagesReplayedTotal    metric.Int64Counter
	messagesDeletedTotal     metric.Int64Counter
	messagesPending          metric.Int64UpDownCounter
	replaySuccessTotal       metric.Int64Counter
	replayFailureTotal       metric.Int64Counter
	messagesQuarantinedTotal metric.Int64Counter
	messagesQuarantined      metric.Int64UpDownCounter
	storeErrorsTotal         metric.Int64Counter
	replayDuration           metric.Float64Histogram
	replayAttempts           metric.Int64Histogram
	messageAge               metric.Float64Histogram
	storeOpDuration          metric.Float64Histogram

	// pendingCounts tracks pending messages per event for gauge updates
	pendingMu     sync.RWMutex
	pendingCounts map[string]int64

	// meter is retained so an observable gauge can be registered lazily once a
	// store-backed provider is available (see RegisterPendingProvider).
	meter       metric.Meter
	pendingOnce sync.Once
}

// NewMetrics creates a new Metrics instance using the global OpenTelemetry meter provider.
//
// Example:
//
//	metrics, err := dlq.NewMetrics()
//	if err != nil {
//		return err
//	}
//	manager, err := dlq.NewManager(store, bus, dlq.WithMetrics(metrics))
func NewMetrics() (*Metrics, error) {
	return NewMetricsWithProvider(otel.GetMeterProvider())
}

// NewMetricsWithProvider creates a new Metrics instance with a custom meter provider.
//
// This is useful for testing or when using a non-global meter provider.
//
// Example:
//
//	provider := sdkmetric.NewMeterProvider(...)
//	metrics, err := dlq.NewMetricsWithProvider(provider)
func NewMetricsWithProvider(provider metric.MeterProvider) (*Metrics, error) {
	meter := provider.Meter(meterName)

	messagesTotal, err := meter.Int64Counter("dlq_messages_total",
		metric.WithDescription("Total number of messages sent to the DLQ"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	messagesReplayedTotal, err := meter.Int64Counter("dlq_messages_replayed_total",
		metric.WithDescription("Total number of messages replayed from the DLQ"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	messagesDeletedTotal, err := meter.Int64Counter("dlq_messages_deleted_total",
		metric.WithDescription("Total number of messages deleted from the DLQ"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	messagesPending, err := meter.Int64UpDownCounter("dlq_messages_pending",
		metric.WithDescription("Current number of pending messages in the DLQ"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	replaySuccessTotal, err := meter.Int64Counter("dlq_replay_success_total",
		metric.WithDescription("Total number of successful message replays"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	replayFailureTotal, err := meter.Int64Counter("dlq_replay_failure_total",
		metric.WithDescription("Total number of failed message replays"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	messagesQuarantinedTotal, err := meter.Int64Counter("dlq_messages_quarantined_total",
		metric.WithDescription("Total messages quarantined as terminal/non-retryable"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	messagesQuarantined, err := meter.Int64UpDownCounter("dlq_messages_quarantined",
		metric.WithDescription("Current number of quarantined messages in the DLQ"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	storeErrorsTotal, err := meter.Int64Counter("dlq_store_errors_total",
		metric.WithDescription("Total DLQ store operation failures, by operation and backend"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return nil, err
	}

	replayDuration, err := meter.Float64Histogram("dlq_replay_duration_seconds",
		metric.WithDescription("Duration of a message replay attempt (retry-inclusive)"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 30),
	)
	if err != nil {
		return nil, err
	}

	replayAttempts, err := meter.Int64Histogram("dlq_replay_attempts",
		metric.WithDescription("Number of attempts made to replay a message before success or failure"),
		metric.WithUnit("{attempt}"),
		metric.WithExplicitBucketBoundaries(1, 2, 3, 5, 10),
	)
	if err != nil {
		return nil, err
	}

	messageAge, err := meter.Float64Histogram("dlq_message_age_seconds",
		metric.WithDescription("Age of a message (time since CreatedAt) when it leaves the DLQ"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(60, 300, 3600, 21600, 86400, 604800),
	)
	if err != nil {
		return nil, err
	}

	storeOpDuration, err := meter.Float64Histogram("dlq_store_op_duration_seconds",
		metric.WithDescription("Duration of a backend store operation, by operation and backend"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5),
	)
	if err != nil {
		return nil, err
	}

	return &Metrics{
		messagesTotal:            messagesTotal,
		messagesReplayedTotal:    messagesReplayedTotal,
		messagesDeletedTotal:     messagesDeletedTotal,
		messagesPending:          messagesPending,
		replaySuccessTotal:       replaySuccessTotal,
		replayFailureTotal:       replayFailureTotal,
		messagesQuarantinedTotal: messagesQuarantinedTotal,
		messagesQuarantined:      messagesQuarantined,
		storeErrorsTotal:         storeErrorsTotal,
		replayDuration:           replayDuration,
		replayAttempts:           replayAttempts,
		messageAge:               messageAge,
		storeOpDuration:          storeOpDuration,
		pendingCounts:            make(map[string]int64),
		meter:                    meter,
	}, nil
}

// RegisterPendingProvider registers an observable gauge, dlq_messages_pending_actual,
// whose value is sourced authoritatively from the provider on every metric
// collection. Unlike the imperative dlq_messages_pending gauge (a process-local
// counter that can drift across bulk deletes, restarts, or multiple managers),
// this reflects the real store state and reconciles automatically.
//
// The Manager calls this with a provider backed by store.Count when metrics are
// enabled. It registers at most once per Metrics instance (subsequent calls are
// no-ops), so the gauge is not double-counted when a Metrics is shared.
//
// The provider runs a store Count on each collection; at very short scrape
// intervals this adds load to the backend.
func (m *Metrics) RegisterPendingProvider(provider func(context.Context) (int64, error)) error {
	if m == nil || provider == nil {
		return nil
	}
	var regErr error
	m.pendingOnce.Do(func() {
		_, regErr = m.meter.Int64ObservableGauge("dlq_messages_pending_actual",
			metric.WithDescription("Authoritative pending message count, queried from the store on collection"),
			metric.WithUnit("{message}"),
			metric.WithInt64Callback(func(ctx context.Context, o metric.Int64Observer) error {
				n, err := provider(ctx)
				if err != nil {
					return err
				}
				o.Observe(n)
				return nil
			}),
		)
	})
	return regErr
}

// RecordMessageStored records that a message was stored in the DLQ.
// Increments dlq_messages_total and dlq_messages_pending.
func (m *Metrics) RecordMessageStored(ctx context.Context, eventName, errorType string) {
	if m == nil {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.String("event", eventName),
		attribute.String("error_type", normalizeErrorType(errorType)),
	}

	m.messagesTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.messagesPending.Add(ctx, 1, metric.WithAttributes(attribute.String("event", eventName)))

	m.pendingMu.Lock()
	m.pendingCounts[eventName]++
	m.pendingMu.Unlock()
}

// RecordMessageReplayed records that a message was replayed from the DLQ.
// Increments dlq_messages_replayed_total and decrements dlq_messages_pending.
func (m *Metrics) RecordMessageReplayed(ctx context.Context, eventName string) {
	if m == nil {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.String("event", eventName),
	}

	m.messagesReplayedTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.messagesPending.Add(ctx, -1, metric.WithAttributes(attrs...))

	m.pendingMu.Lock()
	if m.pendingCounts[eventName] > 0 {
		m.pendingCounts[eventName]--
	}
	m.pendingMu.Unlock()
}

// RecordMessageDeleted records that a message was deleted from the DLQ.
// Increments dlq_messages_deleted_total and decrements dlq_messages_pending.
func (m *Metrics) RecordMessageDeleted(ctx context.Context, eventName string) {
	if m == nil {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.String("event", eventName),
	}

	m.messagesDeletedTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	m.messagesPending.Add(ctx, -1, metric.WithAttributes(attrs...))

	m.pendingMu.Lock()
	if m.pendingCounts[eventName] > 0 {
		m.pendingCounts[eventName]--
	}
	m.pendingMu.Unlock()
}

// RecordBulkDeleted records that multiple messages were deleted from the DLQ.
// Increments dlq_messages_deleted_total by the count.
func (m *Metrics) RecordBulkDeleted(ctx context.Context, count int64) {
	if m == nil || count <= 0 {
		return
	}

	// For bulk deletes without specific event info, we record without event attribute
	m.messagesDeletedTotal.Add(ctx, count)
}

// RecordReplaySuccess records a successful replay attempt.
func (m *Metrics) RecordReplaySuccess(ctx context.Context, eventName string) {
	if m == nil {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.String("event", eventName),
	}

	m.replaySuccessTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordReplayFailure records a failed replay attempt.
func (m *Metrics) RecordReplayFailure(ctx context.Context, eventName, errorType string) {
	if m == nil {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.String("event", eventName),
		attribute.String("error_type", normalizeErrorType(errorType)),
	}

	m.replayFailureTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordQuarantined records that a terminal message was quarantined.
// Increments dlq_messages_quarantined_total and decrements dlq_messages_pending,
// because every backend's Stats.PendingMessages excludes quarantined messages:
// a quarantined message leaves the pending pool just as a replayed or deleted one
// does, so the gauge must drop by one to stay consistent with Stats.
// The reason is a bounded enum (max_replay_attempts / terminal_error), so it is
// safe to use as a label.
func (m *Metrics) RecordQuarantined(ctx context.Context, eventName, reason string) {
	if m == nil {
		return
	}

	eventAttr := metric.WithAttributes(attribute.String("event", eventName))

	m.messagesQuarantinedTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event", eventName),
		attribute.String("reason", reason),
	))
	// A quarantined message leaves the pending pool and enters the quarantined
	// pool: drop the pending gauge and raise the quarantined gauge.
	m.messagesPending.Add(ctx, -1, eventAttr)
	m.messagesQuarantined.Add(ctx, 1, eventAttr)

	m.pendingMu.Lock()
	if m.pendingCounts[eventName] > 0 {
		m.pendingCounts[eventName]--
	}
	m.pendingMu.Unlock()
}

// RecordReplayDuration records how long a (retry-inclusive) replay attempt took.
func (m *Metrics) RecordReplayDuration(ctx context.Context, eventName string, d time.Duration) {
	if m == nil {
		return
	}
	m.replayDuration.Record(ctx, d.Seconds(), metric.WithAttributes(attribute.String("event", eventName)))
}

// RecordReplayAttempts records how many attempts a replay took before it
// succeeded or gave up.
func (m *Metrics) RecordReplayAttempts(ctx context.Context, eventName string, attempts int) {
	if m == nil {
		return
	}
	m.replayAttempts.Record(ctx, int64(attempts), metric.WithAttributes(attribute.String("event", eventName)))
}

// RecordMessageAge records how long a message lived in the DLQ (now - createdAt)
// at the moment it leaves — replayed, deleted, or quarantined. This is the
// "time in DLQ" signal operators alert on. A zero createdAt is ignored.
func (m *Metrics) RecordMessageAge(ctx context.Context, eventName string, createdAt time.Time) {
	if m == nil || createdAt.IsZero() {
		return
	}
	age := time.Since(createdAt).Seconds()
	if age < 0 {
		age = 0
	}
	m.messageAge.Record(ctx, age, metric.WithAttributes(attribute.String("event", eventName)))
}

// RecordStoreError records a failure of a backend store operation, labelled by
// operation and backend so a broken DLQ persistence layer is alertable. Both
// labels are bounded enums.
func (m *Metrics) RecordStoreError(ctx context.Context, op, backend string) {
	if m == nil {
		return
	}
	m.storeErrorsTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("op", op),
		attribute.String("backend", backend),
	))
}

// RecordStoreOpDuration records how long a backend store operation took,
// labelled by operation and backend (both bounded enums).
func (m *Metrics) RecordStoreOpDuration(ctx context.Context, op, backend string, d time.Duration) {
	if m == nil {
		return
	}
	m.storeOpDuration.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.String("op", op),
		attribute.String("backend", backend),
	))
}

// SyncPendingCount synchronizes the pending count for an event.
// Use this to correct the gauge after initialization or recovery.
func (m *Metrics) SyncPendingCount(ctx context.Context, eventName string, count int64) {
	if m == nil {
		return
	}

	m.pendingMu.Lock()
	oldCount := m.pendingCounts[eventName]
	m.pendingCounts[eventName] = count
	m.pendingMu.Unlock()

	// Adjust the gauge by the difference
	diff := count - oldCount
	if diff != 0 {
		m.messagesPending.Add(ctx, diff, metric.WithAttributes(attribute.String("event", eventName)))
	}
}

// normalizeErrorType extracts a simplified error type from an error message.
// This helps reduce cardinality by grouping similar errors.
func normalizeErrorType(errMsg string) string {
	if errMsg == "" {
		return "unknown"
	}

	// Take the first part before colon (common error format)
	for i, c := range errMsg {
		if c == ':' {
			if i > 0 {
				return errMsg[:i]
			}
			break
		}
	}

	// Truncate long error messages. The 50 is a byte budget; if the cut lands in
	// the middle of a multi-byte rune, back off to that rune's start so we never
	// split a valid rune into invalid UTF-8 (which would corrupt the metric
	// attribute). Only the cut point is adjusted — bytes before it are left as-is.
	const maxLen = 50
	if len(errMsg) > maxLen {
		end := maxLen
		for end > 0 && !utf8.RuneStart(errMsg[end]) {
			end--
		}
		if end == 0 {
			// Pathological input with no rune boundary in the budget (e.g. all
			// continuation bytes). Keep the raw cut so we never return empty;
			// such input was already invalid UTF-8 going in.
			end = maxLen
		}
		return errMsg[:end]
	}

	return errMsg
}
