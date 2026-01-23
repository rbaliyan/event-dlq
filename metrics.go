package dlq

import (
	"context"
	"sync"

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
//   - dlq_messages_pending: Gauge of current pending messages in DLQ
//   - dlq_replay_success_total: Counter of successful replays
//   - dlq_replay_failure_total: Counter of failed replays
type Metrics struct {
	messagesTotal        metric.Int64Counter
	messagesReplayedTotal metric.Int64Counter
	messagesDeletedTotal metric.Int64Counter
	messagesPending      metric.Int64UpDownCounter
	replaySuccessTotal   metric.Int64Counter
	replayFailureTotal   metric.Int64Counter

	// pendingCounts tracks pending messages per event for gauge updates
	pendingMu     sync.RWMutex
	pendingCounts map[string]int64
}

// NewMetrics creates a new Metrics instance using the global OpenTelemetry meter provider.
//
// Example:
//
//	metrics := dlq.NewMetrics()
//	manager := dlq.NewManager(store, transport, dlq.WithMetrics(metrics))
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

	return &Metrics{
		messagesTotal:        messagesTotal,
		messagesReplayedTotal: messagesReplayedTotal,
		messagesDeletedTotal: messagesDeletedTotal,
		messagesPending:      messagesPending,
		replaySuccessTotal:   replaySuccessTotal,
		replayFailureTotal:   replayFailureTotal,
		pendingCounts:        make(map[string]int64),
	}, nil
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

	// Truncate long error messages
	if len(errMsg) > 50 {
		return errMsg[:50]
	}

	return errMsg
}
