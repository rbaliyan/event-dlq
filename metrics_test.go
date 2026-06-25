package dlq

import (
	"context"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// readPending collects all exported metrics from the manual reader and returns
// the summed value of the dlq_messages_pending gauge across every data point.
func readPending(t *testing.T, reader sdkmetric.Reader) int64 {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "dlq_messages_pending" {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("dlq_messages_pending has unexpected data type %T", m.Data)
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			return total
		}
	}
	return 0
}

func newTestMetrics(t *testing.T) (*Metrics, sdkmetric.Reader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := NewMetricsWithProvider(provider)
	if err != nil {
		t.Fatalf("NewMetricsWithProvider: %v", err)
	}
	return m, reader
}

// TestRecordQuarantinedDecrementsPending guards the invariant that every exit
// from the pending pool decrements dlq_messages_pending. A quarantined message
// is excluded from Stats.PendingMessages on every backend, so the gauge must
// drop by one when a message is quarantined — exactly as it does on replay and
// delete. Before the fix, RecordQuarantined only bumped the quarantined counter
// and left the gauge permanently over-reporting.
func TestRecordQuarantinedDecrementsPending(t *testing.T) {
	ctx := context.Background()
	m, reader := newTestMetrics(t)

	if got := readPending(t, reader); got != 0 {
		t.Fatalf("pending gauge should start at 0, got %d", got)
	}

	m.RecordMessageStored(ctx, "order.process", "boom")
	if got := readPending(t, reader); got != 1 {
		t.Fatalf("pending gauge after store = %d, want 1", got)
	}

	m.RecordQuarantined(ctx, "order.process")
	if got := readPending(t, reader); got != 0 {
		t.Fatalf("pending gauge after quarantine = %d, want 0 (quarantine must leave the pending pool)", got)
	}

	// The pending bookkeeping map must also be back to zero for the event.
	m.pendingMu.RLock()
	count := m.pendingCounts["order.process"]
	m.pendingMu.RUnlock()
	if count != 0 {
		t.Fatalf("pendingCounts[order.process] = %d, want 0", count)
	}
}

// TestRecordQuarantinedGaugeMatchesReplayAndDelete asserts the three pending
// exits are symmetric: storing three messages and then removing each via a
// different transition (replay, delete, quarantine) drains the gauge to zero.
func TestRecordQuarantinedGaugeMatchesReplayAndDelete(t *testing.T) {
	ctx := context.Background()
	m, reader := newTestMetrics(t)

	for range 3 {
		m.RecordMessageStored(ctx, "order.process", "boom")
	}
	if got := readPending(t, reader); got != 3 {
		t.Fatalf("pending gauge after 3 stores = %d, want 3", got)
	}

	m.RecordMessageReplayed(ctx, "order.process")
	m.RecordMessageDeleted(ctx, "order.process")
	m.RecordQuarantined(ctx, "order.process")

	if got := readPending(t, reader); got != 0 {
		t.Fatalf("pending gauge after replay+delete+quarantine = %d, want 0", got)
	}
}

// TestRecordQuarantinedNilSafe verifies the nil-receiver guard, matching the
// other Record* helpers (metrics are optional and may be nil).
func TestRecordQuarantinedNilSafe(t *testing.T) {
	var m *Metrics
	// Must not panic.
	m.RecordQuarantined(context.Background(), "order.process")
}
