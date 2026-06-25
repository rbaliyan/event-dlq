package dlq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

// benchSizes is the standard store-size sweep for scaling benchmarks.
var benchSizes = []int{100, 1000, 10000}

// discardLogger returns a logger that drops all output, so benchmarks measure
// the operation under test rather than slog text formatting and stdout I/O.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// seedMessages populates store with n messages spread across several event
// names, error strings, sources, and retry counts, with one in five marked
// retried. It gives the scan/aggregate/filter benchmarks a realistic mix
// instead of a single repeated value.
func seedMessages(tb testing.TB, store *MemoryStore, n int) {
	tb.Helper()
	ctx := context.Background()
	events := []string{"orders.created", "orders.updated", "payments.failed", "users.updated"}
	errs := []string{"connection refused", "timeout: deadline exceeded", "duplicate key", "validation failed: bad field"}
	payload := []byte(`{"order_id":"12345","amount":99.99}`)
	base := time.Now().Add(-time.Duration(n) * time.Second)

	for i := 0; i < n; i++ {
		msg := &Message{
			ID:         "dlq-" + strconv.Itoa(i),
			EventName:  events[i%len(events)],
			OriginalID: "orig-" + strconv.Itoa(i),
			Payload:    payload,
			Metadata:   map[string]string{"source": "checkout", "trace": strconv.Itoa(i)},
			Error:      errs[i%len(errs)],
			RetryCount: i % 6,
			CreatedAt:  base.Add(time.Duration(i) * time.Second),
			Source:     "svc-" + strconv.Itoa(i%3),
		}
		if i%5 == 0 {
			ts := msg.CreatedAt.Add(time.Minute)
			msg.RetriedAt = &ts
		}
		if err := store.Store(ctx, msg); err != nil {
			tb.Fatalf("seed store %d: %v", i, err)
		}
	}
}

// BenchmarkMessageCreation benchmarks creating Message structs.
func BenchmarkMessageCreation(b *testing.B) {
	now := time.Now()
	payload := []byte(`{"order_id": "12345", "amount": 99.99}`)
	metadata := map[string]string{"source": "checkout"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Message{
			ID:         "dlq-msg-" + strconv.Itoa(i),
			EventName:  "orders.created",
			OriginalID: "orig-" + strconv.Itoa(i),
			Payload:    payload,
			Metadata:   metadata,
			Error:      "processing failed",
			RetryCount: 3,
			CreatedAt:  now,
			Source:     "order-service",
		}
	}
}

// BenchmarkFilterCreation benchmarks creating Filter structs.
func BenchmarkFilterCreation(b *testing.B) {
	now := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Filter{
			EventName:      "orders.created",
			After:          now.Add(-24 * time.Hour),
			Before:         now,
			ExcludeRetried: true,
			Limit:          100,
		}
	}
}

// BenchmarkMemoryStoreStore benchmarks the insert path into a growing store.
func BenchmarkMemoryStoreStore(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	payload := []byte(`{"order_id": "12345"}`)
	now := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.Store(ctx, &Message{
			ID:         "dlq-msg-" + strconv.Itoa(i),
			EventName:  "orders.created",
			OriginalID: "orig-" + strconv.Itoa(i),
			Payload:    payload,
			Error:      "processing failed",
			RetryCount: 3,
			CreatedAt:  now,
			Source:     "order-service",
		})
	}
}

// BenchmarkMemoryStoreStoreDedup benchmarks the dedup upsert path: every store
// targets the same (EventName, OriginalID), so each call after the first takes
// the increment-and-merge branch rather than a plain insert.
func BenchmarkMemoryStoreStoreDedup(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore(WithMemoryDedup())
	payload := []byte(`{"order_id": "12345"}`)
	now := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.Store(ctx, &Message{
			ID:         "dlq-msg-" + strconv.Itoa(i),
			EventName:  "orders.created",
			OriginalID: "orig-shared",
			Payload:    payload,
			Metadata:   map[string]string{"trace": strconv.Itoa(i)},
			Error:      "processing failed",
			CreatedAt:  now,
		})
	}
}

// BenchmarkMemoryStoreGet benchmarks point lookups across store sizes.
func BenchmarkMemoryStoreGet(b *testing.B) {
	ctx := context.Background()
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			store := NewMemoryStore()
			seedMessages(b, store, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = store.Get(ctx, "dlq-"+strconv.Itoa(i%n))
			}
		})
	}
}

// BenchmarkMemoryStoreGetByOriginalID benchmarks the linear OriginalID scan
// across store sizes (this lookup is O(n) in MemoryStore).
func BenchmarkMemoryStoreGetByOriginalID(b *testing.B) {
	ctx := context.Background()
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			store := NewMemoryStore()
			seedMessages(b, store, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = store.GetByOriginalID(ctx, "orig-"+strconv.Itoa(i%n))
			}
		})
	}
}

// filterCases exercises the all-match path, an event-name match, the
// error-substring branch (strings.ToLower + strings.Contains), the
// exclude-retried branch, and a deep pagination offset.
var filterCases = []struct {
	name   string
	filter Filter
}{
	{"all", Filter{}},
	{"by_event", Filter{EventName: "orders.created"}},
	{"error_substr", Filter{Error: "timeout"}},
	{"exclude_retried", Filter{ExcludeRetried: true}},
	{"paged_deep", Filter{Offset: 90, Limit: 100}},
}

// BenchmarkMemoryStoreList benchmarks List across store size and filter
// selectivity, including the substring and pagination paths.
func BenchmarkMemoryStoreList(b *testing.B) {
	ctx := context.Background()
	for _, n := range benchSizes {
		store := NewMemoryStore()
		seedMessages(b, store, n)
		for _, tc := range filterCases {
			b.Run(fmt.Sprintf("n=%d/%s", n, tc.name), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = store.List(ctx, tc.filter)
				}
			})
		}
	}
}

// BenchmarkMemoryStoreCount benchmarks Count across size and selectivity.
func BenchmarkMemoryStoreCount(b *testing.B) {
	ctx := context.Background()
	for _, n := range benchSizes {
		store := NewMemoryStore()
		seedMessages(b, store, n)
		for _, tc := range filterCases {
			b.Run(fmt.Sprintf("n=%d/%s", n, tc.name), func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_, _ = store.Count(ctx, tc.filter)
				}
			})
		}
	}
}

// BenchmarkMemoryStoreStats benchmarks the Stats aggregation (two map builds
// plus oldest/newest tracking over the whole store) across sizes.
func BenchmarkMemoryStoreStats(b *testing.B) {
	ctx := context.Background()
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			store := NewMemoryStore()
			seedMessages(b, store, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = store.Stats(ctx)
			}
		})
	}
}

// BenchmarkMemoryStoreDeleteByFilter benchmarks bulk deletion. Each iteration
// reseeds a fresh store (deletion is destructive), with setup excluded from the
// timer.
func BenchmarkMemoryStoreDeleteByFilter(b *testing.B) {
	ctx := context.Background()
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store := NewMemoryStore()
				seedMessages(b, store, n)
				b.StartTimer()
				_, _ = store.DeleteByFilter(ctx, Filter{EventName: "orders.created"})
			}
		})
	}
}

// BenchmarkManagerStore benchmarks Manager.Store with logging discarded so the
// measurement reflects the store insert and metadata handling, not slog output.
func BenchmarkManagerStore(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	manager := mustNewManagerWithTransport(b, store, &benchTransport{}, WithLogger(discardLogger()))
	payload := []byte(`{"order_id": "12345"}`)
	err := errors.New("processing failed")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = manager.Store(ctx, StoreParams{
			EventName:  "orders.created",
			OriginalID: "orig-" + strconv.Itoa(i),
			Payload:    payload,
			Metadata:   map[string]string{"key": "value"},
			Err:        err,
			RetryCount: 3,
			Source:     "order-service",
		})
	}
}

// BenchmarkManagerReplay benchmarks the central replay loop (list, per-message
// metadata copy, republish, MarkRetried) across store sizes. Each iteration
// replays a freshly seeded store, with setup excluded from the timer.
func BenchmarkManagerReplay(b *testing.B) {
	ctx := context.Background()
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store := NewMemoryStore()
				seedMessages(b, store, n)
				manager := mustNewManagerWithTransport(b, store, &benchTransport{}, WithLogger(discardLogger()))
				b.StartTimer()
				if _, err := manager.Replay(ctx, Filter{}); err != nil {
					b.Fatalf("replay: %v", err)
				}
			}
			// Report the per-message replay cost so the metadata-copy + republish
			// + MarkRetried overhead is directly legible, not inferred.
			if b.N > 0 {
				b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*n), "ns/msg")
			}
		})
	}
}

// BenchmarkManagerReplayQuarantine benchmarks the replay sweep when every
// message is over the WithMaxReplayAttempts cap, so each is quarantined instead
// of republished (the error-agnostic backstop branch).
func BenchmarkManagerReplayQuarantine(b *testing.B) {
	ctx := context.Background()
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store := NewMemoryStore()
				for j := 0; j < n; j++ {
					_ = store.Store(ctx, &Message{
						ID:         "q-" + strconv.Itoa(j),
						EventName:  "orders.created",
						OriginalID: "qo-" + strconv.Itoa(j),
						Payload:    []byte(`{}`),
						Metadata:   map[string]string{MetadataReplayCount: "3"},
						CreatedAt:  time.Now(),
					})
				}
				manager := mustNewManagerWithTransport(b, store, &benchTransport{},
					WithLogger(discardLogger()), WithMaxReplayAttempts(3))
				b.StartTimer()
				if _, err := manager.Replay(ctx, Filter{}); err != nil {
					b.Fatalf("replay: %v", err)
				}
			}
		})
	}
}

// BenchmarkMemoryStoreDeleteOlderThan benchmarks the age-based cleanup scan
// across store sizes. Each iteration reseeds a fresh store (deletion is
// destructive), with setup excluded from the timer.
func BenchmarkMemoryStoreDeleteOlderThan(b *testing.B) {
	ctx := context.Background()
	for _, n := range benchSizes {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				store := NewMemoryStore()
				seedMessages(b, store, n)
				b.StartTimer()
				_, _ = store.DeleteOlderThan(ctx, time.Minute)
			}
		})
	}
}

// benchTransport is a minimal no-op transport for benchmarks.
type benchTransport struct{}

func (t *benchTransport) Publish(ctx context.Context, event string, msg transport.Message) error {
	return nil
}

func (t *benchTransport) Subscribe(ctx context.Context, event string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	return nil, nil
}

func (t *benchTransport) RegisterEvent(ctx context.Context, event string) error {
	return nil
}

func (t *benchTransport) UnregisterEvent(ctx context.Context, event string) error {
	return nil
}

func (t *benchTransport) Close(ctx context.Context) error {
	return nil
}
