package dlq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

// BenchmarkMessageCreation benchmarks creating Message structs
func BenchmarkMessageCreation(b *testing.B) {
	now := time.Now()
	payload := []byte(`{"order_id": "12345", "amount": 99.99}`)
	metadata := map[string]string{"source": "checkout"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Message{
			ID:         "dlq-msg-" + string(rune(i)),
			EventName:  "orders.created",
			OriginalID: "orig-" + string(rune(i)),
			Payload:    payload,
			Metadata:   metadata,
			Error:      "processing failed",
			RetryCount: 3,
			CreatedAt:  now,
			Source:     "order-service",
		}
	}
}

// BenchmarkMemoryStoreStore benchmarks the Store operation
func BenchmarkMemoryStoreStore(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	payload := []byte(`{"order_id": "12345"}`)
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := &Message{
			ID:         "dlq-msg-" + string(rune(i)),
			EventName:  "orders.created",
			OriginalID: "orig-" + string(rune(i)),
			Payload:    payload,
			Error:      "processing failed",
			RetryCount: 3,
			CreatedAt:  now,
			Source:     "order-service",
		}
		_ = store.Store(ctx, msg)
	}
}

// BenchmarkMemoryStoreGet benchmarks the Get operation
func BenchmarkMemoryStoreGet(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	payload := []byte(`{"order_id": "12345"}`)
	now := time.Now()

	// Pre-populate store
	for i := 0; i < 1000; i++ {
		msg := &Message{
			ID:         "dlq-msg-" + string(rune(i)),
			EventName:  "orders.created",
			OriginalID: "orig-" + string(rune(i)),
			Payload:    payload,
			Error:      "processing failed",
			RetryCount: 3,
			CreatedAt:  now,
			Source:     "order-service",
		}
		_ = store.Store(ctx, msg)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Get(ctx, "dlq-msg-"+string(rune(i%1000)))
	}
}

// BenchmarkMemoryStoreList benchmarks the List operation
func BenchmarkMemoryStoreList(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	payload := []byte(`{"order_id": "12345"}`)
	now := time.Now()

	// Pre-populate store
	for i := 0; i < 1000; i++ {
		msg := &Message{
			ID:         "dlq-msg-" + string(rune(i)),
			EventName:  "orders.created",
			OriginalID: "orig-" + string(rune(i)),
			Payload:    payload,
			Error:      "processing failed",
			RetryCount: 3,
			CreatedAt:  now,
			Source:     "order-service",
		}
		_ = store.Store(ctx, msg)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.List(ctx, Filter{Limit: 100})
	}
}

// BenchmarkMemoryStoreCount benchmarks the Count operation
func BenchmarkMemoryStoreCount(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	payload := []byte(`{"order_id": "12345"}`)
	now := time.Now()

	// Pre-populate store
	for i := 0; i < 1000; i++ {
		msg := &Message{
			ID:         "dlq-msg-" + string(rune(i)),
			EventName:  "orders.created",
			OriginalID: "orig-" + string(rune(i)),
			Payload:    payload,
			Error:      "processing failed",
			RetryCount: 3,
			CreatedAt:  now,
			Source:     "order-service",
		}
		_ = store.Store(ctx, msg)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Count(ctx, Filter{})
	}
}

// BenchmarkManagerStore benchmarks the Manager.Store operation
func BenchmarkManagerStore(b *testing.B) {
	ctx := context.Background()
	store := NewMemoryStore()
	tr := &benchTransport{}
	manager := NewManagerWithTransport(store, tr)
	payload := []byte(`{"order_id": "12345"}`)
	err := errors.New("processing failed")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = manager.Store(ctx, "orders.created", "orig-"+string(rune(i)), payload,
			map[string]string{"key": "value"}, err, 3, "order-service")
	}
}

// BenchmarkFilterCreation benchmarks creating Filter structs
func BenchmarkFilterCreation(b *testing.B) {
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Filter{
			EventName:      "orders.created",
			StartTime:      now.Add(-24 * time.Hour),
			EndTime:        now,
			ExcludeRetried: true,
			Limit:          100,
		}
	}
}

// benchTransport is a minimal transport for benchmarks
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
