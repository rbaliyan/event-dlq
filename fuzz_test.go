package dlq

import (
	"context"
	"testing"
	"time"
)

func FuzzMatchesFilter(f *testing.F) {
	f.Add("orders.created", "connection refused", "order-service", 3, true, false)
	f.Add("", "", "", 0, false, false)
	f.Add("payments.failed", "timeout: deadline exceeded", "payment-svc", 10, false, true)
	f.Add("users.updated", "duplicate key", "", -1, true, true)

	f.Fuzz(func(t *testing.T, eventName, errMsg, source string, retryCount int, excludeRetried, retried bool) {
		store := NewMemoryStore()
		msg := &Message{
			ID:         "fuzz-msg-1",
			EventName:  eventName,
			OriginalID: "orig-1",
			Payload:    []byte(`{"test":true}`),
			Error:      errMsg,
			RetryCount: retryCount,
			CreatedAt:  time.Now(),
			Source:     source,
		}
		if retried {
			now := time.Now()
			msg.RetriedAt = &now
		}

		filter := Filter{
			EventName:      eventName,
			Error:          errMsg,
			Source:         source,
			MaxRetries:     retryCount,
			ExcludeRetried: excludeRetried,
		}

		_ = store.matchesFilter(msg, filter)
	})
}

func FuzzNormalizeErrorType(f *testing.F) {
	f.Add("connection refused: dial tcp 10.0.0.1:5432")
	f.Add("timeout")
	f.Add("")
	f.Add("duplicate key error: E11000")
	f.Add(": leading colon")
	f.Add("a]very[long^error*message+that/goes-on_and_on!for@more#than$fifty%characters&to(test)truncation")

	f.Fuzz(func(t *testing.T, errMsg string) {
		result := normalizeErrorType(errMsg)
		if result == "" {
			t.Error("normalizeErrorType should never return empty string")
		}
	})
}

func FuzzStoreAndFilterMessages(f *testing.F) {
	f.Add("evt1", "err1", "src1", 1, 10, 0)
	f.Add("", "", "", 0, 0, 0)
	f.Add("orders", "timeout", "api", 5, 100, 50)

	f.Fuzz(func(t *testing.T, eventName, errMsg, source string, retryCount, limit, offset int) {
		if limit < 0 || limit > 1000 {
			limit = 10
		}
		if offset < 0 || offset > 1000 {
			offset = 0
		}

		store := NewMemoryStore()
		ctx := context.Background()

		msg := &Message{
			ID:         "fuzz-1",
			EventName:  eventName,
			OriginalID: "orig-1",
			Payload:    []byte(`{}`),
			Error:      errMsg,
			RetryCount: retryCount,
			CreatedAt:  time.Now(),
			Source:     source,
		}
		_ = store.Store(ctx, msg)

		filter := Filter{
			EventName: eventName,
			Error:     errMsg,
			Source:    source,
			Limit:    limit,
			Offset:   offset,
		}
		_, _ = store.List(ctx, filter)
		_, _ = store.Count(ctx, filter)
	})
}
