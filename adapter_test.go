package dlq

import (
	"context"
	"errors"
	"testing"
	"time"

	event "github.com/rbaliyan/event/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStoreAdapter_Store verifies the bridge that lets event.WithDLQ write into
// a dlq.Store: field mapping, the configured-source override, the empty-source
// fallback to the inbound message source, and nil-error handling.
func TestStoreAdapter_Store(t *testing.T) {
	ctx := context.Background()
	created := time.Now().Add(-time.Minute).Truncate(time.Second)

	t.Run("MapsFieldsAndOverridesSource", func(t *testing.T) {
		store := NewMemoryStore()
		adapter := NewStoreAdapter(store, "order-service")

		in := &event.DLQMessage{
			EventName:  "order.created",
			MessageID:  "orig-1",
			Payload:    []byte(`{"id":1}`),
			Metadata:   map[string]string{"trace": "abc"},
			Error:      errors.New("downstream boom"),
			RetryCount: 3,
			Source:     "inbound-source",
			CreatedAt:  created,
		}
		require.NoError(t, adapter.Store(ctx, in))

		got, err := store.GetByOriginalID(ctx, "orig-1")
		require.NoError(t, err)
		assert.NotEmpty(t, got.ID, "adapter must generate a DLQ ID")
		assert.Equal(t, "order.created", got.EventName)
		assert.Equal(t, "orig-1", got.OriginalID)
		assert.Equal(t, []byte(`{"id":1}`), got.Payload)
		assert.Equal(t, "abc", got.Metadata["trace"])
		assert.Equal(t, "downstream boom", got.Error)
		assert.Equal(t, 3, got.RetryCount)
		assert.Equal(t, created.Unix(), got.CreatedAt.Unix())
		// The adapter's configured source wins over the inbound source.
		assert.Equal(t, "order-service", got.Source)
	})

	t.Run("EmptySourceFallsBackToMessageSource", func(t *testing.T) {
		store := NewMemoryStore()
		adapter := NewStoreAdapter(store, "")

		require.NoError(t, adapter.Store(ctx, &event.DLQMessage{
			EventName: "order.created",
			MessageID: "orig-2",
			Payload:   []byte(`{}`),
			Error:     errors.New("boom"),
			Source:    "inbound-source",
			CreatedAt: created,
		}))

		got, err := store.GetByOriginalID(ctx, "orig-2")
		require.NoError(t, err)
		assert.Equal(t, "inbound-source", got.Source)
	})

	t.Run("NilErrorStoresEmptyErrorString", func(t *testing.T) {
		store := NewMemoryStore()
		adapter := NewStoreAdapter(store, "svc")

		require.NoError(t, adapter.Store(ctx, &event.DLQMessage{
			EventName: "order.created",
			MessageID: "orig-3",
			Payload:   []byte(`{}`),
			Error:     nil,
			CreatedAt: created,
		}))

		got, err := store.GetByOriginalID(ctx, "orig-3")
		require.NoError(t, err)
		assert.Equal(t, "", got.Error)
	})

	t.Run("PropagatesStoreError", func(t *testing.T) {
		boom := errors.New("store down")
		adapter := NewStoreAdapter(&recordingStore{storeErr: boom}, "svc")
		err := adapter.Store(ctx, &event.DLQMessage{
			EventName: "e", MessageID: "x", Error: errors.New("y"),
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
	})
}

// TestStoreAdapter_ImplementsDLQStore is a compile-and-runtime guard that the
// adapter satisfies the interface event.WithDLQ expects.
func TestStoreAdapter_ImplementsDLQStore(t *testing.T) {
	var _ event.DLQStore = NewStoreAdapter(NewMemoryStore(), "svc")
}
