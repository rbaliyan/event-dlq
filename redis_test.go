package dlq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rbaliyan/event/v3/health"
)

func setupRedisStore(t *testing.T) (*RedisStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store, err := NewRedisStore(client)
	require.NoError(t, err)
	return store, mr
}

func newRedisMessage(id, eventName string) *Message {
	return &Message{
		ID:         id,
		EventName:  eventName,
		OriginalID: "orig-" + id,
		Payload:    []byte(`{"key":"value"}`),
		Metadata:   map[string]string{"env": "test"},
		Error:      "processing failed",
		RetryCount: 3,
		CreatedAt:  time.Now(),
		Source:     "test-service",
	}
}

func TestRedisStore_Store(t *testing.T) {
	store, _ := setupRedisStore(t)
	ctx := context.Background()

	msg := newRedisMessage("dlq-1", "order.created")
	err := store.Store(ctx, msg)
	require.NoError(t, err)

	// Verify the message can be retrieved
	retrieved, err := store.Get(ctx, "dlq-1")
	require.NoError(t, err)
	assert.Equal(t, "dlq-1", retrieved.ID)
	assert.Equal(t, "order.created", retrieved.EventName)
}

func TestRedisStore_Store_NilMessage(t *testing.T) {
	store, _ := setupRedisStore(t)
	ctx := context.Background()

	err := store.Store(ctx, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "message is nil")
}

func TestRedisStore_Store_EmptyID(t *testing.T) {
	store, _ := setupRedisStore(t)
	ctx := context.Background()

	msg := newRedisMessage("", "order.created")
	err := store.Store(ctx, msg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "message ID is required")
}

func TestRedisStore_Get(t *testing.T) {
	store, _ := setupRedisStore(t)
	ctx := context.Background()

	msg := &Message{
		ID:         "dlq-get-1",
		EventName:  "payment.failed",
		OriginalID: "orig-pay-1",
		Payload:    []byte(`{"amount":100}`),
		Metadata:   map[string]string{"currency": "USD", "region": "us-east"},
		Error:      "insufficient funds",
		RetryCount: 5,
		CreatedAt:  time.Now().Truncate(time.Second),
		Source:     "payment-service",
	}

	err := store.Store(ctx, msg)
	require.NoError(t, err)

	retrieved, err := store.Get(ctx, "dlq-get-1")
	require.NoError(t, err)

	assert.Equal(t, msg.ID, retrieved.ID)
	assert.Equal(t, msg.EventName, retrieved.EventName)
	assert.Equal(t, msg.OriginalID, retrieved.OriginalID)
	assert.Equal(t, msg.Payload, retrieved.Payload)
	assert.Equal(t, msg.Metadata, retrieved.Metadata)
	assert.Equal(t, msg.Error, retrieved.Error)
	assert.Equal(t, msg.RetryCount, retrieved.RetryCount)
	assert.Equal(t, msg.Source, retrieved.Source)
	// Redis stores unix timestamps, so compare at second granularity
	assert.Equal(t, msg.CreatedAt.Unix(), retrieved.CreatedAt.Unix())
	assert.Nil(t, retrieved.RetriedAt)
}

func TestRedisStore_Get_NotFound(t *testing.T) {
	store, _ := setupRedisStore(t)
	ctx := context.Background()

	_, err := store.Get(ctx, "non-existent-id")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound), "expected ErrNotFound, got: %v", err)
}

func TestRedisStore_List(t *testing.T) {
	store, _ := setupRedisStore(t)
	ctx := context.Background()

	// Store messages with different event names
	now := time.Now()
	msgs := []*Message{
		{ID: "dlq-list-1", EventName: "order.created", OriginalID: "o1", Payload: []byte("p1"), Error: "err1", CreatedAt: now, Source: "svc-a"},
		{ID: "dlq-list-2", EventName: "order.updated", OriginalID: "o2", Payload: []byte("p2"), Error: "err2", CreatedAt: now, Source: "svc-b"},
		{ID: "dlq-list-3", EventName: "order.created", OriginalID: "o3", Payload: []byte("p3"), Error: "err3", CreatedAt: now, Source: "svc-a"},
	}
	for _, m := range msgs {
		require.NoError(t, store.Store(ctx, m))
	}

	t.Run("all messages", func(t *testing.T) {
		results, err := store.List(ctx, Filter{})
		require.NoError(t, err)
		assert.Len(t, results, 3)
	})

	t.Run("filter by event name", func(t *testing.T) {
		results, err := store.List(ctx, Filter{EventName: "order.created"})
		require.NoError(t, err)
		assert.Len(t, results, 2)
		for _, r := range results {
			assert.Equal(t, "order.created", r.EventName)
		}
	})

	t.Run("filter by source", func(t *testing.T) {
		results, err := store.List(ctx, Filter{Source: "svc-b"})
		require.NoError(t, err)
		assert.Len(t, results, 1)
		assert.Equal(t, "dlq-list-2", results[0].ID)
	})

	t.Run("filter exclude retried", func(t *testing.T) {
		// Mark one as retried
		require.NoError(t, store.MarkRetried(ctx, "dlq-list-1"))

		results, err := store.List(ctx, Filter{ExcludeRetried: true})
		require.NoError(t, err)
		assert.Len(t, results, 2)
		for _, r := range results {
			assert.Nil(t, r.RetriedAt)
		}
	})

	t.Run("with limit", func(t *testing.T) {
		results, err := store.List(ctx, Filter{Limit: 2})
		require.NoError(t, err)
		assert.Len(t, results, 2)
	})

	t.Run("with offset", func(t *testing.T) {
		results, err := store.List(ctx, Filter{Offset: 2})
		require.NoError(t, err)
		assert.Len(t, results, 1)
	})
}

func TestRedisStore_Delete(t *testing.T) {
	store, _ := setupRedisStore(t)
	ctx := context.Background()

	msg := newRedisMessage("dlq-del-1", "order.created")
	require.NoError(t, store.Store(ctx, msg))

	// Verify it exists
	_, err := store.Get(ctx, "dlq-del-1")
	require.NoError(t, err)

	// Delete it
	err = store.Delete(ctx, "dlq-del-1")
	require.NoError(t, err)

	// Verify it is gone
	_, err = store.Get(ctx, "dlq-del-1")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound))

	// Delete non-existent returns error
	err = store.Delete(ctx, "non-existent")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound))
}

func TestRedisStore_MarkRetried(t *testing.T) {
	store, _ := setupRedisStore(t)
	ctx := context.Background()

	msg := newRedisMessage("dlq-retry-1", "order.created")
	require.NoError(t, store.Store(ctx, msg))

	// Verify not yet retried
	retrieved, err := store.Get(ctx, "dlq-retry-1")
	require.NoError(t, err)
	assert.Nil(t, retrieved.RetriedAt)

	// Mark as retried
	err = store.MarkRetried(ctx, "dlq-retry-1")
	require.NoError(t, err)

	// Verify retried_at is set
	retrieved, err = store.Get(ctx, "dlq-retry-1")
	require.NoError(t, err)
	assert.NotNil(t, retrieved.RetriedAt)

	// Mark non-existent returns error
	err = store.MarkRetried(ctx, "non-existent")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound))
}

func TestRedisStore_DeleteOlderThan(t *testing.T) {
	store, mr := setupRedisStore(t)
	ctx := context.Background()

	now := time.Now()

	// Store an old message (2 hours ago)
	oldMsg := &Message{
		ID:        "dlq-old",
		EventName: "order.created",
		Payload:   []byte("old"),
		Error:     "err",
		CreatedAt: now.Add(-2 * time.Hour),
	}
	require.NoError(t, store.Store(ctx, oldMsg))

	// Store a new message (now)
	newMsg := &Message{
		ID:        "dlq-new",
		EventName: "order.created",
		Payload:   []byte("new"),
		Error:     "err",
		CreatedAt: now,
	}
	require.NoError(t, store.Store(ctx, newMsg))

	// Fast-forward miniredis time isn't needed since we set created_at directly
	_ = mr

	// Delete messages older than 90 minutes
	deleted, err := store.DeleteOlderThan(ctx, 90*time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)

	// Verify the new message still exists
	_, err = store.Get(ctx, "dlq-new")
	require.NoError(t, err)

	// Verify the old message is gone
	_, err = store.Get(ctx, "dlq-old")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound))
}

func TestRedisStore_Count(t *testing.T) {
	store, _ := setupRedisStore(t)
	ctx := context.Background()

	// Empty store
	count, err := store.Count(ctx, Filter{})
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// Store messages
	msgs := []*Message{
		{ID: "dlq-c1", EventName: "order.created", Payload: []byte("p"), Error: "e", CreatedAt: time.Now(), Source: "svc-a"},
		{ID: "dlq-c2", EventName: "order.updated", Payload: []byte("p"), Error: "e", CreatedAt: time.Now(), Source: "svc-b"},
		{ID: "dlq-c3", EventName: "order.created", Payload: []byte("p"), Error: "e", CreatedAt: time.Now(), Source: "svc-a"},
	}
	for _, m := range msgs {
		require.NoError(t, store.Store(ctx, m))
	}

	t.Run("total count", func(t *testing.T) {
		count, err := store.Count(ctx, Filter{})
		require.NoError(t, err)
		assert.Equal(t, int64(3), count)
	})

	t.Run("count by event name", func(t *testing.T) {
		count, err := store.Count(ctx, Filter{EventName: "order.created"})
		require.NoError(t, err)
		assert.Equal(t, int64(2), count)
	})

	t.Run("count with complex filter", func(t *testing.T) {
		count, err := store.Count(ctx, Filter{Source: "svc-b"})
		require.NoError(t, err)
		assert.Equal(t, int64(1), count)
	})
}

func TestRedisStore_Health(t *testing.T) {
	store, _ := setupRedisStore(t)
	ctx := context.Background()

	result := store.Health(ctx)
	assert.Equal(t, health.StatusHealthy, result.Status)
	assert.Contains(t, result.Details, "stream_key")
	assert.Contains(t, result.Details, "message_count")
}

func TestRedisStore_Health_Unhealthy(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store, err := NewRedisStore(client)
	require.NoError(t, err)

	// Close miniredis to simulate connection failure
	mr.Close()

	result := store.Health(context.Background())
	assert.Equal(t, health.StatusUnhealthy, result.Status)
}

func TestRedisStore_Store_Atomicity(t *testing.T) {
	store, _ := setupRedisStore(t)
	ctx := context.Background()

	msg := newRedisMessage("dlq-atomic-1", "order.created")
	err := store.Store(ctx, msg)
	require.NoError(t, err)

	// Verify message can be retrieved with all fields
	got, err := store.Get(ctx, "dlq-atomic-1")
	require.NoError(t, err)
	assert.Equal(t, "dlq-atomic-1", got.ID)
	assert.Equal(t, "order.created", got.EventName)
	assert.Equal(t, "orig-dlq-atomic-1", got.OriginalID)

	// Verify event index was populated (list by event name returns the message)
	msgs, err := store.List(ctx, Filter{EventName: "order.created"})
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "dlq-atomic-1", msgs[0].ID)

	// Verify count works
	count, err := store.Count(ctx, Filter{EventName: "order.created"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestRedisStore_GetByOriginalID(t *testing.T) {
	store, _ := setupRedisStore(t)
	ctx := context.Background()

	msg := newRedisMessage("dlq-orig-1", "order.created")
	require.NoError(t, store.Store(ctx, msg))

	retrieved, err := store.GetByOriginalID(ctx, "orig-dlq-orig-1")
	require.NoError(t, err)
	assert.Equal(t, "dlq-orig-1", retrieved.ID)

	// Not found
	_, err = store.GetByOriginalID(ctx, "non-existent")
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrNotFound))
}

func TestRedisStore_DeleteByFilter(t *testing.T) {
	store, _ := setupRedisStore(t)
	ctx := context.Background()

	msgs := []*Message{
		{ID: "dlq-bf1", EventName: "order.created", Payload: []byte("p"), Error: "e", CreatedAt: time.Now()},
		{ID: "dlq-bf2", EventName: "order.updated", Payload: []byte("p"), Error: "e", CreatedAt: time.Now()},
		{ID: "dlq-bf3", EventName: "order.created", Payload: []byte("p"), Error: "e", CreatedAt: time.Now()},
	}
	for _, m := range msgs {
		require.NoError(t, store.Store(ctx, m))
	}

	deleted, err := store.DeleteByFilter(ctx, Filter{EventName: "order.created"})
	require.NoError(t, err)
	assert.Equal(t, int64(2), deleted)

	count, err := store.Count(ctx, Filter{})
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestRedisStore_Stats(t *testing.T) {
	store, _ := setupRedisStore(t)
	ctx := context.Background()

	msgs := []*Message{
		{ID: "dlq-s1", EventName: "order.created", Payload: []byte("p"), Error: "e", CreatedAt: time.Now()},
		{ID: "dlq-s2", EventName: "order.updated", Payload: []byte("p"), Error: "e", CreatedAt: time.Now()},
		{ID: "dlq-s3", EventName: "order.created", Payload: []byte("p"), Error: "e", CreatedAt: time.Now()},
	}
	for _, m := range msgs {
		require.NoError(t, store.Store(ctx, m))
	}

	// Mark one as retried
	require.NoError(t, store.MarkRetried(ctx, "dlq-s1"))

	stats, err := store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), stats.TotalMessages)
	assert.Equal(t, int64(1), stats.RetriedMessages)
	assert.Equal(t, int64(2), stats.PendingMessages)
	assert.Equal(t, int64(2), stats.MessagesByEvent["order.created"])
	assert.Equal(t, int64(1), stats.MessagesByEvent["order.updated"])
}

func TestNewRedisStore_Options(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})

	t.Run("nil client returns error", func(t *testing.T) {
		_, err := NewRedisStore(nil)
		require.Error(t, err)
	})

	t.Run("custom key prefix", func(t *testing.T) {
		store, err := NewRedisStore(client, WithKeyPrefix("custom:"))
		require.NoError(t, err)
		assert.Equal(t, "custom:messages", store.streamKey)
		assert.Equal(t, "custom:msg:", store.msgPrefix)
	})

	t.Run("custom max length", func(t *testing.T) {
		store, err := NewRedisStore(client, WithMaxLen(1000))
		require.NoError(t, err)
		assert.Equal(t, int64(1000), store.maxLen)
	})
}
