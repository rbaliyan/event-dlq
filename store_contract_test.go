package dlq

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemoryStoreContract verifies the MemoryStore satisfies the full Store contract.
func TestMemoryStoreContract(t *testing.T) {
	runStoreContractTests(t, NewMemoryStore())
}

// TestRedisStoreContract verifies the RedisStore satisfies the full Store contract
// using an in-process Redis mock.
func TestRedisStoreContract(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewRedisStore(client)
	require.NoError(t, err)
	runStoreContractTests(t, store)
}

// runStoreContractTests verifies a Store implementation satisfies the full
// filter and query contract. The store must be empty on entry.
//
// Seed layout (6 messages):
//
//	ID    Event            Source   Error                RetryCount  Age
//	c-1   order.created    svc-a    "timeout error"      1           4h
//	c-2   order.created    svc-b    "validation failed"  3           3h
//	c-3   order.updated    svc-a    "timeout error"      5           2h
//	c-4   payment.failed   svc-b    "insufficient funds" 2           1h
//	c-5   order.created    svc-a    "connection refused" 0           now
//	c-6   order.updated    svc-b    "db error"           4           30m  (retried)
func runStoreContractTests(t *testing.T, store Store) {
	t.Helper()
	ctx := context.Background()
	base := time.Now().Truncate(time.Second)

	seed := []*Message{
		{ID: "c-1", EventName: "order.created", Source: "svc-a", Error: "timeout error", RetryCount: 1, CreatedAt: base.Add(-4 * time.Hour)},
		{ID: "c-2", EventName: "order.created", Source: "svc-b", Error: "validation failed", RetryCount: 3, CreatedAt: base.Add(-3 * time.Hour)},
		{ID: "c-3", EventName: "order.updated", Source: "svc-a", Error: "timeout error", RetryCount: 5, CreatedAt: base.Add(-2 * time.Hour)},
		{ID: "c-4", EventName: "payment.failed", Source: "svc-b", Error: "insufficient funds", RetryCount: 2, CreatedAt: base.Add(-1 * time.Hour)},
		{ID: "c-5", EventName: "order.created", Source: "svc-a", Error: "connection refused", RetryCount: 0, CreatedAt: base},
		{ID: "c-6", EventName: "order.updated", Source: "svc-b", Error: "db error", RetryCount: 4, CreatedAt: base.Add(-30 * time.Minute)},
	}
	for _, m := range seed {
		m.Payload = []byte(`{}`)
		m.OriginalID = "orig-" + m.ID
		require.NoError(t, store.Store(ctx, m), "seed Store %s", m.ID)
	}
	require.NoError(t, store.MarkRetried(ctx, "c-6"))

	t.Run("Get/Found", func(t *testing.T) {
		msg, err := store.Get(ctx, "c-1")
		require.NoError(t, err)
		assert.Equal(t, "c-1", msg.ID)
		assert.Equal(t, "order.created", msg.EventName)
		assert.Equal(t, "svc-a", msg.Source)
		assert.Equal(t, 1, msg.RetryCount)
		assert.Equal(t, "orig-c-1", msg.OriginalID)
		assert.Equal(t, base.Add(-4*time.Hour).Unix(), msg.CreatedAt.Unix())
	})

	t.Run("Get/NotFound", func(t *testing.T) {
		_, err := store.Get(ctx, "no-such-id")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("List/All", func(t *testing.T) {
		results, err := store.List(ctx, Filter{})
		require.NoError(t, err)
		assert.Len(t, results, 6)
	})

	t.Run("List/EventName", func(t *testing.T) {
		// c-1, c-2, c-5 = 3
		results, err := store.List(ctx, Filter{EventName: "order.created"})
		require.NoError(t, err)
		assert.Len(t, results, 3)
		for _, r := range results {
			assert.Equal(t, "order.created", r.EventName)
		}
	})

	t.Run("List/After", func(t *testing.T) {
		// base-2h and newer: c-3, c-4, c-5, c-6 = 4
		results, err := store.List(ctx, Filter{After: base.Add(-2 * time.Hour)})
		require.NoError(t, err)
		assert.Len(t, results, 4)
		for _, r := range results {
			assert.False(t, r.CreatedAt.Before(base.Add(-2*time.Hour)))
		}
	})

	t.Run("List/Before", func(t *testing.T) {
		// base-2h and older: c-1, c-2, c-3 = 3
		results, err := store.List(ctx, Filter{Before: base.Add(-2 * time.Hour)})
		require.NoError(t, err)
		assert.Len(t, results, 3)
		for _, r := range results {
			assert.False(t, r.CreatedAt.After(base.Add(-2*time.Hour)))
		}
	})

	t.Run("List/TimeRange", func(t *testing.T) {
		// between base-3h and base-1h: c-2, c-3, c-4 = 3
		results, err := store.List(ctx, Filter{
			After: base.Add(-3 * time.Hour),
			Before:   base.Add(-1 * time.Hour),
		})
		require.NoError(t, err)
		assert.Len(t, results, 3)
	})

	t.Run("List/EventName+StartTime", func(t *testing.T) {
		// order.created from base-3h onward: c-2, c-5 = 2
		results, err := store.List(ctx, Filter{
			EventName: "order.created",
			After: base.Add(-3 * time.Hour),
		})
		require.NoError(t, err)
		assert.Len(t, results, 2)
		for _, r := range results {
			assert.Equal(t, "order.created", r.EventName)
		}
	})

	t.Run("List/Source", func(t *testing.T) {
		// svc-a: c-1, c-3, c-5 = 3
		results, err := store.List(ctx, Filter{Source: "svc-a"})
		require.NoError(t, err)
		assert.Len(t, results, 3)
		for _, r := range results {
			assert.Equal(t, "svc-a", r.Source)
		}
	})

	t.Run("List/ErrorContains", func(t *testing.T) {
		// case-insensitive "TIMEOUT" matches c-1, c-3 = 2
		results, err := store.List(ctx, Filter{Error: "TIMEOUT"})
		require.NoError(t, err)
		assert.Len(t, results, 2)
	})

	t.Run("List/MaxRetries", func(t *testing.T) {
		// retry_count <= 2: c-1(1), c-4(2), c-5(0) = 3
		results, err := store.List(ctx, Filter{MaxRetries: 2})
		require.NoError(t, err)
		assert.Len(t, results, 3)
		for _, r := range results {
			assert.LessOrEqual(t, r.RetryCount, 2)
		}
	})

	t.Run("List/ExcludeRetried", func(t *testing.T) {
		// c-6 is retried → 5
		results, err := store.List(ctx, Filter{ExcludeRetried: true})
		require.NoError(t, err)
		assert.Len(t, results, 5)
		for _, r := range results {
			assert.Nil(t, r.RetriedAt)
		}
	})

	t.Run("List/Limit", func(t *testing.T) {
		results, err := store.List(ctx, Filter{Limit: 3})
		require.NoError(t, err)
		assert.Len(t, results, 3)
	})

	t.Run("List/Offset", func(t *testing.T) {
		// 6 total; offset=4 → 2 remaining
		results, err := store.List(ctx, Filter{Offset: 4})
		require.NoError(t, err)
		assert.Len(t, results, 2)
	})

	t.Run("List/OffsetPlusLimit", func(t *testing.T) {
		results, err := store.List(ctx, Filter{Offset: 2, Limit: 2})
		require.NoError(t, err)
		assert.Len(t, results, 2)
	})

	t.Run("List/OffsetBeyondTotal", func(t *testing.T) {
		results, err := store.List(ctx, Filter{Offset: 100})
		require.NoError(t, err)
		assert.Empty(t, results)
	})

	t.Run("Count/Total", func(t *testing.T) {
		count, err := store.Count(ctx, Filter{})
		require.NoError(t, err)
		assert.Equal(t, int64(6), count)
	})

	t.Run("Count/EventName", func(t *testing.T) {
		count, err := store.Count(ctx, Filter{EventName: "order.created"})
		require.NoError(t, err)
		assert.Equal(t, int64(3), count)
	})

	t.Run("Count/After", func(t *testing.T) {
		count, err := store.Count(ctx, Filter{After: base.Add(-2 * time.Hour)})
		require.NoError(t, err)
		assert.Equal(t, int64(4), count)
	})

	t.Run("Count/Source", func(t *testing.T) {
		// svc-b: c-2, c-4, c-6 = 3
		count, err := store.Count(ctx, Filter{Source: "svc-b"})
		require.NoError(t, err)
		assert.Equal(t, int64(3), count)
	})

	t.Run("Count/MatchesList", func(t *testing.T) {
		// Count and List must agree for every filter combination.
		filter := Filter{EventName: "order.created", After: base.Add(-3 * time.Hour)}
		results, err := store.List(ctx, filter)
		require.NoError(t, err)
		count, err := store.Count(ctx, filter)
		require.NoError(t, err)
		assert.Equal(t, int64(len(results)), count)
	})

	// Destructive tests below — each manages its own temporary data.

	t.Run("MarkRetried/SetsField", func(t *testing.T) {
		tmp := &Message{
			ID: "c-mark-tmp", EventName: "order.created",
			Payload: []byte("{}"), Error: "e", CreatedAt: base,
		}
		require.NoError(t, store.Store(ctx, tmp))
		t.Cleanup(func() { _ = store.Delete(ctx, "c-mark-tmp") })

		got, err := store.Get(ctx, "c-mark-tmp")
		require.NoError(t, err)
		assert.Nil(t, got.RetriedAt)

		require.NoError(t, store.MarkRetried(ctx, "c-mark-tmp"))

		got, err = store.Get(ctx, "c-mark-tmp")
		require.NoError(t, err)
		assert.NotNil(t, got.RetriedAt)
	})

	t.Run("MarkRetried/NotFound", func(t *testing.T) {
		err := store.MarkRetried(ctx, "no-such-message")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("Delete/RemovesMessage", func(t *testing.T) {
		tmp := &Message{
			ID: "c-del-tmp", EventName: "order.created",
			Payload: []byte("{}"), Error: "e", CreatedAt: base,
		}
		require.NoError(t, store.Store(ctx, tmp))

		require.NoError(t, store.Delete(ctx, "c-del-tmp"))

		_, err := store.Get(ctx, "c-del-tmp")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNotFound)

		// Count should be back to 6 (temporary messages from other subtests
		// are cleaned up by their own t.Cleanup before this point).
		count, err := store.Count(ctx, Filter{})
		require.NoError(t, err)
		assert.Equal(t, int64(6), count)
	})

	t.Run("Delete/NotFound", func(t *testing.T) {
		err := store.Delete(ctx, "no-such-message")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNotFound)
	})

	t.Run("DeleteOlderThan/RemovesOld", func(t *testing.T) {
		old := &Message{
			ID: "c-doa-tmp", EventName: "old.event",
			Payload: []byte("{}"), Error: "e",
			CreatedAt: base.Add(-48 * time.Hour),
		}
		require.NoError(t, store.Store(ctx, old))

		deleted, err := store.DeleteOlderThan(ctx, 24*time.Hour)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, deleted, int64(1))

		_, err = store.Get(ctx, "c-doa-tmp")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNotFound)

		// Recent messages survive
		_, err = store.Get(ctx, "c-5")
		require.NoError(t, err)
	})
}
