package dlq

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newMaxLimitRedisStore(t *testing.T, opts ...RedisStoreOption) *RedisStore {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewRedisStore(client, opts...)
	require.NoError(t, err)
	return store
}

func seedRedis(t *testing.T, store *RedisStore, n int) {
	t.Helper()
	ctx := context.Background()
	base := time.Now().Add(-time.Duration(n) * time.Second)
	for i := range n {
		require.NoError(t, store.Store(ctx, &Message{
			ID:         "m-" + strconv.Itoa(i),
			EventName:  "e",
			OriginalID: "o-" + strconv.Itoa(i),
			Payload:    []byte(`{}`),
			CreatedAt:  base.Add(time.Duration(i) * time.Second),
		}))
	}
}

// TestRedisMaxListLimit verifies the opt-in cap clamps List but leaves Count
// accurate (Count must not be capped).
func TestRedisMaxListLimit(t *testing.T) {
	ctx := context.Background()
	store := newMaxLimitRedisStore(t, WithRedisMaxListLimit(10))
	seedRedis(t, store, 25)

	// List with no explicit Limit is clamped to the cap.
	got, err := store.List(ctx, Filter{})
	require.NoError(t, err)
	assert.Len(t, got, 10, "List with no Limit must clamp to the max")

	// An explicit Limit above the cap is also clamped.
	got, err = store.List(ctx, Filter{Limit: 1000})
	require.NoError(t, err)
	assert.Len(t, got, 10)

	// A Limit below the cap is honored.
	got, err = store.List(ctx, Filter{Limit: 3})
	require.NoError(t, err)
	assert.Len(t, got, 3)

	// Count must report the true total, not the capped value.
	count, err := store.Count(ctx, Filter{})
	require.NoError(t, err)
	assert.Equal(t, int64(25), count, "Count must be accurate despite the list cap")
}

// TestRedisListChunkedPaging exercises the multi-chunk sorted-set walk with a
// small chunk size: List must still return the full result set (no dropped tail)
// and respect Offset/Limit across chunk boundaries.
func TestRedisListChunkedPaging(t *testing.T) {
	orig := redisListChunkSize
	redisListChunkSize = 3
	t.Cleanup(func() { redisListChunkSize = orig })

	ctx := context.Background()
	store := newMaxLimitRedisStore(t) // no cap
	seedRedis(t, store, 10)           // spans 4 chunks of 3

	all, err := store.List(ctx, Filter{})
	require.NoError(t, err)
	assert.Len(t, all, 10, "all messages returned across chunk boundaries")

	count, err := store.Count(ctx, Filter{})
	require.NoError(t, err)
	assert.Equal(t, int64(10), count)

	// Offset + Limit spanning chunk boundaries.
	page, err := store.List(ctx, Filter{Offset: 4, Limit: 3})
	require.NoError(t, err)
	assert.Len(t, page, 3)
}
