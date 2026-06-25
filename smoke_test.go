package dlq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	event "github.com/rbaliyan/event/v3"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSmokeRedisStore builds a RedisStore backed by an in-process miniredis,
// cleaned up when the test ends.
func newSmokeRedisStore(t *testing.T) *RedisStore {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewRedisStore(client)
	require.NoError(t, err)
	return store
}

// TestSmoke_EndToEnd_BusCaptureToReplay is the headline happy-path sanity check:
// the bus-side adapter captures a failed message into the store, the Manager
// lists it, replays it through the transport, and the message ends up marked
// retried with the DLQ replay metadata attached. No external services.
func TestSmoke_EndToEnd_BusCaptureToReplay(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	// 1. The bus writes a failed delivery into the DLQ via the adapter.
	adapter := NewStoreAdapter(store, "order-service")
	require.NoError(t, adapter.Store(ctx, &event.DLQMessage{
		EventName:  "order.created",
		MessageID:  "evt-1",
		Payload:    []byte(`{"id":1}`),
		Error:      errors.New("handler failed"),
		RetryCount: 3,
		CreatedAt:  time.Now(),
	}))

	// 2. An operator inspects the DLQ.
	rep := &capturingRepublisher{}
	mgr, err := NewManager(store, rep)
	require.NoError(t, err)

	pending, err := mgr.List(ctx, Filter{ExcludeRetried: true})
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, "order-service", pending[0].Source)

	// 3. Replay republishes and marks the message retried.
	n, err := mgr.Replay(ctx, Filter{})
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	require.Len(t, rep.sends, 1)

	got, err := mgr.GetByOriginalID(ctx, "evt-1")
	require.NoError(t, err)
	assert.NotNil(t, got.RetriedAt, "replayed message must be marked retried")

	// 4. Stats reflect the replay.
	stats, err := mgr.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), stats.TotalMessages)
	assert.Equal(t, int64(1), stats.RetriedMessages)
	assert.Equal(t, int64(0), stats.PendingMessages)
}

// TestSmoke_ReplayMetadataContract pins the documented replay metadata attached
// to every republished message (README "Replay Metadata").
func TestSmoke_ReplayMetadataContract(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	require.NoError(t, store.Store(ctx, &Message{
		ID: "d1", EventName: "e", OriginalID: "o1", Payload: []byte(`{}`),
		Error: "orig boom", CreatedAt: time.Now(),
	}))

	rep := &capturingRepublisher{}
	mgr, err := NewManager(store, rep)
	require.NoError(t, err)

	_, err = mgr.Replay(ctx, Filter{})
	require.NoError(t, err)
	require.Len(t, rep.sends, 1)

	md := rep.sends[0]
	assert.Equal(t, "true", md["dlq_replay"])
	assert.Equal(t, "d1", md["dlq_message_id"])
	assert.Equal(t, "orig boom", md["dlq_original_error"])
	assert.Equal(t, "1", md[MetadataReplayCount], "replay counter starts at 1 on first republish")
}

// TestSmoke_ManagerReplay_AcrossBackends runs the core republish→MarkRetried flow
// against every in-process backend so a backend-specific regression in the
// replay path is caught without Docker.
func TestSmoke_ManagerReplay_AcrossBackends(t *testing.T) {
	backends := map[string]func(t *testing.T) Store{
		"memory": func(t *testing.T) Store { return NewMemoryStore() },
		"redis":  func(t *testing.T) Store { return newSmokeRedisStore(t) },
	}

	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := mk(t)
			require.NoError(t, store.Store(ctx, &Message{
				ID: "d1", EventName: "order.created", OriginalID: "o1",
				Payload: []byte(`{}`), Error: "boom", CreatedAt: time.Now(),
			}))

			rep := &countingRepublisher{}
			mgr, err := NewManager(store, rep)
			require.NoError(t, err)

			n, err := mgr.Replay(ctx, Filter{})
			require.NoError(t, err)
			assert.Equal(t, 1, n)
			assert.Equal(t, 1, rep.sends)

			got, err := store.Get(ctx, "d1")
			require.NoError(t, err)
			assert.NotNil(t, got.RetriedAt)
		})
	}
}

// TestSmoke_MaxReplayAttempts_QuarantinesAcrossBackends verifies the
// error-agnostic replay cap quarantines a message that has already reached the
// cap, on every in-process backend (the metadata counter must survive a real
// store round-trip, not just MemoryStore).
func TestSmoke_MaxReplayAttempts_QuarantinesAcrossBackends(t *testing.T) {
	backends := map[string]func(t *testing.T) Store{
		"memory": func(t *testing.T) Store { return NewMemoryStore() },
		"redis":  func(t *testing.T) Store { return newSmokeRedisStore(t) },
	}

	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := mk(t)
			require.NoError(t, store.Store(ctx, &Message{
				ID: "m1", EventName: "e", OriginalID: "o1", Payload: []byte(`{}`),
				Metadata: map[string]string{MetadataReplayCount: "3"}, CreatedAt: time.Now(),
			}))

			rep := &countingRepublisher{}
			mgr, err := NewManager(store, rep, WithMaxReplayAttempts(3))
			require.NoError(t, err)

			n, err := mgr.Replay(ctx, Filter{})
			require.NoError(t, err)
			assert.Equal(t, 0, n, "a message at the cap must not be replayed")
			assert.Equal(t, 0, rep.sends)

			got, err := store.Get(ctx, "m1")
			require.NoError(t, err)
			assert.NotNil(t, got.QuarantinedAt, "message at the cap must be quarantined")
		})
	}
}
