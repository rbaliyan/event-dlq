package dlq

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/rbaliyan/event/v3/health"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewManager_NilArgs asserts the constructors reject nil dependencies with a
// sentinel error rather than panicking (CLAUDE.md "no panics" rule).
func TestNewManager_NilArgs(t *testing.T) {
	t.Run("NilStore", func(t *testing.T) {
		_, err := NewManager(nil, &countingRepublisher{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "store is required")
	})

	t.Run("NilRepublisher", func(t *testing.T) {
		_, err := NewManager(NewMemoryStore(), nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "republisher is required")
	})

	t.Run("NilTransport", func(t *testing.T) {
		_, err := NewManagerWithTransport(NewMemoryStore(), nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "transport is required")
	})
}

// TestManager_Store_StoreError verifies a backend write failure is wrapped and
// surfaced rather than swallowed.
func TestManager_Store_StoreError(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("backend unavailable")
	mgr, err := NewManager(&recordingStore{storeErr: boom}, &countingRepublisher{})
	require.NoError(t, err)

	err = mgr.Store(ctx, StoreParams{EventName: "e", OriginalID: "o1", Err: errors.New("orig")})
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "store dlq message")
}

// TestReplay_StoreNotQuarantiner_SkipsAndWarnsOnce drives the replay path where a
// message must be quarantined (replay cap exceeded) but the store does not
// implement Quarantiner: nothing is republished, nothing is marked retried, and
// the operator is warned exactly once for the whole sweep.
func TestReplay_StoreNotQuarantiner_SkipsAndWarnsOnce(t *testing.T) {
	ctx := context.Background()
	logger, handler := newCapturingLogger()

	// Two messages already at the replay cap; neither can be quarantined.
	store := &recordingStore{
		listMsgs: []*Message{
			{ID: "m1", EventName: "e", OriginalID: "o1", Metadata: map[string]string{MetadataReplayCount: "1"}},
			{ID: "m2", EventName: "e", OriginalID: "o2", Metadata: map[string]string{MetadataReplayCount: "1"}},
		},
	}
	rep := &countingRepublisher{}
	mgr, err := NewManager(store, rep, WithMaxReplayAttempts(1), WithLogger(logger))
	require.NoError(t, err)

	replayed, err := mgr.Replay(ctx, Filter{})
	require.NoError(t, err)
	assert.Equal(t, 0, replayed, "messages over the cap must not be replayed")
	assert.Equal(t, 0, rep.sends, "nothing should be republished")
	assert.Empty(t, store.markedIDs, "nothing should be marked retried")
	assert.Equal(t, 1, handler.countLevel(slog.LevelWarn),
		"the cannot-quarantine warning must be emitted exactly once per sweep")
}

// TestReplaySingle_StoreNotQuarantiner returns the not-a-Quarantiner sentinel when
// a capped message is replayed by ID against a store that cannot quarantine.
func TestReplaySingle_StoreNotQuarantiner(t *testing.T) {
	ctx := context.Background()
	store := &recordingStore{
		getMsg: &Message{ID: "m1", EventName: "e", OriginalID: "o1", Metadata: map[string]string{MetadataReplayCount: "2"}},
	}
	mgr, err := NewManager(store, &countingRepublisher{}, WithMaxReplayAttempts(1))
	require.NoError(t, err)

	err = mgr.ReplaySingle(ctx, "m1")
	require.Error(t, err)
	assert.ErrorIs(t, err, errStoreNotQuarantiner)
}

// TestReplaySingle_ReplayError asserts that when the transport publish fails, the
// error is wrapped and the message is NOT marked retried.
func TestReplaySingle_ReplayError(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	require.NoError(t, store.Store(ctx, &Message{
		ID: "m1", EventName: "fail.event", OriginalID: "o1", Payload: []byte(`{}`),
	}))

	tr := &mockTransport{failOn: "fail.event"}
	mgr := mustNewManagerWithTransport(t, store, tr)

	err := mgr.ReplaySingle(ctx, "m1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "replay message")

	got, err := store.Get(ctx, "m1")
	require.NoError(t, err)
	assert.Nil(t, got.RetriedAt, "a failed replay must not mark the message retried")
}

// TestReplaySingle_QuarantineWriteError surfaces the store's quarantine error
// (versus the not-a-Quarantiner sentinel) when the cap is exceeded.
func TestReplaySingle_QuarantineWriteError(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("quarantine write failed")
	store := &quarantinerRecordingStore{quarantineErr: boom}
	store.getMsg = &Message{ID: "m1", EventName: "e", OriginalID: "o1", Metadata: map[string]string{MetadataReplayCount: "3"}}

	mgr, err := NewManager(store, &countingRepublisher{}, WithMaxReplayAttempts(1))
	require.NoError(t, err)

	err = mgr.ReplaySingle(ctx, "m1")
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
}

// TestManager_Health exercises all three status branches plus the
// second-Count-fails degraded path.
func TestManager_Health(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("store down")

	t.Run("HealthyWhenNoPending", func(t *testing.T) {
		store := &recordingStore{countQueue: []countResult{{val: 0}, {val: 0}}}
		mgr, err := NewManager(store, &countingRepublisher{})
		require.NoError(t, err)

		res := mgr.Health(ctx)
		assert.Equal(t, health.StatusHealthy, res.Status)
		assert.Equal(t, int64(0), res.Details["pending_messages"])
	})

	t.Run("DegradedWhenPending", func(t *testing.T) {
		store := &recordingStore{countQueue: []countResult{{val: 5}, {val: 2}}}
		mgr, err := NewManager(store, &countingRepublisher{})
		require.NoError(t, err)

		res := mgr.Health(ctx)
		assert.Equal(t, health.StatusDegraded, res.Status)
		assert.Contains(t, res.Message, "2 messages pending")
		assert.Equal(t, int64(5), res.Details["total_messages"])
	})

	t.Run("UnhealthyWhenCountFails", func(t *testing.T) {
		store := &recordingStore{countQueue: []countResult{{err: boom}}}
		mgr, err := NewManager(store, &countingRepublisher{})
		require.NoError(t, err)

		res := mgr.Health(ctx)
		assert.Equal(t, health.StatusUnhealthy, res.Status)
		assert.Contains(t, res.Message, "store connectivity failed")
	})

	t.Run("DegradedWhenPendingCountFails", func(t *testing.T) {
		store := &recordingStore{countQueue: []countResult{{val: 9}, {err: boom}}}
		mgr, err := NewManager(store, &countingRepublisher{})
		require.NoError(t, err)

		res := mgr.Health(ctx)
		assert.Equal(t, health.StatusDegraded, res.Status)
		assert.Contains(t, res.Message, "failed to count pending")
		assert.Equal(t, int64(9), res.Details["total_messages"])
	})
}

// TestReplay_MarkRetriedError_LogsButContinues verifies that a failure to mark a
// message retried after a successful republish is logged but still counts the
// replay (the message was delivered).
func TestReplay_MarkRetriedError_LogsButContinues(t *testing.T) {
	ctx := context.Background()
	logger, handler := newCapturingLogger()
	store := &recordingStore{
		listMsgs: []*Message{{ID: "m1", EventName: "e", OriginalID: "o1", Payload: []byte(`{}`)}},
		markErr:  errors.New("mark failed"),
	}
	rep := &countingRepublisher{}
	mgr, err := NewManager(store, rep, WithLogger(logger))
	require.NoError(t, err)

	replayed, err := mgr.Replay(ctx, Filter{})
	require.NoError(t, err)
	assert.Equal(t, 1, replayed, "republished message counts even if MarkRetried fails")
	assert.Equal(t, 1, rep.sends)
	assert.GreaterOrEqual(t, handler.countLevel(slog.LevelError), 1, "MarkRetried failure must be logged")
}

// TestManager_Delete_GetError verifies Delete surfaces the Get error before
// attempting the delete.
func TestManager_Delete_GetError(t *testing.T) {
	ctx := context.Background()
	store := &recordingStore{getErr: ErrNotFound}
	mgr, err := NewManager(store, &countingRepublisher{})
	require.NoError(t, err)

	err = mgr.Delete(ctx, "missing")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNotFound)
	assert.Empty(t, store.deletedIDs, "Delete must not be attempted when Get fails")
}

// TestReplaySingle_BackoffCanceledByContext covers the ctx-cancellation branch of
// the retry/backoff loop: a failing republish enters backoff, and an already-
// canceled context aborts the wait with the context error.
func TestReplaySingle_BackoffCanceledByContext(t *testing.T) {
	store := &recordingStore{getMsg: &Message{ID: "m1", EventName: "e", OriginalID: "o1", Payload: []byte(`{}`)}}
	mgr, err := NewManager(store, &failingRepublisher{}, WithMaxRetries(1), WithBackoff(slowBackoff{}))
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = mgr.ReplaySingle(ctx, "m1")
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

// TestManager_GetByOriginalID delegates to the store and surfaces ErrNotFound.
func TestManager_GetByOriginalID(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	require.NoError(t, store.Store(ctx, &Message{
		ID: "d1", EventName: "e", OriginalID: "orig-1", Payload: []byte("{}"),
	}))
	mgr, err := NewManager(store, &countingRepublisher{})
	require.NoError(t, err)

	got, err := mgr.GetByOriginalID(ctx, "orig-1")
	require.NoError(t, err)
	assert.Equal(t, "d1", got.ID)

	_, err = mgr.GetByOriginalID(ctx, "missing")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNotFound)
}

// TestManager_Stats_Fallback verifies the Count-based stats computed when the
// store does not implement StatsProvider.
func TestManager_Stats_Fallback(t *testing.T) {
	ctx := context.Background()

	t.Run("ComputesFromCounts", func(t *testing.T) {
		// recordingStore implements neither StatsProvider nor Quarantiner.
		store := &recordingStore{countQueue: []countResult{{val: 10}, {val: 4}}}
		mgr, err := NewManager(store, &countingRepublisher{})
		require.NoError(t, err)

		stats, err := mgr.Stats(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(10), stats.TotalMessages)
		assert.Equal(t, int64(4), stats.PendingMessages)
		assert.Equal(t, int64(6), stats.RetriedMessages)
	})

	t.Run("PropagatesCountError", func(t *testing.T) {
		boom := errors.New("count failed")
		store := &recordingStore{countQueue: []countResult{{err: boom}}}
		mgr, err := NewManager(store, &countingRepublisher{})
		require.NoError(t, err)

		_, err = mgr.Stats(ctx)
		require.Error(t, err)
		assert.ErrorIs(t, err, boom)
	})
}
