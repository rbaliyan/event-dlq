package dlq

import (
	"context"
	"testing"
	"time"
)

// TestBackwardCompat_DefaultBehaviorUnchanged locks in that with NO opt-ins
// (no WithTerminalError, no dedup options) the Manager and MemoryStore behave
// exactly as they did before the hardening feature was added.
func TestBackwardCompat_DefaultBehaviorUnchanged(t *testing.T) {
	ctx := context.Background()

	t.Run("ReplayWithoutTerminalError_RepublishesAndMarksRetried", func(t *testing.T) {
		store := NewMemoryStore()
		_ = store.Store(ctx, &Message{
			ID:         "m1",
			EventName:  "e",
			OriginalID: "o1",
			Error:      "transient",
			CreatedAt:  time.Now(),
		})

		rep := &countingRepublisher{}
		mgr, err := NewManager(store, rep) // no WithTerminalError
		if err != nil {
			t.Fatalf("new manager: %v", err)
		}

		n, err := mgr.Replay(ctx, Filter{})
		if err != nil {
			t.Fatalf("replay: %v", err)
		}
		if n != 1 {
			t.Fatalf("want 1 replayed, got %d", n)
		}
		if rep.sends != 1 {
			t.Fatalf("want Send called once, got %d", rep.sends)
		}

		got, err := store.Get(ctx, "m1")
		if err != nil {
			t.Fatalf("store.Get: %v", err)
		}
		if got.RetriedAt == nil {
			t.Fatal("expected RetriedAt set after default replay")
		}
		if got.QuarantinedAt != nil {
			t.Fatal("message must not be quarantined without WithTerminalError")
		}
	})

	t.Run("DefaultMemoryStore_NoDedup", func(t *testing.T) {
		store := NewMemoryStore() // no WithMemoryDedup
		_ = store.Store(ctx, &Message{ID: "a", EventName: "e", OriginalID: "o1", CreatedAt: time.Now()})
		_ = store.Store(ctx, &Message{ID: "b", EventName: "e", OriginalID: "o1", CreatedAt: time.Now()})

		n, err := store.Count(ctx, Filter{})
		if err != nil {
			t.Fatalf("Count: %v", err)
		}
		if n != 2 {
			t.Fatalf("default store must not dedup: want 2, got %d", n)
		}
	})

	t.Run("TerminalPatternIgnoredWhenNotConfigured", func(t *testing.T) {
		store := NewMemoryStore()
		_ = store.Store(ctx, &Message{
			ID:         "p1",
			EventName:  "e",
			OriginalID: "o2",
			Error:      "cannot decode string into a call.EndReason",
			CreatedAt:  time.Now(),
		})

		rep := &countingRepublisher{}
		// no WithTerminalError, despite a "terminal-looking" error
		mgr, err := NewManager(store, rep)
		if err != nil {
			t.Fatalf("new manager: %v", err)
		}

		if _, err := mgr.Replay(ctx, Filter{}); err != nil {
			t.Fatalf("replay: %v", err)
		}
		if rep.sends != 1 {
			t.Fatalf("terminal-looking error must STILL be republished when predicate not set; got %d sends", rep.sends)
		}

		got, err := store.Get(ctx, "p1")
		if err != nil {
			t.Fatalf("store.Get: %v", err)
		}
		if got.QuarantinedAt != nil {
			t.Fatal("must not quarantine without WithTerminalError")
		}
	})
}
