package dlq

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStore_Stats_Quarantine(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	now := time.Now()

	msgs := []*Message{
		{ID: "m1", EventName: "e", OriginalID: "o1", CreatedAt: now},
		{ID: "m2", EventName: "e", OriginalID: "o2", CreatedAt: now},
		{ID: "m3", EventName: "e", OriginalID: "o3", CreatedAt: now},
	}
	for _, m := range msgs {
		if err := s.Store(ctx, m); err != nil {
			t.Fatalf("store %s: %v", m.ID, err)
		}
	}

	// Quarantine one message; pending count must drop, quarantined must increment.
	if err := s.Quarantine(ctx, "m1"); err != nil {
		t.Fatalf("quarantine: %v", err)
	}

	stats, err := s.Stats(ctx)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.TotalMessages != 3 {
		t.Errorf("TotalMessages: want 3, got %d", stats.TotalMessages)
	}
	if stats.QuarantinedMessages != 1 {
		t.Errorf("QuarantinedMessages: want 1, got %d", stats.QuarantinedMessages)
	}
	if stats.PendingMessages != 2 {
		t.Errorf("PendingMessages: want 2 (quarantined excluded), got %d", stats.PendingMessages)
	}
	if stats.RetriedMessages != 0 {
		t.Errorf("RetriedMessages: want 0, got %d", stats.RetriedMessages)
	}
}

func TestMemoryStore_Quarantine(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	now := time.Now()
	if err := s.Store(ctx, &Message{ID: "m1", EventName: "e", OriginalID: "o1", CreatedAt: now}); err != nil {
		t.Fatalf("store: %v", err)
	}

	if err := s.Quarantine(ctx, "m1"); err != nil {
		t.Fatalf("quarantine: %v", err)
	}

	got, err := s.Get(ctx, "m1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.QuarantinedAt == nil {
		t.Fatal("expected QuarantinedAt to be set")
	}

	// ExcludeQuarantined must hide it.
	list, err := s.List(ctx, Filter{ExcludeQuarantined: true})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("expected 0 non-quarantined messages, got %d", len(list))
	}

	// Quarantining a missing ID returns an error.
	if err := s.Quarantine(ctx, "nope"); err == nil {
		t.Fatal("expected error quarantining missing message")
	}
}
