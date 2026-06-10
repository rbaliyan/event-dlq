package dlq

import (
	"context"
	"testing"
	"time"
)

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
