package dlq

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStore_DedupOffByDefault(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore() // no WithMemoryDedup
	_ = s.Store(ctx, &Message{ID: "a", EventName: "e", OriginalID: "o1", CreatedAt: time.Now()})
	_ = s.Store(ctx, &Message{ID: "b", EventName: "e", OriginalID: "o1", CreatedAt: time.Now()})

	n, _ := s.Count(ctx, Filter{})
	if n != 2 {
		t.Fatalf("dedup must be off by default: want 2 rows, got %d", n)
	}
}

func TestMemoryStore_DedupUpserts(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore(WithMemoryDedup())
	first := time.Now().Add(-time.Hour)
	_ = s.Store(ctx, &Message{ID: "a", EventName: "e", OriginalID: "o1", Error: "err1", RetryCount: 3, CreatedAt: first})
	_ = s.Store(ctx, &Message{ID: "b", EventName: "e", OriginalID: "o1", Error: "err2", RetryCount: 0, CreatedAt: time.Now()})

	n, _ := s.Count(ctx, Filter{})
	if n != 1 {
		t.Fatalf("dedup on: want 1 row, got %d", n)
	}
	list, _ := s.List(ctx, Filter{})
	got := list[0]
	if got.RetryCount != 4 {
		t.Fatalf("retry_count must increment: want 4, got %d", got.RetryCount)
	}
	if got.Error != "err2" {
		t.Fatalf("error must be latest: want err2, got %q", got.Error)
	}
	if !got.CreatedAt.Equal(first) {
		t.Fatalf("created_at must be preserved as first-seen: want %v, got %v", first, got.CreatedAt)
	}
	if got.RetriedAt != nil {
		t.Fatal("retried_at must be reset to nil on upsert")
	}
}

func TestMemoryStore_DedupEmptyOriginalIDInsertsDistinct(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore(WithMemoryDedup())
	_ = s.Store(ctx, &Message{ID: "a", EventName: "e", OriginalID: "", CreatedAt: time.Now()})
	_ = s.Store(ctx, &Message{ID: "b", EventName: "e", OriginalID: "", CreatedAt: time.Now()})
	n, _ := s.Count(ctx, Filter{})
	if n != 2 {
		t.Fatalf("empty OriginalID must not dedup: want 2, got %d", n)
	}
}

func TestMemoryStore_DedupDeleteClearsIndex(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore(WithMemoryDedup())
	_ = s.Store(ctx, &Message{ID: "a", EventName: "e", OriginalID: "o1", CreatedAt: time.Now()})
	if err := s.Delete(ctx, "a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// After deleting, storing the same key must insert fresh (not resurrect/upsert a ghost).
	_ = s.Store(ctx, &Message{ID: "b", EventName: "e", OriginalID: "o1", RetryCount: 0, CreatedAt: time.Now()})
	list, _ := s.List(ctx, Filter{})
	if len(list) != 1 || list[0].ID != "b" {
		t.Fatalf("expected single fresh row b after delete+restore, got %+v", list)
	}
}
