package dlq

import (
	"context"
	"testing"
)

// capturingRepublisher records the metadata of each Send so tests can assert the
// replay-count is incremented on the republished message.
type capturingRepublisher struct {
	sends []map[string]string
}

func (c *capturingRepublisher) Send(ctx context.Context, eventName, eventID string, payload []byte, md map[string]string) error {
	c.sends = append(c.sends, md)
	return nil
}

func TestReplay_MaxReplayAttempts_QuarantinesAtCap(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	// A message already replayed 3 times (metadata count = 3); cap is 3.
	_ = store.Store(ctx, &Message{
		ID: "m1", EventName: "e", OriginalID: "o1", Error: "anything",
		Metadata: map[string]string{MetadataReplayCount: "3"},
	})

	rep := &capturingRepublisher{}
	mgr, err := NewManager(store, rep, WithMaxReplayAttempts(3))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	replayed, err := mgr.Replay(ctx, Filter{})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed != 0 {
		t.Fatalf("want 0 replayed at cap, got %d", replayed)
	}
	if len(rep.sends) != 0 {
		t.Fatalf("message at cap must not be republished; got %d sends", len(rep.sends))
	}
	got, _ := store.Get(ctx, "m1")
	if got.QuarantinedAt == nil {
		t.Fatal("message at cap must be quarantined")
	}
}

func TestReplay_MaxReplayAttempts_IncrementsAndReplaysBelowCap(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	// No prior replay count (fresh) -> count 0, below cap of 3.
	_ = store.Store(ctx, &Message{ID: "m1", EventName: "e", OriginalID: "o1", Error: "transient"})

	rep := &capturingRepublisher{}
	mgr, err := NewManager(store, rep, WithMaxReplayAttempts(3))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	replayed, err := mgr.Replay(ctx, Filter{})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed != 1 {
		t.Fatalf("want 1 replayed below cap, got %d", replayed)
	}
	if len(rep.sends) != 1 {
		t.Fatalf("want 1 republish, got %d", len(rep.sends))
	}
	// The republished message must carry an incremented replay count so the next
	// re-DLQ'd row inherits count=1.
	if got := rep.sends[0][MetadataReplayCount]; got != "1" {
		t.Fatalf("republished %s = %q, want \"1\"", MetadataReplayCount, got)
	}
}

func TestReplay_MaxReplayAttempts_UnlimitedByDefault(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	_ = store.Store(ctx, &Message{
		ID: "m1", EventName: "e", OriginalID: "o1",
		Metadata: map[string]string{MetadataReplayCount: "9999"},
	})
	rep := &capturingRepublisher{}
	mgr, _ := NewManager(store, rep) // no WithMaxReplayAttempts -> unlimited

	replayed, err := mgr.Replay(ctx, Filter{})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed != 1 || len(rep.sends) != 1 {
		t.Fatalf("default must replay regardless of count; replayed=%d sends=%d", replayed, len(rep.sends))
	}
}

func TestReplaySingle_MaxReplayAttempts_QuarantinesAtCap(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	_ = store.Store(ctx, &Message{
		ID: "m1", EventName: "e", OriginalID: "o1",
		Metadata: map[string]string{MetadataReplayCount: "5"},
	})
	rep := &capturingRepublisher{}
	mgr, _ := NewManager(store, rep, WithMaxReplayAttempts(5))

	if err := mgr.ReplaySingle(ctx, "m1"); err != nil {
		t.Fatalf("ReplaySingle: %v", err)
	}
	if len(rep.sends) != 0 {
		t.Fatalf("at cap must not republish; got %d sends", len(rep.sends))
	}
	got, _ := store.Get(ctx, "m1")
	if got.QuarantinedAt == nil {
		t.Fatal("at cap must be quarantined")
	}
}

func TestReplayCountHelper(t *testing.T) {
	cases := map[string]int{"": 0, "0": 0, "3": 3, "-1": 0, "x": 0}
	for in, want := range cases {
		msg := &Message{Metadata: map[string]string{MetadataReplayCount: in}}
		if got := replayCount(msg); got != want {
			t.Errorf("replayCount(%q) = %d, want %d", in, got, want)
		}
	}
	if got := replayCount(&Message{}); got != 0 {
		t.Errorf("replayCount(nil metadata) = %d, want 0", got)
	}
}
