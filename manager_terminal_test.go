package dlq

import (
	"context"
	"testing"
)

// countingRepublisher records how many times Send is called.
type countingRepublisher struct{ sends int }

func (c *countingRepublisher) Send(ctx context.Context, eventName, eventID string, payload []byte, md map[string]string) error {
	c.sends++
	return nil
}

func TestReplay_QuarantinesTerminalAndDoesNotRepublish(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	_ = store.Store(ctx, &Message{
		ID: "poison", EventName: "call.updated.GLOBAL", OriginalID: "orig-1",
		Error: "error decoding key end_reason: cannot decode string into a call.EndReason",
	})

	rep := &countingRepublisher{}
	mgr, err := NewManager(store, rep, WithTerminalError(TerminalErrorMatching("cannot decode")))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	replayed, err := mgr.Replay(ctx, Filter{})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed != 0 {
		t.Fatalf("expected 0 replayed, got %d", replayed)
	}
	if rep.sends != 0 {
		t.Fatalf("terminal message must not be republished; Send called %d times", rep.sends)
	}

	got, _ := store.Get(ctx, "poison")
	if got.QuarantinedAt == nil {
		t.Fatal("expected terminal message to be quarantined")
	}

	// A second sweep must skip it entirely (ExcludeQuarantined) — still no sends.
	if _, err := mgr.Replay(ctx, Filter{}); err != nil {
		t.Fatalf("replay 2: %v", err)
	}
	if rep.sends != 0 {
		t.Fatalf("quarantined message re-evaluated; Send called %d times", rep.sends)
	}
}

func TestReplaySingle_QuarantinesTerminal(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	_ = store.Store(ctx, &Message{
		ID: "poison", EventName: "call.updated.GLOBAL", OriginalID: "orig-1",
		Error: "cannot decode string into a call.EndReason",
	})
	rep := &countingRepublisher{}
	mgr, err := NewManager(store, rep, WithTerminalError(TerminalErrorMatching("cannot decode")))
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}

	if err := mgr.ReplaySingle(ctx, "poison"); err != nil {
		t.Fatalf("ReplaySingle: unexpected error: %v", err)
	}
	if rep.sends != 0 {
		t.Fatalf("terminal message must not be republished; Send called %d times", rep.sends)
	}
	got, _ := store.Get(ctx, "poison")
	if got.QuarantinedAt == nil {
		t.Fatal("expected terminal message to be quarantined")
	}
}

func TestTerminalErrorMatching(t *testing.T) {
	pred := TerminalErrorMatching("cannot decode", "unmarshal")

	if !pred(&Message{Error: "error decoding key end_reason: cannot decode string into a call.EndReason"}) {
		t.Fatal("expected match on 'cannot decode'")
	}
	if pred(&Message{Error: "connection refused"}) {
		t.Fatal("did not expect match on unrelated error")
	}
	if pred(&Message{Error: ""}) {
		t.Fatal("empty error must not match")
	}
}
