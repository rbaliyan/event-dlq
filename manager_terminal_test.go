package dlq

import "testing"

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
