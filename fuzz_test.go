package dlq

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// FuzzMatchesFilter checks invariants of the predicate rather than only that it
// does not panic: it is deterministic, the empty filter matches everything, and
// a positive match implies every set scalar field actually agrees.
func FuzzMatchesFilter(f *testing.F) {
	f.Add("orders.created", "connection refused", "order-service", 3, true, false)
	f.Add("", "", "", 0, false, false)
	f.Add("payments.failed", "timeout: deadline exceeded", "payment-svc", 10, false, true)
	f.Add("users.updated", "duplicate key", "", -1, true, true)

	f.Fuzz(func(t *testing.T, eventName, errMsg, source string, retryCount int, excludeRetried, retried bool) {
		store := NewMemoryStore()
		msg := &Message{
			ID:         "fuzz-msg-1",
			EventName:  eventName,
			OriginalID: "orig-1",
			Payload:    []byte(`{"test":true}`),
			Error:      errMsg,
			RetryCount: retryCount,
			CreatedAt:  time.Now(),
			Source:     source,
		}
		if retried {
			now := time.Now()
			msg.RetriedAt = &now
		}

		// The empty filter must match any message.
		if !store.matchesFilter(msg, Filter{}) {
			t.Fatal("empty filter must match every message")
		}

		filter := Filter{
			EventName:      eventName,
			Error:          errMsg,
			Source:         source,
			MaxRetries:     retryCount,
			ExcludeRetried: excludeRetried,
		}

		got := store.matchesFilter(msg, filter)
		// Determinism: same inputs, same answer.
		if got != store.matchesFilter(msg, filter) {
			t.Fatal("matchesFilter is not deterministic")
		}

		// Consistency: a positive match implies every set scalar field agrees.
		if got {
			if filter.EventName != "" && msg.EventName != filter.EventName {
				t.Errorf("matched but EventName %q != filter %q", msg.EventName, filter.EventName)
			}
			if filter.Source != "" && msg.Source != filter.Source {
				t.Errorf("matched but Source %q != filter %q", msg.Source, filter.Source)
			}
			if filter.ExcludeRetried && msg.RetriedAt != nil {
				t.Error("matched with ExcludeRetried but message is retried")
			}
			if filter.MaxRetries > 0 && msg.RetryCount > filter.MaxRetries {
				t.Errorf("matched but RetryCount %d > MaxRetries %d", msg.RetryCount, filter.MaxRetries)
			}
		}
	})
}

// FuzzNormalizeErrorType asserts the output is non-empty and always valid UTF-8.
// The é seed at byte boundary 50 guards the byte-slice truncation that previously
// could split a multi-byte rune.
func FuzzNormalizeErrorType(f *testing.F) {
	f.Add("connection refused: dial tcp 10.0.0.1:5432")
	f.Add("timeout")
	f.Add("")
	f.Add("duplicate key error: E11000")
	f.Add(": leading colon")
	f.Add("a]very[long^error*message+that/goes-on_and_on!for@more#than$fifty%characters&to(test)truncation")
	// 49 ASCII bytes then a 2-byte rune: a naive errMsg[:50] would split the rune.
	f.Add(strings.Repeat("a", 49) + "é")
	// Multi-byte runes packed past the 50-byte budget.
	f.Add(strings.Repeat("é", 40))

	f.Fuzz(func(t *testing.T, errMsg string) {
		result := normalizeErrorType(errMsg)
		if result == "" {
			t.Error("normalizeErrorType should never return empty string")
		}
		// The function must never turn a valid UTF-8 error string into invalid
		// UTF-8 (the byte-truncation rune-split bug). Inputs that are already
		// invalid UTF-8 may pass through unchanged.
		if utf8.ValidString(errMsg) && !utf8.ValidString(result) {
			t.Errorf("normalizeErrorType corrupted valid input %q into invalid UTF-8 %q", errMsg, result)
		}
	})
}

// FuzzStoreAndFilterMessages stores several messages and asserts query
// invariants: List honors the Limit bound, every returned message satisfies the
// predicate, and List never returns more than Count for the same predicate.
func FuzzStoreAndFilterMessages(f *testing.F) {
	f.Add("evt1", "err1", "src1", 1, 10, 0)
	f.Add("", "", "", 0, 0, 0)
	f.Add("orders", "timeout", "api", 5, 100, 50)

	f.Fuzz(func(t *testing.T, eventName, errMsg, source string, retryCount, limit, offset int) {
		if limit < 0 || limit > 1000 {
			limit = 10
		}
		if offset < 0 || offset > 1000 {
			offset = 0
		}

		store := NewMemoryStore()
		ctx := context.Background()

		// Store a handful of messages so pagination and Count are meaningful.
		const seeded = 5
		for i := range seeded {
			_ = store.Store(ctx, &Message{
				ID:         "fuzz-" + strconv.Itoa(i),
				EventName:  eventName,
				OriginalID: "orig-" + strconv.Itoa(i),
				Payload:    []byte(`{}`),
				Error:      errMsg,
				RetryCount: retryCount,
				CreatedAt:  time.Now(),
				Source:     source,
			})
		}

		predicate := Filter{EventName: eventName, Error: errMsg, Source: source}
		paged := predicate
		paged.Limit = limit
		paged.Offset = offset

		list, err := store.List(ctx, paged)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		count, err := store.Count(ctx, predicate)
		if err != nil {
			t.Fatalf("Count: %v", err)
		}

		if limit > 0 && len(list) > limit {
			t.Errorf("List returned %d > limit %d", len(list), limit)
		}
		if int64(len(list)) > count {
			t.Errorf("List returned %d > Count %d for the same predicate", len(list), count)
		}
		for _, m := range list {
			if !store.matchesFilter(m, predicate) {
				t.Errorf("List returned message %s that does not match the predicate", m.ID)
			}
		}
	})
}

// FuzzReplayCount fuzzes the metadata value parsed by replayCount (strconv.Atoi
// of the dlq_replay_count key). The count must never be negative and must match
// the parsed integer for valid non-negative inputs.
func FuzzReplayCount(f *testing.F) {
	f.Add("0")
	f.Add("5")
	f.Add("-1")
	f.Add("")
	f.Add("not-a-number")
	f.Add("99999999999999999999999999")
	f.Add("  3  ")
	f.Add("3.5")

	f.Fuzz(func(t *testing.T, raw string) {
		msg := &Message{Metadata: map[string]string{MetadataReplayCount: raw}}
		got := replayCount(msg)
		if got < 0 {
			t.Errorf("replayCount returned negative %d for %q", got, raw)
		}
		// A clean non-negative integer string must round-trip exactly.
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 && got != n {
			t.Errorf("replayCount(%q) = %d, want %d", raw, got, n)
		}
	})
}

// FuzzTerminalErrorMatching checks the predicate against a reference
// implementation: it must return true iff the error is non-empty and contains
// one of the patterns (case-sensitive).
func FuzzTerminalErrorMatching(f *testing.F) {
	f.Add("proto: cannot parse", "proto:", "unknown field")
	f.Add("", "x", "y")
	f.Add("SCHEMA_INCOMPATIBLE: v2", "INCOMPATIBLE", "")
	f.Add("timeout", "", "")

	f.Fuzz(func(t *testing.T, errMsg, p1, p2 string) {
		pred := TerminalErrorMatching(p1, p2)
		msg := &Message{Error: errMsg}
		got := pred(msg)

		want := false
		if errMsg != "" {
			for _, p := range []string{p1, p2} {
				if p != "" && strings.Contains(errMsg, p) {
					want = true
					break
				}
			}
		}
		if got != want {
			t.Errorf("TerminalErrorMatching(%q,%q)(%q) = %v, want %v", p1, p2, errMsg, got, want)
		}
		// Empty error must never be terminal.
		if errMsg == "" && got {
			t.Error("empty error must never match")
		}
	})
}

// FuzzMongoDecode fuzzes the BSON document decoder (decodeMongoDoc) — the
// untrusted-input path the Mongo store uses for every Get/List result. It must
// never panic on arbitrary bytes; malformed BSON is rejected with an error, and
// a successful decode yields a non-nil message.
func FuzzMongoDecode(f *testing.F) {
	// Seed with valid BSON so the fuzzer mutates real document structure.
	seeds := []mongoMessage{
		{ID: "d1", EventName: "orders.created", OriginalID: "o1", Payload: []byte("payload"), Error: "boom", RetryCount: 2, CreatedAt: time.Unix(1700000000, 0)},
		{ID: "", EventName: "", Metadata: map[string]string{"trace": "abc"}},
	}
	for _, mm := range seeds {
		if raw, err := bson.Marshal(mm); err == nil {
			f.Add(raw)
		}
	}
	f.Add([]byte{})
	f.Add([]byte("not a bson document"))

	f.Fuzz(func(t *testing.T, raw []byte) {
		msg, err := decodeMongoDoc(raw)
		if err != nil {
			return // malformed BSON rejected — acceptable
		}
		if msg == nil {
			t.Fatal("decodeMongoDoc returned nil message with nil error")
		}
		// toMessage is a pure field copy; the property under test is that no
		// crafted BSON makes the decode panic or yield a nil-with-nil-error.
	})
}

// FuzzMemoryDedupUpsert asserts the documented dedup upsert invariants: a second
// store of the same non-empty (EventName, OriginalID) collapses into the first
// row (retry_count incremented, error/payload/metadata updated to the latest,
// created_at preserved, retried_at cleared), while an empty OriginalID always
// inserts a distinct row.
func FuzzMemoryDedupUpsert(f *testing.F) {
	f.Add("orders.created", "o1", "err-a", 2, "err-b", 0)
	f.Add("", "", "x", 1, "y", 1)
	f.Add("e", "", "a", 0, "b", 5)

	f.Fuzz(func(t *testing.T, eventName, originalID, err1 string, rc1 int, err2 string, rc2 int) {
		ctx := context.Background()
		store := NewMemoryStore(WithMemoryDedup())
		created := time.Now().Add(-time.Hour).Truncate(time.Second)

		retried := created.Add(time.Minute)
		first := &Message{
			ID: "id-1", EventName: eventName, OriginalID: originalID,
			Payload: []byte("payload-1"), Error: err1, RetryCount: rc1,
			CreatedAt: created, RetriedAt: &retried,
		}
		if err := store.Store(ctx, first); err != nil {
			t.Fatalf("store first: %v", err)
		}
		second := &Message{
			ID: "id-2", EventName: eventName, OriginalID: originalID,
			Payload: []byte("payload-2"), Error: err2, RetryCount: rc2,
			CreatedAt: time.Now(),
		}
		if err := store.Store(ctx, second); err != nil {
			t.Fatalf("store second: %v", err)
		}

		count, err := store.Count(ctx, Filter{})
		if err != nil {
			t.Fatalf("count: %v", err)
		}

		if originalID == "" {
			if count != 2 {
				t.Errorf("empty OriginalID must insert distinct rows, got %d", count)
			}
			return
		}

		if count != 1 {
			t.Fatalf("dedup must collapse to 1 row, got %d", count)
		}
		got, err := store.Get(ctx, "id-1") // the first row survives
		if err != nil {
			t.Fatalf("survivor (id-1) not found: %v", err)
		}
		if got.RetryCount != rc1+1 {
			t.Errorf("retry_count: got %d want %d (existing+1)", got.RetryCount, rc1+1)
		}
		if got.Error != err2 {
			t.Errorf("error must be latest: got %q want %q", got.Error, err2)
		}
		if string(got.Payload) != "payload-2" {
			t.Errorf("payload must be latest: got %q", got.Payload)
		}
		if !got.CreatedAt.Equal(created) {
			t.Errorf("created_at must be preserved: got %v want %v", got.CreatedAt, created)
		}
		if got.RetriedAt != nil {
			t.Error("retried_at must be cleared on re-store")
		}
	})
}

// FuzzRedisParseMessage fuzzes the Redis hash decoder (json.Unmarshal of the
// metadata field, strconv parsing of retry_count and the timestamps). It must
// never panic, and on a successful parse the directly-mapped fields must be
// preserved and the numeric fields must equal their parsed values.
func FuzzRedisParseMessage(f *testing.F) {
	// parseMessage only reads the supplied fields map (no client access), so a
	// zero-value store is sufficient and keeps the target dependency-free for
	// the ClusterFuzzLite build.
	store := &RedisStore{}

	f.Add("d1", "orders.created", "o1", `{"order":1}`, `{"trace":"abc"}`, "3", "1700000000", "", "")
	f.Add("", "", "", "", "", "", "", "", "")
	f.Add("d2", "e", "o2", "raw", "not json", "abc", "xyz", "-1", "99999999999999999999")

	f.Fuzz(func(t *testing.T, id, eventName, originalID, payload, metadata, retryCount, createdAt, retriedAt, quarantinedAt string) {
		fields := map[string]string{
			"id":             id,
			"event_name":     eventName,
			"original_id":    originalID,
			"payload":        payload,
			"metadata":       metadata,
			"retry_count":    retryCount,
			"created_at":     createdAt,
			"retried_at":     retriedAt,
			"quarantined_at": quarantinedAt,
		}

		// Undecodable inputs must be rejected with an error, not silently
		// accepted with zeroed fields. (json.Valid is a lower bound: valid JSON
		// that isn't a string map still fails Unmarshal, which is also fine.)
		shouldFail := false
		if retryCount != "" {
			if _, e := strconv.Atoi(retryCount); e != nil {
				shouldFail = true
			}
		}
		for _, ts := range []string{createdAt, retriedAt, quarantinedAt} {
			if ts != "" {
				if _, e := strconv.ParseInt(ts, 10, 64); e != nil {
					shouldFail = true
				}
			}
		}
		if metadata != "" && !json.Valid([]byte(metadata)) {
			shouldFail = true
		}

		msg, err := store.parseMessage(fields)
		if shouldFail && err == nil {
			t.Errorf("parseMessage accepted undecodable input (should reject): %+v", fields)
		}
		if err != nil {
			return // malformed input rejected with an error — acceptable
		}

		// On success, the directly-mapped fields must be preserved verbatim.
		if msg.ID != id || msg.EventName != eventName || msg.OriginalID != originalID {
			t.Errorf("identity fields not preserved: %+v", msg)
		}
		if string(msg.Payload) != payload {
			t.Errorf("payload not preserved: got %q want %q", msg.Payload, payload)
		}
		// retry_count must equal its parsed value (empty defaults to 0).
		if retryCount != "" {
			if n, perr := strconv.Atoi(retryCount); perr == nil && msg.RetryCount != n {
				t.Errorf("retry_count: got %d want %d", msg.RetryCount, n)
			}
		} else if msg.RetryCount != 0 {
			t.Errorf("empty retry_count should yield 0, got %d", msg.RetryCount)
		}

		// created_at is a value field: a parsed timestamp round-trips by Unix
		// seconds; empty leaves the zero time.
		if createdAt != "" {
			if unix, perr := strconv.ParseInt(createdAt, 10, 64); perr == nil && msg.CreatedAt.Unix() != unix {
				t.Errorf("created_at: got %d want %d", msg.CreatedAt.Unix(), unix)
			}
		}
		// retried_at / quarantined_at are optional pointers: present and parsed
		// iff the field was a valid integer, else nil.
		assertOptionalTime(t, "retried_at", retriedAt, msg.RetriedAt)
		assertOptionalTime(t, "quarantined_at", quarantinedAt, msg.QuarantinedAt)
	})
}

// assertOptionalTime checks the pointer-time round-trip: when the raw field
// parses as an int64 the pointer must be set to that Unix time; an empty field
// must leave it nil.
func assertOptionalTime(t *testing.T, field, raw string, got *time.Time) {
	t.Helper()
	if raw == "" {
		if got != nil {
			t.Errorf("%s: empty field must yield nil, got %v", field, got)
		}
		return
	}
	if unix, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if got == nil {
			t.Errorf("%s: parsed %q but pointer is nil", field, raw)
		} else if got.Unix() != unix {
			t.Errorf("%s: got %d want %d", field, got.Unix(), unix)
		}
	}
}
