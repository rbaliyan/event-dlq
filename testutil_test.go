package dlq

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// countResult is one queued (value, error) outcome for a recordingStore.Count call.
type countResult struct {
	val int64
	err error
}

// recordingStore is a configurable Store fake for exercising Manager branches
// that a real backend cannot easily be forced into (store-write failure, count
// failure, empty result sets).
//
// It deliberately does NOT implement Quarantiner or StatsProvider, so it also
// drives the "store does not support quarantine" replay path and the Stats
// Count-based fallback. Use quarantinerRecordingStore when a Quarantiner is
// needed.
type recordingStore struct {
	storeErr error

	getMsg *Message
	getErr error

	listMsgs []*Message
	listErr  error

	markErr error

	deleteErr        error
	deleteByFilterN  int64
	deleteByFilterEr error
	deleteOlderN     int64
	deleteOlderErr   error

	// countQueue supplies per-call results for Count, consumed in order.
	// Health and the Stats fallback both issue exactly two Count calls
	// (total, then ExcludeRetried), so a two-element queue covers them.
	countQueue []countResult
	countCalls int

	mu          sync.Mutex
	storedMsgs  []*Message
	markedIDs   []string
	deletedIDs  []string
}

func (s *recordingStore) Store(_ context.Context, msg *Message) error {
	if s.storeErr != nil {
		return s.storeErr
	}
	s.mu.Lock()
	s.storedMsgs = append(s.storedMsgs, msg)
	s.mu.Unlock()
	return nil
}

func (s *recordingStore) Get(_ context.Context, _ string) (*Message, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.getMsg, nil
}

func (s *recordingStore) GetByOriginalID(_ context.Context, _ string) (*Message, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	return s.getMsg, nil
}

func (s *recordingStore) List(_ context.Context, _ Filter) ([]*Message, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.listMsgs, nil
}

func (s *recordingStore) Count(_ context.Context, _ Filter) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.countCalls < len(s.countQueue) {
		r := s.countQueue[s.countCalls]
		s.countCalls++
		return r.val, r.err
	}
	return 0, nil
}

func (s *recordingStore) MarkRetried(_ context.Context, id string) error {
	if s.markErr != nil {
		return s.markErr
	}
	s.mu.Lock()
	s.markedIDs = append(s.markedIDs, id)
	s.mu.Unlock()
	return nil
}

func (s *recordingStore) Delete(_ context.Context, id string) error {
	if s.deleteErr != nil {
		return s.deleteErr
	}
	s.mu.Lock()
	s.deletedIDs = append(s.deletedIDs, id)
	s.mu.Unlock()
	return nil
}

func (s *recordingStore) DeleteOlderThan(_ context.Context, _ time.Duration) (int64, error) {
	return s.deleteOlderN, s.deleteOlderErr
}

func (s *recordingStore) DeleteByFilter(_ context.Context, _ Filter) (int64, error) {
	return s.deleteByFilterN, s.deleteByFilterEr
}

// quarantinerRecordingStore is a recordingStore that also implements Quarantiner,
// for asserting that the quarantine write error is surfaced (versus the
// not-a-Quarantiner branch covered by recordingStore itself).
type quarantinerRecordingStore struct {
	recordingStore
	quarantineErr error
	quarantinedID string
}

func (s *quarantinerRecordingStore) Quarantine(_ context.Context, id string) error {
	if s.quarantineErr != nil {
		return s.quarantineErr
	}
	s.quarantinedID = id
	return nil
}

// failingRepublisher always fails its Send, for exercising replay-failure and
// backoff paths.
type failingRepublisher struct {
	err   error
	calls int
}

func (f *failingRepublisher) Send(_ context.Context, _, _ string, _ []byte, _ map[string]string) error {
	f.calls++
	if f.err != nil {
		return f.err
	}
	return errors.New("republish failed")
}

// slowBackoff returns a delay long enough that an already-canceled context wins
// the backoff select deterministically (no timer race).
type slowBackoff struct{}

func (slowBackoff) NextDelay(int) time.Duration { return 30 * time.Second }
func (slowBackoff) Reset()                       {}

// capturingLogHandler is a slog.Handler that records every emitted record so
// tests can assert on log level and message (e.g. the warn-once path when a
// store cannot quarantine).
type capturingLogHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *capturingLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *capturingLogHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *capturingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingLogHandler) WithGroup(string) slog.Handler      { return h }

// countLevel returns how many captured records were emitted at the given level.
func (h *capturingLogHandler) countLevel(level slog.Level) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		if r.Level == level {
			n++
		}
	}
	return n
}

// newCapturingLogger returns a logger writing into the returned handler.
func newCapturingLogger() (*slog.Logger, *capturingLogHandler) {
	h := &capturingLogHandler{}
	return slog.New(h), h
}
