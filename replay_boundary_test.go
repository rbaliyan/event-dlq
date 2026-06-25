package dlq

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReplayCap_Boundary pins the exact >= boundary of the error-agnostic replay
// cap, including the cap=0 "unlimited" sentinel. A message is quarantined iff
// maxReplayAttempts > 0 and its replay count has reached the cap.
func TestReplayCap_Boundary(t *testing.T) {
	cases := []struct {
		name           string
		replayCount    int
		cap            int
		wantQuarantine bool
		wantReplayed   int
	}{
		{name: "UnlimitedNeverQuarantines", replayCount: 5, cap: 0, wantQuarantine: false, wantReplayed: 1},
		{name: "BelowCapReplays", replayCount: 0, cap: 1, wantQuarantine: false, wantReplayed: 1},
		{name: "AtCapQuarantines", replayCount: 1, cap: 1, wantQuarantine: true, wantReplayed: 0},
		{name: "JustBelowCapReplays", replayCount: 2, cap: 3, wantQuarantine: false, wantReplayed: 1},
		{name: "ReachesCapQuarantines", replayCount: 3, cap: 3, wantQuarantine: true, wantReplayed: 0},
		{name: "OverCapQuarantines", replayCount: 9, cap: 3, wantQuarantine: true, wantReplayed: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := NewMemoryStore()
			require.NoError(t, store.Store(ctx, &Message{
				ID: "m1", EventName: "e", OriginalID: "o1", Payload: []byte(`{}`),
				Metadata:  map[string]string{MetadataReplayCount: strconv.Itoa(tc.replayCount)},
				CreatedAt: time.Now(),
			}))

			rep := &countingRepublisher{}
			mgr, err := NewManager(store, rep, WithMaxReplayAttempts(tc.cap))
			require.NoError(t, err)

			replayed, err := mgr.Replay(ctx, Filter{})
			require.NoError(t, err)
			assert.Equal(t, tc.wantReplayed, replayed)
			assert.Equal(t, tc.wantReplayed, rep.sends)

			got, err := store.Get(ctx, "m1")
			require.NoError(t, err)
			if tc.wantQuarantine {
				assert.NotNil(t, got.QuarantinedAt, "expected quarantine at count=%d cap=%d", tc.replayCount, tc.cap)
				assert.Nil(t, got.RetriedAt)
			} else {
				assert.Nil(t, got.QuarantinedAt, "must not quarantine at count=%d cap=%d", tc.replayCount, tc.cap)
				assert.NotNil(t, got.RetriedAt, "replayed message should be marked retried")
			}
		})
	}
}
