package dlq

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClampListLimit(t *testing.T) {
	cases := []struct {
		name     string
		limit    int
		max      int
		wantLim  int
	}{
		{"unbounded max leaves limit", 5, 0, 5},
		{"unbounded max leaves zero", 0, 0, 0},
		{"zero limit clamps to max", 0, 10, 10},
		{"limit above max clamps", 25, 10, 10},
		{"limit below max preserved", 3, 10, 3},
		{"negative limit clamps to max", -1, 10, 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := clampListLimit(Filter{Limit: tc.limit}, tc.max)
			assert.Equal(t, tc.wantLim, got.Limit)
		})
	}
}

// TestMemoryMaxListLimit verifies the opt-in cap clamps List while Count stays
// accurate. MemoryStore exercises the shared clampListLimit helper used by all
// backends.
func TestMemoryMaxListLimit(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore(WithMemoryMaxListLimit(3))
	for i := 0; i < 10; i++ {
		require.NoError(t, store.Store(ctx, &Message{
			ID: "m-" + strconv.Itoa(i), EventName: "e", OriginalID: "o-" + strconv.Itoa(i),
			Payload: []byte(`{}`), CreatedAt: time.Now(),
		}))
	}

	got, err := store.List(ctx, Filter{})
	require.NoError(t, err)
	assert.Len(t, got, 3, "List with no Limit clamps to the cap")

	got, err = store.List(ctx, Filter{Limit: 100})
	require.NoError(t, err)
	assert.Len(t, got, 3, "Limit above the cap clamps")

	got, err = store.List(ctx, Filter{Limit: 2})
	require.NoError(t, err)
	assert.Len(t, got, 2, "Limit below the cap is honored")

	count, err := store.Count(ctx, Filter{})
	require.NoError(t, err)
	assert.Equal(t, int64(10), count, "Count is not capped")
}
