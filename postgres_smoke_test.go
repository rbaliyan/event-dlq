package dlq

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSQLMockStore returns a PostgresStore backed by a sqlmock DB so the SQL
// scan/mapping code can be smoke-tested hermetically (no Docker/Postgres).
func newSQLMockStore(t *testing.T) (*PostgresStore, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	store, err := NewPostgresStore(db)
	require.NoError(t, err)
	return store, mock
}

// TestSmoke_PostgresStore_GetScanMapping verifies, without a real database, that
// PostgresStore.Get maps every column onto Message correctly: NULL retried_at /
// quarantined_at become nil pointers, NULL source becomes "", and the JSONB
// metadata column is unmarshalled.
func TestSmoke_PostgresStore_GetScanMapping(t *testing.T) {
	ctx := context.Background()
	created := time.Now().Truncate(time.Second)

	t.Run("FoundWithNulls", func(t *testing.T) {
		store, mock := newSQLMockStore(t)
		rows := sqlmock.NewRows([]string{
			"id", "event_name", "original_id", "payload", "metadata",
			"error", "retry_count", "source", "created_at", "retried_at", "quarantined_at",
		}).AddRow(
			"c-1", "order.created", "orig-1", []byte(`{"id":1}`), []byte(`{"trace":"abc"}`),
			"boom", 2, nil, created, nil, nil,
		)
		mock.ExpectQuery("SELECT (.+) FROM (.+) WHERE id").WithArgs("c-1").WillReturnRows(rows)

		got, err := store.Get(ctx, "c-1")
		require.NoError(t, err)
		assert.Equal(t, "c-1", got.ID)
		assert.Equal(t, "order.created", got.EventName)
		assert.Equal(t, "orig-1", got.OriginalID)
		assert.Equal(t, "abc", got.Metadata["trace"])
		assert.Equal(t, 2, got.RetryCount)
		assert.Equal(t, "", got.Source, "NULL source maps to empty string")
		assert.Nil(t, got.RetriedAt, "NULL retried_at maps to nil")
		assert.Nil(t, got.QuarantinedAt, "NULL quarantined_at maps to nil")
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("FoundWithTimestampsSet", func(t *testing.T) {
		store, mock := newSQLMockStore(t)
		retried := created.Add(time.Minute)
		quarantined := created.Add(2 * time.Minute)
		rows := sqlmock.NewRows([]string{
			"id", "event_name", "original_id", "payload", "metadata",
			"error", "retry_count", "source", "created_at", "retried_at", "quarantined_at",
		}).AddRow(
			"c-2", "order.updated", "orig-2", []byte(`{}`), []byte(`{}`),
			"e", 5, "svc-a", created, retried, quarantined,
		)
		mock.ExpectQuery("SELECT (.+) FROM (.+) WHERE id").WithArgs("c-2").WillReturnRows(rows)

		got, err := store.Get(ctx, "c-2")
		require.NoError(t, err)
		assert.Equal(t, "svc-a", got.Source)
		require.NotNil(t, got.RetriedAt)
		assert.Equal(t, retried.Unix(), got.RetriedAt.Unix())
		require.NotNil(t, got.QuarantinedAt)
		assert.Equal(t, quarantined.Unix(), got.QuarantinedAt.Unix())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("NotFound", func(t *testing.T) {
		store, mock := newSQLMockStore(t)
		mock.ExpectQuery("SELECT (.+) FROM (.+) WHERE id").WithArgs("missing").
			WillReturnError(sqlmock.ErrCancelled) // any driver error
		_, err := store.Get(ctx, "missing")
		require.Error(t, err)
	})

	t.Run("NoRowsMapsToErrNotFound", func(t *testing.T) {
		store, mock := newSQLMockStore(t)
		empty := sqlmock.NewRows([]string{
			"id", "event_name", "original_id", "payload", "metadata",
			"error", "retry_count", "source", "created_at", "retried_at", "quarantined_at",
		})
		mock.ExpectQuery("SELECT (.+) FROM (.+) WHERE id").WithArgs("gone").WillReturnRows(empty)
		_, err := store.Get(ctx, "gone")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrNotFound)
	})
}

// TestSmoke_PostgresStore_Count exercises the Count query path hermetically.
func TestSmoke_PostgresStore_Count(t *testing.T) {
	ctx := context.Background()
	store, mock := newSQLMockStore(t)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(
		sqlmock.NewRows([]string{"count"}).AddRow(int64(7)),
	)

	n, err := store.Count(ctx, Filter{})
	require.NoError(t, err)
	assert.Equal(t, int64(7), n)
	require.NoError(t, mock.ExpectationsWereMet())
}
