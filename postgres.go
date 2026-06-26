package dlq

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	"github.com/lib/pq"
	"github.com/rbaliyan/event/v3/health"
	"github.com/rbaliyan/event/v3/store/base"
)

// validIdentifier matches safe SQL/collection identifiers (alphanumeric and underscores).
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

/*
PostgreSQL Schema:

CREATE TABLE event_dlq (
    id          VARCHAR(36) PRIMARY KEY,
    event_name  VARCHAR(255) NOT NULL,
    original_id VARCHAR(36) NOT NULL,
    payload     BYTEA NOT NULL,
    metadata    JSONB,
    error       TEXT NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    source      VARCHAR(255),
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    retried_at  TIMESTAMP
);

CREATE INDEX idx_dlq_event_name ON event_dlq(event_name);
CREATE INDEX idx_dlq_created_at ON event_dlq(created_at);
CREATE INDEX idx_dlq_retried_at ON event_dlq(retried_at) WHERE retried_at IS NULL;
*/

// PostgresStoreOption configures a PostgresStore.
type PostgresStoreOption func(*postgresStoreOptions)

type postgresStoreOptions struct {
	table        string
	dedup        bool
	maxListLimit int
}

// WithTable sets a custom table name.
// The name must contain only alphanumeric characters and underscores.
func WithTable(table string) PostgresStoreOption {
	return func(o *postgresStoreOptions) {
		if table != "" {
			o.table = table
		}
	}
}

// WithPostgresDedup enables deduplication mode for the PostgresStore.
// When enabled, EnsureTable will create a unique index on (event_name, original_id),
// preventing duplicate DLQ entries for the same original message. Only enable this
// after ensuring no duplicate (event_name, original_id) rows exist (see MigrateDedup).
// When disabled (the default), multiple failures of the same original message each
// get their own row, which is the correct behavior for plain INSERT semantics.
func WithPostgresDedup() PostgresStoreOption {
	return func(o *postgresStoreOptions) {
		o.dedup = true
	}
}

// WithPostgresMaxListLimit caps the number of messages a single List returns
// (default 0 = unbounded). A List with no Limit, or a Limit above the cap, is
// clamped to the cap; Count is unaffected.
func WithPostgresMaxListLimit(n int) PostgresStoreOption {
	return func(o *postgresStoreOptions) {
		if n > 0 {
			o.maxListLimit = n
		}
	}
}

// PostgresStore is a PostgreSQL-based DLQ store
type PostgresStore struct {
	db           *sql.DB
	table        string
	dedup        bool
	maxListLimit int
}

// NewPostgresStore creates a new PostgreSQL DLQ store.
// Returns an error if db is nil or table name is invalid.
func NewPostgresStore(db *sql.DB, opts ...PostgresStoreOption) (*PostgresStore, error) {
	if db == nil {
		return nil, fmt.Errorf("dlq: db must not be nil")
	}

	o := &postgresStoreOptions{
		table: "event_dlq",
	}
	for _, opt := range opts {
		opt(o)
	}

	if !validIdentifier.MatchString(o.table) {
		return nil, fmt.Errorf("dlq: invalid table name %q", o.table)
	}

	return &PostgresStore{
		db:           db,
		table:        o.table,
		dedup:        o.dedup,
		maxListLimit: o.maxListLimit,
	}, nil
}

// EnsureTable creates the DLQ table and indexes if they don't exist.
// This is safe to call multiple times (uses IF NOT EXISTS).
// When the store was constructed with WithPostgresDedup, EnsureTable also
// attempts to create a unique index on (event_name, original_id). That step
// is failure-tolerant: if legacy duplicate rows block the index, a warning is
// logged and EnsureTable still succeeds. Run MigrateDedup to remove duplicates
// before re-calling EnsureTable so the unique index is created.
// #nosec G201 -- table name is set at construction, not user input
func (s *PostgresStore) EnsureTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id             VARCHAR(36) PRIMARY KEY,
			event_name     VARCHAR(255) NOT NULL,
			original_id    TEXT NOT NULL,
			payload        BYTEA NOT NULL,
			metadata       JSONB,
			error          TEXT NOT NULL,
			retry_count    INT NOT NULL DEFAULT 0,
			source         VARCHAR(255),
			created_at     TIMESTAMP NOT NULL DEFAULT NOW(),
			retried_at     TIMESTAMP,
			quarantined_at TIMESTAMP
		)
	`, s.table)

	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	// Guard the original_id widening: only ALTER when the column is not already TEXT.
	// Running ALTER COLUMN TYPE unconditionally acquires an ACCESS EXCLUSIVE lock on
	// every call even when the type is already correct; checking first avoids the lock
	// on steady-state deployments.
	var dataType string
	_ = s.db.QueryRowContext(ctx,
		"SELECT data_type FROM information_schema.columns WHERE table_name=$1 AND column_name='original_id'",
		s.table).Scan(&dataType)
	if dataType != "" && dataType != "text" {
		// #nosec G201 -- table name is set at construction, not user input
		if _, err := s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN original_id TYPE TEXT", s.table)); err != nil {
			return fmt.Errorf("alter original_id type: %w", err)
		}
	}

	// Add quarantined_at column if not present (IF NOT EXISTS is cheap and idempotent).
	// #nosec G201 -- table name is set at construction, not user input
	if _, err := s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS quarantined_at TIMESTAMP", s.table)); err != nil {
		return fmt.Errorf("alter table: %w", err)
	}

	// #nosec G201 -- table name is set at construction, not user input
	indexes := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_event_name ON %s(event_name)", s.table, s.table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_created_at ON %s(created_at)", s.table, s.table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_retried_at ON %s(retried_at) WHERE retried_at IS NULL", s.table, s.table),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_original_id ON %s(original_id)", s.table, s.table),
	}

	for _, idx := range indexes {
		if _, err := s.db.ExecContext(ctx, idx); err != nil {
			return fmt.Errorf("create index: %w", err)
		}
	}

	// Unique dedup index: only created when WithPostgresDedup is set.
	// Legacy duplicate (event_name, original_id) rows would block creation;
	// in that case we log a warning and continue rather than failing EnsureTable.
	// Run MigrateDedup first to remove duplicates, then re-call EnsureTable.
	if s.dedup {
		// #nosec G201 -- table name is set at construction, not user input
		uniqIdx := fmt.Sprintf(
			"CREATE UNIQUE INDEX IF NOT EXISTS uniq_%s_event_original ON %s(event_name, original_id)",
			s.table, s.table)
		if _, err := s.db.ExecContext(ctx, uniqIdx); err != nil {
			slog.Default().Warn("dlq: unique dedup index not created (legacy duplicates?); run MigrateDedup",
				"table", s.table, "error", err)
		}
	}

	return nil
}

// Store adds a message to the DLQ
func (s *PostgresStore) Store(ctx context.Context, msg *Message) error {
	if msg == nil {
		return fmt.Errorf("message is nil")
	}
	if msg.ID == "" {
		return fmt.Errorf("message ID is required")
	}

	metadata, err := base.MarshalMetadata(msg.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	if s.dedup && msg.OriginalID != "" {
		return s.storeDedup(ctx, msg, metadata)
	}

	// #nosec G201 -- table name is set at construction, not user input
	query := fmt.Sprintf(`
		INSERT INTO %s (id, event_name, original_id, payload, metadata, error, retry_count, source, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
	`, s.table)

	_, err = s.db.ExecContext(ctx, query,
		msg.ID,
		msg.EventName,
		msg.OriginalID,
		msg.Payload,
		metadata,
		msg.Error,
		msg.RetryCount,
		msg.Source,
		msg.CreatedAt.UTC(),
	)

	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	return nil
}

// storeDedup upserts on (event_name, original_id): increments retry_count,
// updates error/payload/metadata to the latest, preserves created_at (first-seen),
// and clears retried_at. Prefers the native ON CONFLICT path; if the unique
// constraint is not present yet (upgraded-but-unmigrated table), it falls back to
// a transactional SELECT ... FOR UPDATE upsert so writes never fail.
// #nosec G201 -- table name is set at construction, not user input
func (s *PostgresStore) storeDedup(ctx context.Context, msg *Message, metadata []byte) error {
	upsert := fmt.Sprintf(`
		INSERT INTO %s (id, event_name, original_id, payload, metadata, error, retry_count, source, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (event_name, original_id) DO UPDATE SET
			retry_count = %s.retry_count + 1,
			error       = EXCLUDED.error,
			payload     = EXCLUDED.payload,
			metadata    = EXCLUDED.metadata,
			retried_at  = NULL
	`, s.table, s.table)

	_, err := s.db.ExecContext(ctx, upsert,
		msg.ID, msg.EventName, msg.OriginalID, msg.Payload, metadata,
		msg.Error, msg.RetryCount, msg.Source, msg.CreatedAt.UTC())
	if err == nil {
		return nil
	}
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && string(pqErr.Code) == "42P10" {
		return s.storeDedupFallback(ctx, msg, metadata)
	}
	return fmt.Errorf("upsert: %w", err)
}

// storeDedupFallback performs dedup when no unique constraint exists yet, using
// SELECT ... FOR UPDATE within a transaction so concurrent stores serialize.
// #nosec G201 -- table name is set at construction, not user input
func (s *PostgresStore) storeDedupFallback(ctx context.Context, msg *Message, metadata []byte) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	sel := fmt.Sprintf("SELECT id FROM %s WHERE event_name=$1 AND original_id=$2 ORDER BY created_at LIMIT 1 FOR UPDATE", s.table)
	var existingID string
	err = tx.QueryRowContext(ctx, sel, msg.EventName, msg.OriginalID).Scan(&existingID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		ins := fmt.Sprintf(`INSERT INTO %s (id, event_name, original_id, payload, metadata, error, retry_count, source, created_at)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, s.table)
		if _, err = tx.ExecContext(ctx, ins, msg.ID, msg.EventName, msg.OriginalID, msg.Payload,
			metadata, msg.Error, msg.RetryCount, msg.Source, msg.CreatedAt.UTC()); err != nil {
			return fmt.Errorf("fallback insert: %w", err)
		}
	case err != nil:
		return fmt.Errorf("fallback select: %w", err)
	default:
		upd := fmt.Sprintf(`UPDATE %s SET retry_count = retry_count + 1, error=$1, payload=$2, metadata=$3, retried_at=NULL WHERE id=$4`, s.table)
		if _, err = tx.ExecContext(ctx, upd, msg.Error, msg.Payload, metadata, existingID); err != nil {
			return fmt.Errorf("fallback update: %w", err)
		}
	}
	return tx.Commit()
}

// Get retrieves a single message by ID
// #nosec G201 -- table name is set at construction, not user input
func (s *PostgresStore) Get(ctx context.Context, id string) (*Message, error) {
	query := fmt.Sprintf(`
		SELECT id, event_name, original_id, payload, metadata, error, retry_count, source, created_at, retried_at, quarantined_at
		FROM %s
		WHERE id = $1
	`, s.table)

	var msg Message
	var metadata []byte
	var retriedAt sql.NullTime
	var quarantinedAt sql.NullTime
	var source sql.NullString

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&msg.ID,
		&msg.EventName,
		&msg.OriginalID,
		&msg.Payload,
		&metadata,
		&msg.Error,
		&msg.RetryCount,
		&source,
		&msg.CreatedAt,
		&retriedAt,
		&quarantinedAt,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%s: %w", id, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	msg.Metadata, _ = base.UnmarshalMetadata(metadata)
	msg.RetriedAt = base.NullTime(retriedAt)
	msg.QuarantinedAt = base.NullTime(quarantinedAt)
	msg.Source = base.NullString(source)

	return &msg, nil
}

// List returns messages matching the filter
func (s *PostgresStore) List(ctx context.Context, filter Filter) ([]*Message, error) {
	filter = clampListLimit(filter, s.maxListLimit)
	query, args := s.buildListQuery(filter, false)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var messages []*Message
	for rows.Next() {
		var msg Message
		var metadata []byte
		var retriedAt sql.NullTime
		var quarantinedAt sql.NullTime
		var source sql.NullString

		err := rows.Scan(
			&msg.ID,
			&msg.EventName,
			&msg.OriginalID,
			&msg.Payload,
			&metadata,
			&msg.Error,
			&msg.RetryCount,
			&source,
			&msg.CreatedAt,
			&retriedAt,
			&quarantinedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		msg.Metadata, _ = base.UnmarshalMetadata(metadata)
		msg.RetriedAt = base.NullTime(retriedAt)
		msg.QuarantinedAt = base.NullTime(quarantinedAt)
		msg.Source = base.NullString(source)

		messages = append(messages, &msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	return messages, nil
}

// Count returns the number of messages matching the filter
func (s *PostgresStore) Count(ctx context.Context, filter Filter) (int64, error) {
	query, args := s.buildListQuery(filter, true)

	var count int64
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query: %w", err)
	}

	return count, nil
}

// buildFilterClauses returns a QueryBuilder populated with the standard
// DLQ filter conditions. Callers use it to construct SELECT, COUNT, or DELETE queries.
func (s *PostgresStore) buildFilterClauses(filter Filter) *base.QueryBuilder {
	qb := base.NewQueryBuilder()

	qb.AddIfNotEmpty("event_name = $%d", filter.EventName)
	qb.AddIfNotZero("created_at >= $%d", filter.After.UTC())
	qb.AddIfNotZero("created_at <= $%d", filter.Before.UTC())
	if filter.Error != "" {
		qb.Add("error ILIKE $%d", "%"+filter.Error+"%")
	}
	qb.AddIfPositive("retry_count <= $%d", filter.MaxRetries)
	qb.AddIfNotEmpty("source = $%d", filter.Source)
	qb.AddRawIf(filter.ExcludeRetried, "retried_at IS NULL")
	qb.AddRawIf(filter.ExcludeQuarantined, "quarantined_at IS NULL")

	return qb
}

// buildListQuery builds the SQL query for List and Count
func (s *PostgresStore) buildListQuery(filter Filter, countOnly bool) (string, []any) {
	qb := s.buildFilterClauses(filter)

	if countOnly {
		return qb.Build(fmt.Sprintf("SELECT COUNT(*) FROM %s %%s", s.table))
	}

	query := fmt.Sprintf(`
		SELECT id, event_name, original_id, payload, metadata, error, retry_count, source, created_at, retried_at, quarantined_at
		FROM %s
		%%s
		ORDER BY created_at DESC
	`, s.table)

	baseQuery, args := qb.Build(query)

	if filter.Limit > 0 || filter.Offset > 0 {
		limitClause := qb.AppendLimit(filter.Limit, filter.Offset)
		baseQuery += limitClause
		args = qb.Args()
	}

	return baseQuery, args
}

// #nosec G201 -- table name is set at construction, not user input
// MarkRetried marks a message as replayed
func (s *PostgresStore) MarkRetried(ctx context.Context, id string) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET retried_at = $1
		WHERE id = $2
	`, s.table)

	result, err := s.db.ExecContext(ctx, query, time.Now().UTC(), id)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	return nil
}

// Quarantine marks a message as a terminal, non-retryable failure.
// #nosec G201 -- table name is set at construction, not user input
func (s *PostgresStore) Quarantine(ctx context.Context, id string) error {
	q := fmt.Sprintf("UPDATE %s SET quarantined_at = $1 WHERE id = $2", s.table)
	res, err := s.db.ExecContext(ctx, q, time.Now().UTC(), id)
	if err != nil {
		return fmt.Errorf("quarantine: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}
	return nil
}

// Delete removes a message from the DLQ
func (s *PostgresStore) Delete(ctx context.Context, id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", s.table) // #nosec G201 -- table name is set at construction, not user input
	result, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	return nil
}

// DeleteOlderThan removes messages older than the specified age
func (s *PostgresStore) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	query := fmt.Sprintf("DELETE FROM %s WHERE created_at < $1", s.table) // #nosec G201 -- table name is set at construction, not user input
	result, err := s.db.ExecContext(ctx, query, time.Now().Add(-age).UTC())
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.RowsAffected()
}

// DeleteByFilter removes messages matching the filter
func (s *PostgresStore) DeleteByFilter(ctx context.Context, filter Filter) (int64, error) {
	query, args := s.buildDeleteQuery(filter)

	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.RowsAffected()
}

// buildDeleteQuery builds a DELETE SQL query from the filter.
// It reuses the same filter clause logic as buildListQuery but constructs
// the DELETE statement directly instead of transforming a SELECT.
func (s *PostgresStore) buildDeleteQuery(filter Filter) (string, []any) {
	qb := s.buildFilterClauses(filter)
	return qb.Build(fmt.Sprintf("DELETE FROM %s %%s", s.table))
}

// Stats returns DLQ statistics
func (s *PostgresStore) Stats(ctx context.Context) (*Stats, error) {
	stats := &Stats{
		MessagesByEvent: make(map[string]int64),
		MessagesByError: make(map[string]int64),
	}

	// Total count
	// #nosec G201 -- table name is set at construction, not user input
	err := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", s.table)).Scan(&stats.TotalMessages)
	if err != nil {
		return nil, fmt.Errorf("count total: %w", err)
	}

	// Pending count: not retried AND not quarantined.
	// #nosec G201 -- table name is set at construction, not user input
	err = s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE retried_at IS NULL AND quarantined_at IS NULL", s.table)).Scan(&stats.PendingMessages)
	if err != nil {
		return nil, fmt.Errorf("count pending: %w", err)
	}

	// Retried count: messages that have been replayed.
	// #nosec G201 -- table name is set at construction, not user input
	err = s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE retried_at IS NOT NULL", s.table)).Scan(&stats.RetriedMessages)
	if err != nil {
		return nil, fmt.Errorf("count retried: %w", err)
	}

	// Quarantined count: messages marked as terminal.
	// #nosec G201 -- table name is set at construction, not user input
	err = s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE quarantined_at IS NOT NULL", s.table)).Scan(&stats.QuarantinedMessages)
	if err != nil {
		return nil, fmt.Errorf("count quarantined: %w", err)
	}

	// Messages by event
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("SELECT event_name, COUNT(*) FROM %s GROUP BY event_name", s.table))
	if err != nil {
		return nil, fmt.Errorf("count by event: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var eventName string
		var count int64
		if err := rows.Scan(&eventName, &count); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		stats.MessagesByEvent[eventName] = count
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	// Oldest and newest
	var oldest, newest sql.NullTime
	err = s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT MIN(created_at), MAX(created_at) FROM %s", s.table)).Scan(&oldest, &newest)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("min/max: %w", err)
	}

	if oldest.Valid {
		stats.OldestMessage = &oldest.Time
	}
	if newest.Valid {
		stats.NewestMessage = &newest.Time
	}

	return stats, nil
}

// GetByOriginalID retrieves a message by its original event message ID
func (s *PostgresStore) GetByOriginalID(ctx context.Context, originalID string) (*Message, error) {
	// #nosec G201 -- table name is set at construction, not user input
	query := fmt.Sprintf(`
		SELECT id, event_name, original_id, payload, metadata, error, retry_count, source, created_at, retried_at, quarantined_at
		FROM %s
		WHERE original_id = $1
	`, s.table)

	var msg Message
	var metadata []byte
	var retriedAt sql.NullTime
	var quarantinedAt sql.NullTime
	var source sql.NullString

	err := s.db.QueryRowContext(ctx, query, originalID).Scan(
		&msg.ID,
		&msg.EventName,
		&msg.OriginalID,
		&msg.Payload,
		&metadata,
		&msg.Error,
		&msg.RetryCount,
		&source,
		&msg.CreatedAt,
		&retriedAt,
		&quarantinedAt,
	)

	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("original_id %s: %w", originalID, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	msg.Metadata, _ = base.UnmarshalMetadata(metadata)
	msg.RetriedAt = base.NullTime(retriedAt)
	msg.QuarantinedAt = base.NullTime(quarantinedAt)
	msg.Source = base.NullString(source)

	return &msg, nil
}

// Health performs a health check by pinging the PostgreSQL database.
// Returns healthy if the ping succeeds, unhealthy otherwise.
// Backend returns the store's backend name for metric labelling.
func (s *PostgresStore) Backend() string { return "postgres" }

func (s *PostgresStore) Health(ctx context.Context) *health.Result {
	start := time.Now()

	err := s.db.PingContext(ctx)
	if err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   "postgres store unreachable",
			Latency:   time.Since(start),
			CheckedAt: start,
			Details: map[string]any{
				"table": s.table,
				"error": err.Error(),
			},
		}
	}

	// Get message count
	var count int64
	err = s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", s.table)).Scan(&count)
	if err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   "postgres store count failed",
			Latency:   time.Since(start),
			CheckedAt: start,
			Details: map[string]any{
				"table": s.table,
				"error": err.Error(),
			},
		}
	}

	return &health.Result{
		Status:    health.StatusHealthy,
		Latency:   time.Since(start),
		CheckedAt: start,
		Details: map[string]any{
			"table":         s.table,
			"message_count": count,
		},
	}
}

// MigrateDedup collapses historical duplicate (event_name, original_id) rows into
// a single row (keeps the newest by created_at; sums retry_count; sets created_at
// to the oldest; keeps the earliest quarantined_at), then creates the unique index
// concurrently. It refuses to delete more than 50% of all rows unless WithForce is
// given. Returns the number of rows removed. Idempotent.
// #nosec G201 -- table name is set at construction, not user input
func (s *PostgresStore) MigrateDedup(ctx context.Context, opts ...MigrateDedupOption) (int64, error) {
	o := applyMigrateDedupOptions(opts)

	var total int64
	if err := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", s.table)).Scan(&total); err != nil {
		return 0, fmt.Errorf("count total: %w", err)
	}
	var toDelete int64
	countDel := fmt.Sprintf(`
		SELECT COALESCE(COUNT(*) - COUNT(DISTINCT (event_name, original_id)), 0)
		FROM %s WHERE original_id <> ''`, s.table)
	if err := s.db.QueryRowContext(ctx, countDel).Scan(&toDelete); err != nil {
		return 0, fmt.Errorf("count duplicates: %w", err)
	}
	if !o.force && total > 0 && toDelete*2 > total {
		return 0, fmt.Errorf("dlq: MigrateDedup would delete %d of %d rows (>50%%); pass WithForce to proceed", toDelete, total)
	}
	if toDelete == 0 {
		// Nothing to collapse; still ensure the unique index exists.
		if err := s.createUniqueIndexConcurrently(ctx); err != nil {
			return 0, err
		}
		return 0, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Single-pass: compute ranks + aggregates once, then UPDATE survivors and
	// DELETE duplicates in two separate statements that both reference the same
	// pre-computed ranking (stored in a temp table so the ranking is frozen
	// before the UPDATE mutates created_at).
	createTmp := fmt.Sprintf(`
		CREATE TEMP TABLE _dlq_dedup_work ON COMMIT DROP AS
		SELECT id,
			row_number() OVER (PARTITION BY event_name, original_id ORDER BY created_at DESC, id) AS rn,
			SUM(retry_count)    OVER (PARTITION BY event_name, original_id) AS sum_retry,
			MIN(created_at)     OVER (PARTITION BY event_name, original_id) AS min_created,
			MIN(quarantined_at) OVER (PARTITION BY event_name, original_id) AS min_quar
		FROM %s WHERE original_id <> ''`, s.table)
	if _, err := tx.ExecContext(ctx, createTmp); err != nil {
		return 0, fmt.Errorf("create temp: %w", err)
	}

	// Update survivors (rn=1) with aggregated values.
	updSurvivors := fmt.Sprintf(`
		UPDATE %s t
		SET retry_count    = w.sum_retry,
		    created_at     = w.min_created,
		    quarantined_at = w.min_quar
		FROM _dlq_dedup_work w
		WHERE t.id = w.id AND w.rn = 1`, s.table)
	if _, err := tx.ExecContext(ctx, updSurvivors); err != nil {
		return 0, fmt.Errorf("update survivors: %w", err)
	}

	// Delete non-survivors (rn>1) using the frozen ranking.
	delDups := fmt.Sprintf(`
		DELETE FROM %s t
		USING _dlq_dedup_work w
		WHERE t.id = w.id AND w.rn > 1`, s.table)
	res, err := tx.ExecContext(ctx, delDups)
	if err != nil {
		return 0, fmt.Errorf("delete duplicates: %w", err)
	}
	removed, _ := res.RowsAffected()

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}

	if err := s.createUniqueIndexConcurrently(ctx); err != nil {
		return removed, err
	}
	return removed, nil
}

// createUniqueIndexConcurrently creates the dedup unique index without holding a
// long write lock. CONCURRENTLY cannot run inside a transaction, so this runs as
// a standalone statement.
// #nosec G201 -- table name is set at construction, not user input
func (s *PostgresStore) createUniqueIndexConcurrently(ctx context.Context) error {
	idx := fmt.Sprintf(
		"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uniq_%s_event_original ON %s(event_name, original_id)",
		s.table, s.table)
	if _, err := s.db.ExecContext(ctx, idx); err != nil {
		return fmt.Errorf("create unique index: %w", err)
	}
	return nil
}

// Compile-time checks
var _ Store = (*PostgresStore)(nil)
var _ StatsProvider = (*PostgresStore)(nil)
var _ health.Checker = (*PostgresStore)(nil)
var _ Quarantiner = (*PostgresStore)(nil)
