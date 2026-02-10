package dlq

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

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
	table string
}

// WithTable sets a custom table name.
// The name must contain only alphanumeric characters and underscores.
func WithTable(table string) PostgresStoreOption {
	return func(o *postgresStoreOptions) {
		if table != "" {
			if !validIdentifier.MatchString(table) {
				panic(fmt.Sprintf("dlq: invalid table name %q", table))
			}
			o.table = table
		}
	}
}

// PostgresStore is a PostgreSQL-based DLQ store
type PostgresStore struct {
	db    *sql.DB
	table string
}

// NewPostgresStore creates a new PostgreSQL DLQ store.
func NewPostgresStore(db *sql.DB, opts ...PostgresStoreOption) *PostgresStore {
	o := &postgresStoreOptions{
		table: "event_dlq",
	}
	for _, opt := range opts {
		opt(o)
	}

	return &PostgresStore{
		db:    db,
		table: o.table,
	}
}

// EnsureTable creates the DLQ table and indexes if they don't exist.
// This is safe to call multiple times (uses IF NOT EXISTS).
func (s *PostgresStore) EnsureTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
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
		)
	`, s.table)

	if _, err := s.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

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
		msg.CreatedAt,
	)

	if err != nil {
		return fmt.Errorf("insert: %w", err)
	}

	return nil
}

// Get retrieves a single message by ID
func (s *PostgresStore) Get(ctx context.Context, id string) (*Message, error) {
	query := fmt.Sprintf(`
		SELECT id, event_name, original_id, payload, metadata, error, retry_count, source, created_at, retried_at
		FROM %s
		WHERE id = $1
	`, s.table)

	var msg Message
	var metadata []byte
	var retriedAt sql.NullTime
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
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%s: %w", id, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	msg.Metadata, _ = base.UnmarshalMetadata(metadata)
	msg.RetriedAt = base.NullTime(retriedAt)
	msg.Source = base.NullString(source)

	return &msg, nil
}

// List returns messages matching the filter
func (s *PostgresStore) List(ctx context.Context, filter Filter) ([]*Message, error) {
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
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		msg.Metadata, _ = base.UnmarshalMetadata(metadata)
		msg.RetriedAt = base.NullTime(retriedAt)
		msg.Source = base.NullString(source)

		messages = append(messages, &msg)
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

// buildListQuery builds the SQL query for List and Count
func (s *PostgresStore) buildListQuery(filter Filter, countOnly bool) (string, []any) {
	qb := base.NewQueryBuilder()

	qb.AddIfNotEmpty("event_name = $%d", filter.EventName)
	qb.AddIfNotZero("created_at >= $%d", filter.StartTime)
	qb.AddIfNotZero("created_at <= $%d", filter.EndTime)
	if filter.Error != "" {
		qb.Add("error ILIKE $%d", "%"+filter.Error+"%")
	}
	qb.AddIfPositive("retry_count <= $%d", filter.MaxRetries)
	qb.AddIfNotEmpty("source = $%d", filter.Source)
	qb.AddRawIf(filter.ExcludeRetried, "retried_at IS NULL")

	if countOnly {
		return qb.Build(fmt.Sprintf("SELECT COUNT(*) FROM %s %%s", s.table))
	}

	query := fmt.Sprintf(`
		SELECT id, event_name, original_id, payload, metadata, error, retry_count, source, created_at, retried_at
		FROM %s
		%%s
		ORDER BY created_at DESC
	`, s.table)

	baseQuery, args := qb.Build(query)

	if filter.Limit > 0 {
		limitClause := qb.AppendLimit(filter.Limit, filter.Offset)
		baseQuery += limitClause
		args = qb.Args()
	}

	return baseQuery, args
}

// MarkRetried marks a message as replayed
func (s *PostgresStore) MarkRetried(ctx context.Context, id string) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET retried_at = $1
		WHERE id = $2
	`, s.table)

	result, err := s.db.ExecContext(ctx, query, time.Now(), id)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	return nil
}

// Delete removes a message from the DLQ
func (s *PostgresStore) Delete(ctx context.Context, id string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", s.table)

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
	query := fmt.Sprintf("DELETE FROM %s WHERE created_at < $1", s.table)

	result, err := s.db.ExecContext(ctx, query, time.Now().Add(-age))
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.RowsAffected()
}

// DeleteByFilter removes messages matching the filter
func (s *PostgresStore) DeleteByFilter(ctx context.Context, filter Filter) (int64, error) {
	listQuery, args := s.buildListQuery(filter, false)
	// Convert SELECT to DELETE
	deleteQuery := strings.Replace(listQuery, "SELECT id, event_name, original_id, payload, metadata, error, retry_count, source, created_at, retried_at", "DELETE", 1)
	// Remove ORDER BY and LIMIT for delete
	if idx := strings.Index(deleteQuery, "ORDER BY"); idx > 0 {
		deleteQuery = deleteQuery[:idx]
	}

	result, err := s.db.ExecContext(ctx, deleteQuery, args...)
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.RowsAffected()
}

// Stats returns DLQ statistics
func (s *PostgresStore) Stats(ctx context.Context) (*Stats, error) {
	stats := &Stats{
		MessagesByEvent: make(map[string]int64),
		MessagesByError: make(map[string]int64),
	}

	// Total count
	err := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", s.table)).Scan(&stats.TotalMessages)
	if err != nil {
		return nil, fmt.Errorf("count total: %w", err)
	}

	// Pending count
	err = s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE retried_at IS NULL", s.table)).Scan(&stats.PendingMessages)
	if err != nil {
		return nil, fmt.Errorf("count pending: %w", err)
	}

	stats.RetriedMessages = stats.TotalMessages - stats.PendingMessages

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

	// Oldest and newest
	var oldest, newest sql.NullTime
	err = s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT MIN(created_at), MAX(created_at) FROM %s", s.table)).Scan(&oldest, &newest)
	if err != nil && err != sql.ErrNoRows {
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
	query := fmt.Sprintf(`
		SELECT id, event_name, original_id, payload, metadata, error, retry_count, source, created_at, retried_at
		FROM %s
		WHERE original_id = $1
	`, s.table)

	var msg Message
	var metadata []byte
	var retriedAt sql.NullTime
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
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("original_id %s: %w", originalID, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	msg.Metadata, _ = base.UnmarshalMetadata(metadata)
	msg.RetriedAt = base.NullTime(retriedAt)
	msg.Source = base.NullString(source)

	return &msg, nil
}

// Compile-time checks
var _ Store = (*PostgresStore)(nil)
var _ StatsProvider = (*PostgresStore)(nil)
