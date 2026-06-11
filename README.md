# event-dlq

[![CI](https://github.com/rbaliyan/event-dlq/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/event-dlq/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/event-dlq.svg)](https://pkg.go.dev/github.com/rbaliyan/event-dlq)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/event-dlq)](https://goreportcard.com/report/github.com/rbaliyan/event-dlq)
[![Release](https://img.shields.io/github/v/release/rbaliyan/event-dlq)](https://github.com/rbaliyan/event-dlq/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/rbaliyan/event-dlq/badge)](https://scorecard.dev/viewer/?uri=github.com/rbaliyan/event-dlq)

Dead Letter Queue (DLQ) management package for the [event](https://github.com/rbaliyan/event) pub-sub library.

## Overview

This package provides dead-letter queue capabilities for handling messages that fail processing after all retry attempts. Key features include:

- **Message Storage**: Persist failed messages with full error context
- **Filtering & Listing**: Query DLQ messages by event, time, error, source, and retry status
- **Replay**: Republish failed messages back to their original events
- **Cleanup**: Remove old messages automatically or on-demand
- **Statistics**: Monitor DLQ health with detailed metrics

## Installation

```bash
go get github.com/rbaliyan/event-dlq
```

## Store Implementations

| Store | Use Case |
|-------|----------|
| `MemoryStore` | Testing and development |
| `PostgresStore` | Production with PostgreSQL |
| `MongoStore` | Production with MongoDB |
| `RedisStore` | Production with Redis |

## Usage

### Basic Setup

```go
import (
    "github.com/rbaliyan/event-dlq"
    "github.com/rbaliyan/event/v3/transport"
)

// Create a store (choose one based on your infrastructure)
store := dlq.NewMemoryStore()                    // For testing
// store := dlq.NewPostgresStore(db)             // For PostgreSQL
// store := dlq.NewMongoStore(mongoDatabase)     // For MongoDB
// store := dlq.NewRedisStore(redisClient)       // For Redis

// Create the manager with your transport
manager, err := dlq.NewManager(store, transport)
if err != nil {
    log.Fatal(err)
}
```

### Storing Failed Messages

Store messages that have exhausted all retries:

```go
func handleMessage(ctx context.Context, msg Message) error {
    for attempt := 0; attempt < maxRetries; attempt++ {
        if err := process(ctx, msg); err == nil {
            return nil // Success
        }
    }

    // All retries exhausted - send to DLQ
    return manager.Store(ctx, dlq.StoreParams{
        EventName:  "order.process",
        OriginalID: msg.ID,
        Payload:    msg.Payload,
        Metadata:   msg.Metadata,
        Err:        errors.New("max retries"),
        RetryCount: maxRetries,
        Source:     "order-service",
    })
}
```

### Listing DLQ Messages

Query messages with flexible filtering:

```go
// List all pending messages for a specific event
messages, err := manager.List(ctx, dlq.Filter{
    EventName:      "order.process",
    ExcludeRetried: true,
    Limit:          100,
})

// List messages from the last 24 hours
messages, err := manager.List(ctx, dlq.Filter{
    StartTime: time.Now().Add(-24 * time.Hour),
})

// List messages with specific error pattern
messages, err := manager.List(ctx, dlq.Filter{
    Error: "timeout",
    Limit: 50,
})
```

### Retrieving a Single Message

```go
msg, err := manager.Get(ctx, "dlq-message-id")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Event: %s, Error: %s, Retries: %d\n",
    msg.EventName, msg.Error, msg.RetryCount)
```

### Counting Messages

```go
// Count all messages
total, err := manager.Count(ctx, dlq.Filter{})

// Count pending messages for specific event
pending, err := manager.Count(ctx, dlq.Filter{
    EventName:      "payment.failed",
    ExcludeRetried: true,
})
```

### Replaying Messages

Replay messages after fixing the underlying issue:

```go
// Replay all unretried messages for an event
replayed, err := manager.Replay(ctx, dlq.Filter{
    EventName:      "order.process",
    ExcludeRetried: true,
})
fmt.Printf("Replayed %d messages\n", replayed)

// Replay a single message by ID
err := manager.ReplaySingle(ctx, "dlq-message-id")
```

Replayed messages include additional metadata:
- `dlq_replay`: "true"
- `dlq_message_id`: original DLQ message ID
- `dlq_original_error`: the error that caused the original failure

### Deleting Messages

```go
// Delete a single message
err := manager.Delete(ctx, "dlq-message-id")

// Delete all messages matching a filter
deleted, err := manager.DeleteByFilter(ctx, dlq.Filter{
    EventName: "test.event",
})
```

### Cleanup Old Messages

```go
// Remove messages older than 30 days
deleted, err := manager.Cleanup(ctx, 30*24*time.Hour)
fmt.Printf("Cleaned up %d old messages\n", deleted)
```

### Getting Statistics

```go
stats, err := manager.Stats(ctx)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Total Messages: %d\n", stats.TotalMessages)
fmt.Printf("Pending: %d\n", stats.PendingMessages)
fmt.Printf("Retried: %d\n", stats.RetriedMessages)

for event, count := range stats.MessagesByEvent {
    fmt.Printf("  %s: %d\n", event, count)
}
```

## Store-Specific Configuration

### PostgreSQL

```go
import "database/sql"

db, _ := sql.Open("postgres", connectionString)
store := dlq.NewPostgresStore(db).
    WithTable("custom_dlq_table") // Optional: custom table name
```

Required schema:
```sql
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
```

### MongoDB

```go
import "go.mongodb.org/mongo-driver/v2/mongo"

db := mongoClient.Database("myapp")
store := dlq.NewMongoStore(db).
    WithCollection("custom_dlq") // Optional: custom collection name

// Create indexes
err := store.EnsureIndexes(ctx)

// Optional: Create as capped collection for automatic cleanup
err := store.CreateCapped(ctx, 100*1024*1024, 50000) // 100MB, max 50k docs
```

### Redis

```go
import "github.com/redis/go-redis/v9"

client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
store := dlq.NewRedisStore(client).
    WithKeyPrefix("myapp:dlq:").  // Optional: custom key prefix
    WithMaxLen(100000)            // Optional: max stream length
```

## Filter Options

| Field | Description |
|-------|-------------|
| `EventName` | Filter by event name (exact match) |
| `After` | Filter messages created after this time |
| `Before` | Filter messages created before this time |
| `Error` | Filter by error message (contains match) |
| `MaxRetries` | Filter by maximum retry count |
| `Source` | Filter by source service |
| `ExcludeRetried` | Exclude already replayed messages |
| `ExcludeQuarantined` | Exclude quarantined (terminal) messages |
| `Limit` | Maximum number of results |
| `Offset` | Pagination offset |

## Message Structure

```go
type Message struct {
    ID             string            // Unique DLQ message ID
    EventName      string            // Original event name
    OriginalID     string            // Original message ID
    Payload        []byte            // Message payload
    Metadata       map[string]string // Message metadata
    Error          string            // Error description
    RetryCount     int               // Retries attempted
    CreatedAt      time.Time         // When added to DLQ
    RetriedAt      *time.Time        // When replayed (nil if never)
    QuarantinedAt  *time.Time        // When classified as terminal (nil if not quarantined)
    Source         string            // Source service
}
```

## Preventing replay loops (terminal-error quarantine)

Some messages will never succeed regardless of how many times they are replayed — for example, messages whose payload cannot be decoded against the current schema, or whose downstream dependency has been permanently removed. Replaying them forever wastes resources and can destabilise consumers.

`WithTerminalError` adds a predicate to the Manager. During `Replay` and `ReplaySingle`, any message for which the predicate returns true is **quarantined** instead of republished: its `QuarantinedAt` timestamp is set and it is excluded from every future sweep automatically.

```go
manager, err := dlq.NewManager(store, bus,
    // Quarantine messages whose error contains either substring.
    dlq.WithTerminalError(dlq.TerminalErrorMatching(
        "proto: cannot parse",
        "unknown field",
    )),
)
if err != nil {
    log.Fatal(err)
}
```

`TerminalErrorMatching` is a convenience helper that performs a case-sensitive substring match against `Message.Error`. An empty error never matches. You can supply any `func(*dlq.Message) bool` for more complex logic:

```go
dlq.WithTerminalError(func(msg *dlq.Message) bool {
    return strings.HasPrefix(msg.Error, "SCHEMA_INCOMPATIBLE:")
})
```

**Behaviour at a glance:**
- Default is `nil` — when unset, `Replay` behaves exactly as before (every message is replayed).
- Quarantine requires the store to implement the optional `Quarantiner` interface (all four built-in stores do). If the store does not implement `Quarantiner`, the message is skipped with a warning instead of being republished.
- `Replay` always sets `Filter.ExcludeQuarantined = true` internally, so quarantined messages are never re-evaluated on subsequent sweeps.
- `Stats.QuarantinedMessages` counts quarantined messages; `Stats.PendingMessages` excludes them.
- The OTel counter `dlq_messages_quarantined_total{event}` increments on each quarantine.

### Stopping an active poison-replay loop (operational runbook)

If a deployment is already caught in a replay loop before quarantine is configured:

1. **Disable the scheduled Replay job** to stop further republishing.
2. **Identify and purge the poison rows** — use `DeleteByFilter` or a direct query filtered by the known error pattern.
3. **Fix the upstream decode/schema error** that caused the permanent failures.
4. Re-deploy with `WithTerminalError` wired into the Manager.
5. **Re-enable the Replay job**.

## Optional deduplication

By default each `Store` call inserts a distinct row, so multiple failures of the same original message each occupy their own DLQ entry. This is correct for most workloads where every failure is independent.

When a consumer can re-DLQ the same logical message multiple times (e.g., at-least-once delivery with an unhealthy consumer), opt-in deduplication collapses those entries into a single row. Enable it per store:

```go
// Memory (testing)
store := dlq.NewMemoryStore(dlq.WithMemoryDedup())

// PostgreSQL
store, err := dlq.NewPostgresStore(db, dlq.WithPostgresDedup())

// MongoDB
store, err := dlq.NewMongoStore(db, dlq.WithMongoDedup())

// Redis
store, err := dlq.NewRedisStore(client, dlq.WithRedisDedup())
```

When dedup is enabled, re-storing a message with the same non-empty `(EventName, OriginalID)` pair **upserts**: it increments `retry_count`, updates the error/payload/metadata to the latest values, preserves `created_at` (first-seen timestamp), and clears `retried_at`. Messages with an empty `OriginalID` always insert as distinct rows.

A unique index on `(event_name, original_id)` is created automatically when dedup is enabled. For Mongo this happens in `EnsureIndexes`; for Postgres in `EnsureTable`. Index creation is failure-tolerant — if legacy duplicates are present the index creation is skipped with a warning (see `MigrateDedup` below).

**Semantic shift when dedup is enabled:** `retry_count` becomes a re-DLQ recurrence counter rather than a count of retries before DLQ. The first insert keeps the caller-supplied value; subsequent re-stores increment it. This change applies only when dedup is enabled; the default (dedup off) is unchanged.

### Migrating an existing table or collection

If you enable dedup on a store that already contains duplicate `(event_name, original_id)` rows, index creation will fail silently. Run `MigrateDedup` once before — or immediately after — enabling the option:

```go
// Run once during deployment / migration
removed, err := store.MigrateDedup(ctx)
if err != nil {
    log.Fatal("dedup migration failed", err)
}
log.Printf("collapsed %d duplicate DLQ entries", removed)
```

`MigrateDedup` collapses each duplicate group: it keeps the newest row, sums `retry_count`, preserves the oldest `created_at`, and OR-s the quarantine flag. It then creates the unique index.

As a safety guard it refuses to delete more than 50 % of all rows unless `WithForce` is passed:

```go
removed, err := store.MigrateDedup(ctx, dlq.WithForce())
```

`MigrateDedup` is idempotent — a second call on an already-clean store is a no-op. Postgres uses `CREATE UNIQUE INDEX CONCURRENTLY` so the migration can run without locking the table.

## Best Practices

1. **Store before exhausting retries**: Add messages to DLQ after a reasonable number of retries
2. **Include context**: Add source service and meaningful metadata for debugging
3. **Monitor DLQ size**: Set up alerts when DLQ grows unexpectedly
4. **Regular cleanup**: Schedule periodic cleanup of old messages
5. **Automated replay**: Consider automated replay for recoverable errors after fixes are deployed
6. **Use terminal-error quarantine**: Wire `WithTerminalError` to prevent poison messages from blocking replay sweeps indefinitely
7. **Enable dedup for recurring failures**: Use `With*Dedup()` when the same original message may re-enter the DLQ multiple times; run `MigrateDedup` on any pre-existing table first

## Examples

See the `examples/` directory for a complete at-least-once delivery example that demonstrates:

- Transactional outbox pattern for atomic database writes with event publishing
- Resume tokens for crash recovery
- Ack store for tracking acknowledged events
- DLQ integration for handling permanent failures
- Worker groups for load-balanced message processing
- Background DLQ maintenance (cleanup and stats)

Run the example with MongoDB (requires replica set):

```bash
cd examples
go run main.go
```

## License

MIT License - see LICENSE file for details.
