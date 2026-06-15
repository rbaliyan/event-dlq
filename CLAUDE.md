# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Build Commands

```bash
go test ./...          # Run all tests
go test -run TestName  # Run a specific test
go build ./...         # Build all packages
go mod tidy            # Clean up dependencies
```

## Project Overview

Dead Letter Queue (DLQ) package for the `github.com/rbaliyan/event` event pub-sub library. Provides storage, retrieval, replay, and cleanup of messages that failed processing after exhausting retries.

## Architecture

### Core Components

**Store Interface (`store.go`)** - Defines the contract for DLQ persistence:
- `Store(msg)` - Add a failed message
- `Get(id)` - Retrieve a message by ID
- `List(filter)` - Query messages with filtering
- `Count(filter)` - Count matching messages
- `MarkRetried(id)` - Mark message as replayed
- `Delete(id)` - Remove a message
- `DeleteOlderThan(age)` - Cleanup old messages
- `DeleteByFilter(filter)` - Bulk delete matching messages

**Manager (`manager.go`)** - High-level API for DLQ operations:
- Wraps a Store and Transport
- `Store()` - Creates DLQ message with auto-generated ID
- `Replay()` - Republishes messages to original events
- `ReplaySingle()` - Replay a single message by ID
- `Cleanup()` - Remove old messages
- `Stats()` - Get DLQ statistics

**Store Implementations:**
- `MemoryStore` (`memory.go`) - Thread-safe in-memory store for testing
- `PostgresStore` (`postgres.go`) - PostgreSQL with JSONB metadata
- `MongoStore` (`mongodb.go`) - MongoDB with capped collection support
- `RedisStore` (`redis.go`) - Redis Streams + Hashes

### Key Types

```go
// Message in the DLQ
type Message struct {
    ID             string            // DLQ message ID (generated)
    EventName      string            // Original event name
    OriginalID     string            // Original message ID
    Payload        []byte            // Message payload
    Metadata       map[string]string // Message metadata
    Error          string            // Failure error
    RetryCount     int               // Retries attempted (or re-DLQ count when dedup enabled)
    CreatedAt      time.Time         // When added to DLQ (first-seen when dedup enabled)
    RetriedAt      *time.Time        // When replayed (nil if never)
    QuarantinedAt  *time.Time        // When Replay classified as terminal (nil if not quarantined)
    Source         string            // Source service
}

// Filter for querying messages
type Filter struct {
    EventName          string
    After              time.Time // messages received after this time
    Before             time.Time // messages received before this time
    Error              string    // Contains match
    MaxRetries         int
    Source             string
    ExcludeRetried     bool
    ExcludeQuarantined bool      // Exclude terminal/quarantined messages
    Limit              int
    Offset             int
}

// Statistics
type Stats struct {
    TotalMessages       int64
    PendingMessages     int64            // Excludes quarantined messages
    RetriedMessages     int64
    QuarantinedMessages int64            // Messages quarantined as terminal
    MessagesByEvent     map[string]int64
    MessagesByError     map[string]int64
    OldestMessage       *time.Time
    NewestMessage       *time.Time
}
```

### Design Patterns

**Interface-Based Extensibility**: `Store` interface allows custom implementations

**Optional Interface**: `StatsProvider` for stores that support detailed statistics

**Optional Interface**: `Quarantiner` for stores that support terminal-message quarantine — `Quarantine(ctx, id) error`; detected via type assertion (same pattern as `StatsProvider`); all four built-in stores implement it

**Builder Pattern**: Store constructors return self for method chaining:
```go
store := dlq.NewMongoStore(db).
    WithCollection("custom_dlq")
```

**Replay Metadata**: Replayed messages include DLQ metadata:
- `dlq_replay`: "true"
- `dlq_message_id`: DLQ ID
- `dlq_original_error`: original failure

**Terminal-error quarantine**: `WithTerminalError(func(*Message) bool) ManagerOption` — default nil (no behaviour change). When set, `Replay`/`ReplaySingle` quarantine matching messages instead of republishing them. `TerminalErrorMatching(patterns ...string)` is a helper that does case-sensitive substring match on `Message.Error` (empty error never matches). `Replay` always forces `Filter{ExcludeQuarantined:true}` internally.

**Max replay attempts (error-agnostic loop cap)**: `WithMaxReplayAttempts(n int) ManagerOption` — default 0 (unlimited, prior behaviour). When n>0, `Replay`/`ReplaySingle` quarantine a message once it has been replayed n times, *regardless of why it fails*. The count rides in message metadata under `MetadataReplayCount` (`dlq_replay_count`) and is incremented by `replayMessage` on each republish, so it accumulates across the republish→re-DLQ cycle without requiring dedup or a stable row. This is the generic backstop against a permanently-failing message (decode error, future type change, bad payload) looping forever; prefer it over `WithTerminalError` for guaranteed termination. Quarantine reason is logged/distinguished as `max_replay_attempts` vs `terminal_error`.

**Opt-in deduplication**: OFF by default — every `Store` call inserts a distinct row. Enable per backend: `WithMemoryDedup()`, `WithMongoDedup()`, `WithPostgresDedup()`, `WithRedisDedup()`. When enabled, re-storing a message with the same non-empty `(EventName, OriginalID)` upserts: increments `retry_count`, updates error/payload/metadata, preserves `created_at`, clears `retried_at`. Empty `OriginalID` always inserts distinct.

**MigrateDedup**: `MigrateDedup(ctx, ...MigrateDedupOption) (int64, error)` on Mongo/Postgres/Redis stores. Collapses existing duplicates (keeps newest, sums retry_count, keeps oldest created_at, OR-s quarantine), then creates the unique index. Refuses to delete >50% of rows unless `WithForce()` is passed. Run before enabling dedup on a table/collection that already has data.

## Dependencies

- `github.com/rbaliyan/event/v3` - Event library (transport, message types)
- `github.com/google/uuid` - Message ID generation
- `github.com/redis/go-redis/v9` - Redis client
- `go.mongodb.org/mongo-driver` - MongoDB driver
- `go.opentelemetry.io/otel/trace` - Span context for replay

## Testing

Tests use `MemoryStore` and mock transport. Run with:
```bash
go test -v ./...
```

## Examples

See `examples/main.go` for a complete at-least-once delivery setup with:
- Transactional outbox
- Resume tokens
- Ack store
- DLQ integration
- Worker groups
