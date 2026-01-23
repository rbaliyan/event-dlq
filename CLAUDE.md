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
    ID         string            // DLQ message ID (generated)
    EventName  string            // Original event name
    OriginalID string            // Original message ID
    Payload    []byte            // Message payload
    Metadata   map[string]string // Message metadata
    Error      string            // Failure error
    RetryCount int               // Retries attempted
    CreatedAt  time.Time         // When added to DLQ
    RetriedAt  *time.Time        // When replayed (nil if never)
    Source     string            // Source service
}

// Filter for querying messages
type Filter struct {
    EventName      string
    StartTime      time.Time
    EndTime        time.Time
    Error          string    // Contains match
    MaxRetries     int
    Source         string
    ExcludeRetried bool
    Limit          int
    Offset         int
}

// Statistics
type Stats struct {
    TotalMessages   int64
    PendingMessages int64
    RetriedMessages int64
    MessagesByEvent map[string]int64
    MessagesByError map[string]int64
    OldestMessage   *time.Time
    NewestMessage   *time.Time
}
```

### Design Patterns

**Interface-Based Extensibility**: `Store` interface allows custom implementations

**Optional Interface**: `StatsProvider` for stores that support detailed statistics

**Builder Pattern**: Store constructors return self for method chaining:
```go
store := dlq.NewMongoStore(db).
    WithCollection("custom_dlq")
```

**Replay Metadata**: Replayed messages include DLQ metadata:
- `dlq_replay`: "true"
- `dlq_message_id`: DLQ ID
- `dlq_original_error`: original failure

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
