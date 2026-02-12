//go:build integration

package dlq

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// getMongoClient creates a MongoDB client for integration tests.
// Set MONGO_URI environment variable to override the default connection string.
func getMongoClient(t *testing.T) *mongo.Client {
	t.Helper()

	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Skipf("MongoDB not available: %v", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		client.Disconnect(ctx)
		t.Skipf("MongoDB not available: %v", err)
	}

	t.Cleanup(func() {
		client.Disconnect(context.Background())
	})

	return client
}

// getPostgresDB creates a PostgreSQL connection for integration tests.
// Set POSTGRES_URI environment variable to override the default connection string.
func getPostgresDB(t *testing.T) *sql.DB {
	t.Helper()

	uri := os.Getenv("POSTGRES_URI")
	if uri == "" {
		uri = "postgres://localhost:5432/test?sslmode=disable"
	}

	db, err := sql.Open("postgres", uri)
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Skipf("PostgreSQL not available: %v", err)
	}

	t.Cleanup(func() {
		db.Close()
	})

	return db
}

func TestMongoStoreIntegration(t *testing.T) {
	client := getMongoClient(t)
	ctx := context.Background()

	// Use a unique database for this test
	dbName := "dlq_test_" + time.Now().Format("20060102150405")
	db := client.Database(dbName)

	t.Cleanup(func() {
		db.Drop(context.Background())
	})

	store := NewMongoStore(db, WithCollection("dlq_messages"))
	if err := store.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes failed: %v", err)
	}

	runStoreTests(t, store)
}

func TestMongoStoreCappedIntegration(t *testing.T) {
	client := getMongoClient(t)
	ctx := context.Background()

	// Use a unique database for this test
	dbName := "dlq_capped_test_" + time.Now().Format("20060102150405")
	db := client.Database(dbName)

	t.Cleanup(func() {
		db.Drop(context.Background())
	})

	store := NewMongoStore(db, WithCollection("dlq_capped"))

	// Create as capped collection (1MB, 1000 docs max)
	err := store.CreateCapped(ctx, 1024*1024, 1000)
	if err != nil {
		t.Fatalf("CreateCapped failed: %v", err)
	}

	// Verify it's capped
	capped, err := store.IsCapped(ctx)
	if err != nil {
		t.Fatalf("IsCapped failed: %v", err)
	}
	if !capped {
		t.Error("expected collection to be capped")
	}

	// Test basic operations
	msg := &Message{
		ID:         "dlq-capped-1",
		EventName:  "order.created",
		OriginalID: "msg-123",
		Payload:    []byte(`{"id":"order-1"}`),
		Metadata:   map[string]string{"key": "value"},
		Error:      "processing failed",
		RetryCount: 3,
		CreatedAt:  time.Now(),
	}

	err = store.Store(ctx, msg)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	retrieved, err := store.Get(ctx, "dlq-capped-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if retrieved.ID != "dlq-capped-1" {
		t.Errorf("expected ID dlq-capped-1, got %s", retrieved.ID)
	}

	// Delete should return error for capped collection
	err = store.Delete(ctx, "dlq-capped-1")
	if err == nil {
		t.Error("expected error when deleting from capped collection")
	}

	// DeleteOlderThan should return 0 for capped collection (no-op)
	deleted, err := store.DeleteOlderThan(ctx, time.Hour)
	if err != nil {
		t.Errorf("DeleteOlderThan failed: %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected 0 deleted for capped collection, got %d", deleted)
	}
}

func TestPostgresStoreIntegration(t *testing.T) {
	db := getPostgresDB(t)
	ctx := context.Background()

	// Use a unique table for this test
	tableName := "dlq_test_" + time.Now().Format("20060102150405")

	store, err := NewPostgresStore(db, WithTable(tableName))
	if err != nil {
		t.Fatalf("NewPostgresStore failed: %v", err)
	}
	if err := store.EnsureTable(ctx); err != nil {
		t.Fatalf("EnsureTable failed: %v", err)
	}

	t.Cleanup(func() {
		db.Exec("DROP TABLE IF EXISTS " + tableName)
	})

	runStoreTests(t, store)
}

// runStoreTests runs common store tests against any Store implementation
func runStoreTests(t *testing.T, store Store) {
	ctx := context.Background()

	t.Run("Store and Get", func(t *testing.T) {
		msg := &Message{
			ID:         "dlq-int-1",
			EventName:  "order.created",
			OriginalID: "msg-123",
			Payload:    []byte(`{"id":"order-1"}`),
			Metadata:   map[string]string{"key": "value"},
			Error:      "processing failed",
			RetryCount: 3,
			CreatedAt:  time.Now(),
			Source:     "order-service",
		}

		err := store.Store(ctx, msg)
		if err != nil {
			t.Fatalf("Store failed: %v", err)
		}

		retrieved, err := store.Get(ctx, "dlq-int-1")
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if retrieved.ID != "dlq-int-1" {
			t.Errorf("expected ID dlq-int-1, got %s", retrieved.ID)
		}
		if retrieved.EventName != "order.created" {
			t.Errorf("expected event order.created, got %s", retrieved.EventName)
		}
		if retrieved.OriginalID != "msg-123" {
			t.Errorf("expected original ID msg-123, got %s", retrieved.OriginalID)
		}
		if string(retrieved.Payload) != `{"id":"order-1"}` {
			t.Errorf("unexpected payload: %s", retrieved.Payload)
		}
		if retrieved.Metadata["key"] != "value" {
			t.Error("expected metadata key=value")
		}
		if retrieved.RetryCount != 3 {
			t.Errorf("expected retry count 3, got %d", retrieved.RetryCount)
		}
	})

	t.Run("Get non-existent returns error", func(t *testing.T) {
		_, err := store.Get(ctx, "non-existent-id")
		if err == nil {
			t.Error("expected error for non-existent message")
		}
	})

	t.Run("List with filters", func(t *testing.T) {
		// Store additional messages
		store.Store(ctx, &Message{ID: "dlq-int-2", EventName: "order.created", CreatedAt: time.Now()})
		store.Store(ctx, &Message{ID: "dlq-int-3", EventName: "order.updated", CreatedAt: time.Now()})
		store.Store(ctx, &Message{ID: "dlq-int-4", EventName: "order.created", CreatedAt: time.Now()})

		// Filter by event name
		messages, err := store.List(ctx, Filter{EventName: "order.created"})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		count := 0
		for _, msg := range messages {
			if msg.EventName == "order.created" {
				count++
			}
		}
		if count < 3 {
			t.Errorf("expected at least 3 order.created messages, got %d", count)
		}
	})

	t.Run("Count", func(t *testing.T) {
		count, err := store.Count(ctx, Filter{})
		if err != nil {
			t.Fatalf("Count failed: %v", err)
		}

		if count < 4 {
			t.Errorf("expected at least 4 messages, got %d", count)
		}
	})

	t.Run("MarkRetried", func(t *testing.T) {
		err := store.MarkRetried(ctx, "dlq-int-2")
		if err != nil {
			t.Fatalf("MarkRetried failed: %v", err)
		}

		msg, _ := store.Get(ctx, "dlq-int-2")
		if msg.RetriedAt == nil {
			t.Error("expected RetriedAt to be set")
		}
	})

	t.Run("List ExcludeRetried", func(t *testing.T) {
		messages, err := store.List(ctx, Filter{ExcludeRetried: true})
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		for _, msg := range messages {
			if msg.RetriedAt != nil {
				t.Errorf("message %s should not have RetriedAt set", msg.ID)
			}
		}
	})

	t.Run("Delete", func(t *testing.T) {
		err := store.Delete(ctx, "dlq-int-3")
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		_, err = store.Get(ctx, "dlq-int-3")
		if err == nil {
			t.Error("expected error after delete")
		}
	})

	t.Run("DeleteOlderThan", func(t *testing.T) {
		// Store an old message
		oldMsg := &Message{
			ID:        "dlq-int-old",
			EventName: "old.event",
			CreatedAt: time.Now().Add(-48 * time.Hour),
		}
		store.Store(ctx, oldMsg)

		deleted, err := store.DeleteOlderThan(ctx, 24*time.Hour)
		if err != nil {
			t.Fatalf("DeleteOlderThan failed: %v", err)
		}

		if deleted < 1 {
			t.Errorf("expected at least 1 deleted, got %d", deleted)
		}
	})

	// Test Stats if store implements StatsProvider
	if sp, ok := store.(StatsProvider); ok {
		t.Run("Stats", func(t *testing.T) {
			stats, err := sp.Stats(ctx)
			if err != nil {
				t.Fatalf("Stats failed: %v", err)
			}

			if stats.TotalMessages < 1 {
				t.Errorf("expected at least 1 total message, got %d", stats.TotalMessages)
			}
			if stats.MessagesByEvent == nil {
				t.Error("expected MessagesByEvent to be initialized")
			}
		})
	}

	// Test GetByOriginalID if store implements OriginalIDGetter
	if og, ok := store.(interface {
		GetByOriginalID(ctx context.Context, originalID string) (*Message, error)
	}); ok {
		t.Run("GetByOriginalID", func(t *testing.T) {
			msg, err := og.GetByOriginalID(ctx, "msg-123")
			if err != nil {
				t.Fatalf("GetByOriginalID failed: %v", err)
			}

			if msg.ID != "dlq-int-1" {
				t.Errorf("expected ID dlq-int-1, got %s", msg.ID)
			}
		})
	}
}
