//go:build integration

package dlq

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/rbaliyan/event/v3/health"
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

	store, err := NewMongoStore(db, WithCollection("dlq_messages"))
	if err != nil {
		t.Fatalf("NewMongoStore failed: %v", err)
	}
	if err := store.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes failed: %v", err)
	}

	runStoreContractTests(t, store)
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

	store, err := NewMongoStore(db, WithCollection("dlq_capped"))
	if err != nil {
		t.Fatalf("NewMongoStore failed: %v", err)
	}

	// Create as capped collection (1MB, 1000 docs max)
	err = store.CreateCapped(ctx, 1024*1024, 1000)
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

func TestMongoStoreHealth(t *testing.T) {
	client := getMongoClient(t)
	ctx := context.Background()

	dbName := "dlq_health_test_" + time.Now().Format("20060102150405")
	db := client.Database(dbName)
	t.Cleanup(func() {
		db.Drop(context.Background())
	})

	store, err := NewMongoStore(db, WithCollection("dlq_health"))
	if err != nil {
		t.Fatalf("NewMongoStore failed: %v", err)
	}

	t.Run("healthy when connected", func(t *testing.T) {
		result := store.Health(ctx)
		if result.Status != health.StatusHealthy {
			t.Errorf("expected healthy, got %s: %s", result.Status, result.Message)
		}
		if result.Details["collection"] != "dlq_health" {
			t.Errorf("expected collection dlq_health, got %v", result.Details["collection"])
		}
		if result.Latency <= 0 {
			t.Error("expected positive latency")
		}
	})

	t.Run("unhealthy when disconnected", func(t *testing.T) {
		// Create a separate client and disconnect it
		badClient, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:1"))
		if err != nil {
			t.Skipf("could not create bad client: %v", err)
		}
		badDB := badClient.Database("nonexistent")
		badStore, err := NewMongoStore(badDB, WithCollection("bad"))
		if err != nil {
			t.Fatalf("NewMongoStore failed: %v", err)
		}

		tctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		result := badStore.Health(tctx)
		if result.Status != health.StatusUnhealthy {
			t.Errorf("expected unhealthy, got %s", result.Status)
		}
		if result.Message == "" {
			t.Error("expected non-empty message for unhealthy status")
		}
	})
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

	runStoreContractTests(t, store)
}
