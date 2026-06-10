//go:build integration

package dlq

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/rbaliyan/event/v3/health"
	"go.mongodb.org/mongo-driver/v2/bson"
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

func TestMongoStoreDedup(t *testing.T) {
	client := getMongoClient(t)
	ctx := context.Background()

	dbName := "dlq_dedup_test_" + time.Now().Format("20060102150405")
	db := client.Database(dbName)
	t.Cleanup(func() { db.Drop(context.Background()) })

	// dedup OFF => two distinct docs
	off, err := NewMongoStore(db, WithCollection("dlq_dedup_off_test"))
	if err != nil {
		t.Fatalf("NewMongoStore (off): %v", err)
	}
	_ = off.Collection().Drop(ctx)
	t.Cleanup(func() { _ = off.Collection().Drop(ctx) })
	// Ensure index bootstrap has run so the assertion is not a race: with dedup
	// OFF there must be NO unique index, so duplicate (event,original) inserts
	// are allowed even after EnsureIndexes completes.
	if err := off.EnsureIndexes(ctx); err != nil {
		t.Fatalf("ensure indexes: %v", err)
	}
	_ = off.Store(ctx, &Message{ID: "a", EventName: "e", OriginalID: "o1", CreatedAt: time.Now()})
	if err := off.Store(ctx, &Message{ID: "b", EventName: "e", OriginalID: "o1", CreatedAt: time.Now()}); err != nil {
		t.Fatalf("dedup off: second insert of same (event,original) must succeed, got: %v", err)
	}
	if n, _ := off.Count(ctx, Filter{}); n != 2 {
		t.Fatalf("dedup off: want 2, got %d", n)
	}

	// dedup ON => upsert
	on, err := NewMongoStore(db, WithCollection("dlq_dedup_on_test"), WithMongoDedup())
	if err != nil {
		t.Fatalf("NewMongoStore (on): %v", err)
	}
	_ = on.Collection().Drop(ctx)
	t.Cleanup(func() { _ = on.Collection().Drop(ctx) })
	if err := on.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	first := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	_ = on.Store(ctx, &Message{ID: "a", EventName: "e", OriginalID: "o1", Error: "err1", RetryCount: 3, CreatedAt: first})
	_ = on.Store(ctx, &Message{ID: "b", EventName: "e", OriginalID: "o1", Error: "err2", RetryCount: 0, CreatedAt: time.Now()})
	if n, _ := on.Count(ctx, Filter{}); n != 1 {
		t.Fatalf("dedup on: want 1, got %d", n)
	}
	list, err := on.List(ctx, Filter{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) == 0 {
		t.Fatal("list returned no messages")
	}
	g := list[0]
	if g.RetryCount != 4 {
		t.Fatalf("retry_count: want 4, got %d", g.RetryCount)
	}
	if g.Error != "err2" {
		t.Fatalf("error latest: want err2, got %q", g.Error)
	}
	if !g.CreatedAt.UTC().Equal(first.UTC()) {
		t.Fatalf("created_at preserved: want %v, got %v", first.UTC(), g.CreatedAt.UTC())
	}
	if g.RetriedAt != nil {
		t.Fatal("retried_at must be nil after upsert")
	}

	// empty OriginalID => distinct under dedup
	on2, err := NewMongoStore(db, WithCollection("dlq_dedup_empty_test"), WithMongoDedup())
	if err != nil {
		t.Fatalf("NewMongoStore (on2): %v", err)
	}
	_ = on2.Collection().Drop(ctx)
	t.Cleanup(func() { _ = on2.Collection().Drop(ctx) })
	_ = on2.Store(ctx, &Message{ID: "a", EventName: "e", OriginalID: "", CreatedAt: time.Now()})
	_ = on2.Store(ctx, &Message{ID: "b", EventName: "e", OriginalID: "", CreatedAt: time.Now()})
	if n, _ := on2.Count(ctx, Filter{}); n != 2 {
		t.Fatalf("empty original under dedup: want 2, got %d", n)
	}
}

func TestMongoStoreMigrateDedup(t *testing.T) {
	client := getMongoClient(t)
	ctx := context.Background()

	dbName := "dlq_migrate_test_" + time.Now().Format("20060102150405")
	db := client.Database(dbName)
	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	store, err := NewMongoStore(db, WithCollection("dlq_migrate_test"), WithMongoDedup())
	if err != nil {
		t.Fatalf("NewMongoStore: %v", err)
	}
	coll := store.Collection()
	_ = coll.Drop(ctx)
	t.Cleanup(func() { _ = coll.Drop(ctx) })

	// Seed 3 duplicate docs for (e, o1) with distinct _ids, plus 1 unique (e, o2).
	// Insert directly via the collection to bypass the dedup upsert path.
	old := time.Now().Add(-2 * time.Hour).UTC().Truncate(time.Millisecond)
	mid := time.Now().Add(-1 * time.Hour).UTC().Truncate(time.Millisecond)
	newest := time.Now().UTC().Truncate(time.Millisecond)
	docs := []interface{}{
		bson.M{"_id": "d1", "event_name": "e", "original_id": "o1", "error": "e1", "retry_count": 1, "created_at": mid},
		bson.M{"_id": "d2", "event_name": "e", "original_id": "o1", "error": "e2", "retry_count": 2, "created_at": old},
		bson.M{"_id": "d3", "event_name": "e", "original_id": "o1", "error": "e3", "retry_count": 3, "created_at": newest},
		bson.M{"_id": "u1", "event_name": "e", "original_id": "o2", "error": "x", "retry_count": 5, "created_at": newest},
	}
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Without force, deleting 2 of 4 rows (50%) is allowed (guard triggers only at >50%).
	removed, err := store.MigrateDedup(ctx)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if removed != 2 {
		t.Fatalf("want 2 removed, got %d", removed)
	}

	// One survivor for (e,o1): newest doc (d3) kept, retry_count summed = 6, created_at = oldest.
	survivor, err := store.GetByOriginalID(ctx, "o1")
	if err != nil {
		t.Fatalf("get survivor: %v", err)
	}
	if survivor.ID != "d3" {
		t.Fatalf("expected newest doc d3 kept, got %s", survivor.ID)
	}
	if survivor.RetryCount != 6 {
		t.Fatalf("expected summed retry_count 6, got %d", survivor.RetryCount)
	}
	if !survivor.CreatedAt.UTC().Equal(old) {
		t.Fatalf("expected oldest created_at %v, got %v", old, survivor.CreatedAt.UTC())
	}

	if n, _ := store.Count(ctx, Filter{}); n != 2 {
		t.Fatalf("expected 2 docs total after migrate, got %d", n)
	}

	// Unique index now present: a raw duplicate insert for (e,o1) must fail.
	_, err = coll.InsertOne(ctx, bson.M{"_id": "dup", "event_name": "e", "original_id": "o1", "created_at": newest})
	if err == nil {
		t.Fatal("expected duplicate-key error after unique index created")
	}
}

func TestMongoStoreMigrateDedup_ForceGuard(t *testing.T) {
	client := getMongoClient(t)
	ctx := context.Background()

	dbName := "dlq_migrate_guard_test_" + time.Now().Format("20060102150405")
	db := client.Database(dbName)
	t.Cleanup(func() { _ = db.Drop(context.Background()) })

	store, err := NewMongoStore(db, WithCollection("dlq_migrate_guard_test"), WithMongoDedup())
	if err != nil {
		t.Fatalf("NewMongoStore: %v", err)
	}
	coll := store.Collection()
	_ = coll.Drop(ctx)
	t.Cleanup(func() { _ = coll.Drop(ctx) })

	// 4 docs all same (e,o1): collapsing deletes 3 of 4 = 75% > 50% => guarded.
	var docs []interface{}
	for i := 0; i < 4; i++ {
		docs = append(docs, bson.M{"_id": fmt.Sprintf("d%d", i), "event_name": "e", "original_id": "o1", "retry_count": 1, "created_at": time.Now().UTC()})
	}
	_, _ = coll.InsertMany(ctx, docs)

	if _, err := store.MigrateDedup(ctx); err == nil {
		t.Fatal("expected guard error when >50% would be deleted")
	}
	if _, err := store.MigrateDedup(ctx, WithForce()); err != nil {
		t.Fatalf("with force: %v", err)
	}
	if n, _ := store.Count(ctx, Filter{}); n != 1 {
		t.Fatalf("after forced migrate want 1, got %d", n)
	}
}

func TestMongoStoreQuarantine(t *testing.T) {
	client := getMongoClient(t)
	ctx := context.Background()

	dbName := "dlq_quarantine_test_" + time.Now().Format("20060102150405")
	db := client.Database(dbName)
	t.Cleanup(func() {
		db.Drop(context.Background())
	})

	store, err := NewMongoStore(db, WithCollection("dlq_quarantine_test"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.EnsureIndexes(ctx); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}

	now := time.Now()
	if err := store.Store(ctx, &Message{ID: "m1", EventName: "e", OriginalID: "o1", CreatedAt: now}); err != nil {
		t.Fatalf("store: %v", err)
	}

	// Quarantine the message and verify QuarantinedAt is set.
	if err := store.Quarantine(ctx, "m1"); err != nil {
		t.Fatalf("quarantine: %v", err)
	}
	got, err := store.Get(ctx, "m1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.QuarantinedAt == nil {
		t.Fatal("expected QuarantinedAt set after Quarantine")
	}

	// ExcludeQuarantined must hide the quarantined message.
	list, err := store.List(ctx, Filter{ExcludeQuarantined: true})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for _, m := range list {
		if m.ID == "m1" {
			t.Fatal("ExcludeQuarantined must hide quarantined message")
		}
	}

	// Quarantining a missing ID must return an error.
	if err := store.Quarantine(ctx, "missing"); err == nil {
		t.Fatal("expected error quarantining missing message")
	}
}

func TestPostgresStoreQuarantine(t *testing.T) {
	db := getPostgresDB(t)
	ctx := context.Background()
	store, err := NewPostgresStore(db, WithTable("dlq_pg_quarantine_test"))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS dlq_pg_quarantine_test")
	if err := store.EnsureTable(ctx); err != nil {
		t.Fatalf("ensure table: %v", err)
	}
	t.Cleanup(func() { _, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS dlq_pg_quarantine_test") })

	now := time.Now()
	if err := store.Store(ctx, &Message{ID: "m1", EventName: "e", OriginalID: "o1", Payload: []byte("{}"), Error: "x", CreatedAt: now}); err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := store.Quarantine(ctx, "m1"); err != nil {
		t.Fatalf("quarantine: %v", err)
	}
	got, err := store.Get(ctx, "m1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.QuarantinedAt == nil {
		t.Fatal("expected QuarantinedAt set")
	}
	list, _ := store.List(ctx, Filter{ExcludeQuarantined: true})
	for _, m := range list {
		if m.ID == "m1" {
			t.Fatal("ExcludeQuarantined must hide quarantined message")
		}
	}
	if err := store.Quarantine(ctx, "missing"); err == nil {
		t.Fatal("expected error quarantining missing id")
	}

	// Long original_id (former VARCHAR(36) would have rejected this ~140-char value)
	longID := "826A100506000000112B042C0100296E5A100475D36D2326024308B40C3D05BFEF1AE7463C6F7065726174696F6E54797065003C7570646174650046646F63756D656E744B6579"
	if err := store.Store(ctx, &Message{ID: "m2", EventName: "e", OriginalID: longID, Payload: []byte("{}"), Error: "x", CreatedAt: now}); err != nil {
		t.Fatalf("store long original_id (TEXT widen): %v", err)
	}
}
