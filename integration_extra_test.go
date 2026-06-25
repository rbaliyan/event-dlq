//go:build integration

package dlq

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/health"
	"github.com/redis/go-redis/v9"
)

// getRedisClient connects to a real Redis server for integration tests, skipping
// cleanly when none is reachable. Set REDIS_URI to override the default address.
func getRedisClient(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("REDIS_URI")
	if addr == "" {
		addr = "localhost:6379"
	}

	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		t.Skipf("Redis not available at %s: %v", addr, err)
	}
	// Start from a clean keyspace so the contract's seed counts are exact.
	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("FlushDB: %v", err)
	}
	t.Cleanup(func() {
		_ = client.FlushDB(context.Background()).Err()
		_ = client.Close()
	})
	return client
}

// newRealRedisStore builds a RedisStore against a real server.
func newRealRedisStore(t *testing.T) *RedisStore {
	t.Helper()
	store, err := NewRedisStore(getRedisClient(t))
	if err != nil {
		t.Fatalf("NewRedisStore: %v", err)
	}
	return store
}

// TestRedisStoreIntegration runs the full Store contract against a real Redis
// server (the default unit lane only exercises an in-process miniredis mock,
// which does not implement every Stream/Lua semantic).
func TestRedisStoreIntegration(t *testing.T) {
	runStoreContractTests(t, newRealRedisStore(t))
}

// uniqueName returns a collision-free table/collection name for a test.
func uniqueName(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
}

// newPostgresDedupStore returns a dedup-enabled PostgresStore on a fresh table.
func newPostgresDedupStore(t *testing.T) *PostgresStore {
	t.Helper()
	db := getPostgresDB(t)
	table := uniqueName("dlq_pg")
	store, err := NewPostgresStore(db, WithTable(table), WithPostgresDedup())
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	if err := store.EnsureTable(context.Background()); err != nil {
		t.Fatalf("EnsureTable: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec("DROP TABLE IF EXISTS " + table) })
	return store
}

// newMongoDedupStore returns a dedup-enabled MongoStore on a fresh collection.
func newMongoDedupStore(t *testing.T) *MongoStore {
	t.Helper()
	client := getMongoClient(t)
	db := client.Database(uniqueName("dlq_mongo"))
	t.Cleanup(func() { _ = db.Drop(context.Background()) })
	store, err := NewMongoStore(db, WithCollection("dlq_messages"), WithMongoDedup())
	if err != nil {
		t.Fatalf("NewMongoStore: %v", err)
	}
	if err := store.EnsureIndexes(context.Background()); err != nil {
		t.Fatalf("EnsureIndexes: %v", err)
	}
	return store
}

// TestConcurrentDedupUpsert verifies the unique-index-backed dedup upsert is safe
// under contention: N goroutines storing the SAME (event_name, original_id)
// concurrently must collapse to exactly one row whose retry_count reflects every
// store (no lost updates, no spurious duplicate-key failure). This is the
// scenario the unique index exists to protect.
func TestConcurrentDedupUpsert(t *testing.T) {
	const writers = 10

	backends := map[string]func(t *testing.T) Store{
		"postgres": func(t *testing.T) Store { return newPostgresDedupStore(t) },
		"mongo":    func(t *testing.T) Store { return newMongoDedupStore(t) },
	}

	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := mk(t)

			var wg sync.WaitGroup
			errs := make(chan error, writers)
			for i := range writers {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					// Distinct DLQ IDs, identical (event, original) dedup key,
					// each carrying RetryCount 1 so the surviving row's count
					// equals the writer count (insert=1, then +1 per update).
					errs <- store.Store(ctx, &Message{
						ID:         fmt.Sprintf("d-%d", i),
						EventName:  "order.created",
						OriginalID: "orig-shared",
						Payload:    []byte(`{}`),
						Error:      "boom",
						RetryCount: 1,
						CreatedAt:  time.Now(),
					})
				}(i)
			}
			wg.Wait()
			close(errs)
			failed := 0
			for err := range errs {
				if err != nil {
					failed++
					t.Errorf("concurrent Store failed: %v", err)
				}
			}
			if failed > 0 {
				t.Fatalf("%d/%d concurrent stores failed", failed, writers)
			}

			count, err := store.Count(ctx, Filter{})
			if err != nil {
				t.Fatalf("Count: %v", err)
			}
			if count != 1 {
				t.Fatalf("dedup under contention: want exactly 1 row, got %d", count)
			}

			list, err := store.List(ctx, Filter{})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if len(list) != 1 {
				t.Fatalf("List: want 1 message, got %d", len(list))
			}
			if list[0].RetryCount != writers {
				t.Fatalf("retry_count under contention: want %d (no lost updates), got %d", writers, list[0].RetryCount)
			}
		})
	}
}

// TestConcurrentMarkRetried verifies that many goroutines marking the same
// message retried at once is safe (no panic, no race, idempotent end state) on
// each real backend.
func TestConcurrentMarkRetried(t *testing.T) {
	const workers = 12

	backends := map[string]func(t *testing.T) Store{
		"postgres": func(t *testing.T) Store { return newPostgresDedupStore(t) },
		"mongo":    func(t *testing.T) Store { return newMongoDedupStore(t) },
		"redis":    func(t *testing.T) Store { return newRealRedisStore(t) },
	}

	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := mk(t)
			require := func(err error, msg string) {
				if err != nil {
					t.Fatalf("%s: %v", msg, err)
				}
			}
			require(store.Store(ctx, &Message{
				ID: "m1", EventName: "e", OriginalID: "o1", Payload: []byte(`{}`), CreatedAt: time.Now(),
			}), "seed")

			var wg sync.WaitGroup
			for range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = store.MarkRetried(ctx, "m1")
				}()
			}
			wg.Wait()

			got, err := store.Get(ctx, "m1")
			require(err, "get")
			if got.RetriedAt == nil {
				t.Fatal("message must be marked retried after concurrent MarkRetried")
			}
		})
	}
}

// TestPostgresStore_HealthUnhealthy asserts the Postgres store reports
// StatusUnhealthy when the backend is unreachable, mirroring the Mongo and Redis
// unhealthy-path coverage.
func TestPostgresStore_HealthUnhealthy(t *testing.T) {
	// Point at a closed port with a short connect timeout; sql.Open does not
	// dial, so the failure surfaces when Health issues its Count query.
	db, err := sql.Open("postgres", "postgres://127.0.0.1:1/none?sslmode=disable&connect_timeout=2")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store, err := NewPostgresStore(db, WithTable("dlq_unhealthy"))
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}

	res := store.Health(context.Background())
	if res == nil {
		t.Fatal("Health returned nil")
	}
	if res.Status != health.StatusUnhealthy {
		t.Fatalf("want StatusUnhealthy for unreachable backend, got %q (%s)", res.Status, res.Message)
	}
}

// TestStore_ContextCancellation verifies that operations against a live backend
// honor an already-canceled context instead of blocking or silently succeeding.
func TestStore_ContextCancellation(t *testing.T) {
	backends := map[string]func(t *testing.T) Store{
		"postgres": func(t *testing.T) Store { return newPostgresDedupStore(t) },
		"mongo":    func(t *testing.T) Store { return newMongoDedupStore(t) },
	}

	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			store := mk(t)
			canceled, cancel := context.WithCancel(context.Background())
			cancel()

			if _, err := store.List(canceled, Filter{}); err == nil {
				t.Error("List with canceled context: want error, got nil")
			}
			if _, err := store.Count(canceled, Filter{}); err == nil {
				t.Error("Count with canceled context: want error, got nil")
			}
			if err := store.Store(canceled, &Message{ID: "x", EventName: "e", OriginalID: "o", CreatedAt: time.Now()}); err == nil {
				t.Error("Store with canceled context: want error, got nil")
			}
			if err := store.MarkRetried(canceled, "x"); err == nil {
				t.Error("MarkRetried with canceled context: want error, got nil")
			}
			if err := store.Delete(canceled, "x"); err == nil {
				t.Error("Delete with canceled context: want error, got nil")
			}
			if _, err := store.DeleteByFilter(canceled, Filter{EventName: "e"}); err == nil {
				t.Error("DeleteByFilter with canceled context: want error, got nil")
			}
		})
	}
}

// TestManager_Replay_RealBackend runs the headline Manager flows against real
// Postgres and Mongo: a successful replay republishes with DLQ metadata and marks
// the row retried, and the WithMaxReplayAttempts cap persists QuarantinedAt in the
// backend row.
func TestManager_Replay_RealBackend(t *testing.T) {
	backends := map[string]func(t *testing.T) Store{
		"postgres": func(t *testing.T) Store { return newPostgresDedupStore(t) },
		"mongo":    func(t *testing.T) Store { return newMongoDedupStore(t) },
	}

	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()

			t.Run("ReplayMarksRetriedWithMetadata", func(t *testing.T) {
				store := mk(t)
				if err := store.Store(ctx, &Message{
					ID: "d1", EventName: "order.created", OriginalID: "o1",
					Payload: []byte(`{}`), Error: "boom", CreatedAt: time.Now(),
				}); err != nil {
					t.Fatalf("seed Store: %v", err)
				}

				rep := &capturingRepublisher{}
				mgr, err := NewManager(store, rep)
				if err != nil {
					t.Fatalf("NewManager: %v", err)
				}

				n, err := mgr.Replay(ctx, Filter{})
				if err != nil {
					t.Fatalf("Replay: %v", err)
				}
				if n != 1 {
					t.Fatalf("replayed: want 1, got %d", n)
				}
				if len(rep.sends) != 1 || rep.sends[0]["dlq_replay"] != "true" || rep.sends[0]["dlq_message_id"] != "d1" {
					t.Fatalf("replay metadata not attached: %+v", rep.sends)
				}

				got, err := store.Get(ctx, "d1")
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if got.RetriedAt == nil {
					t.Fatal("replayed message must have RetriedAt persisted")
				}
			})

			t.Run("MaxReplayAttemptsQuarantinesInBackend", func(t *testing.T) {
				store := mk(t)
				if err := store.Store(ctx, &Message{
					ID: "d2", EventName: "order.created", OriginalID: "o2",
					Payload: []byte(`{}`), Error: "boom", CreatedAt: time.Now(),
					Metadata: map[string]string{MetadataReplayCount: "3"},
				}); err != nil {
					t.Fatalf("seed Store: %v", err)
				}

				rep := &capturingRepublisher{}
				mgr, err := NewManager(store, rep, WithMaxReplayAttempts(3))
				if err != nil {
					t.Fatalf("NewManager: %v", err)
				}

				n, err := mgr.Replay(ctx, Filter{})
				if err != nil {
					t.Fatalf("Replay: %v", err)
				}
				if n != 0 {
					t.Fatalf("a capped message must not be replayed, got %d", n)
				}
				if len(rep.sends) != 0 {
					t.Fatalf("a capped message must not be republished, got %d sends", len(rep.sends))
				}

				got, err := store.Get(ctx, "d2")
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if got.QuarantinedAt == nil {
					t.Fatal("capped message must have QuarantinedAt persisted in the backend")
				}
			})

			t.Run("ReplaySingleMarksRetried", func(t *testing.T) {
				store := mk(t)
				if err := store.Store(ctx, &Message{
					ID: "s1", EventName: "order.created", OriginalID: "so1",
					Payload: []byte(`{}`), Error: "boom", CreatedAt: time.Now(),
				}); err != nil {
					t.Fatalf("seed Store: %v", err)
				}
				mgr, err := NewManager(store, &capturingRepublisher{})
				if err != nil {
					t.Fatalf("NewManager: %v", err)
				}
				if err := mgr.ReplaySingle(ctx, "s1"); err != nil {
					t.Fatalf("ReplaySingle: %v", err)
				}
				got, err := store.Get(ctx, "s1")
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if got.RetriedAt == nil {
					t.Fatal("ReplaySingle must persist RetriedAt")
				}
			})

			t.Run("TransportFailureLeavesUnretriedAndReplayable", func(t *testing.T) {
				store := mk(t)
				if err := store.Store(ctx, &Message{
					ID: "f1", EventName: "order.created", OriginalID: "fo1",
					Payload: []byte(`{}`), Error: "boom", CreatedAt: time.Now(),
				}); err != nil {
					t.Fatalf("seed Store: %v", err)
				}
				// Republish always fails: the row must NOT be marked retried, so a
				// later sweep can pick it up again (no partial state corruption).
				mgr, err := NewManager(store, &failingRepublisher{})
				if err != nil {
					t.Fatalf("NewManager: %v", err)
				}
				n, err := mgr.Replay(ctx, Filter{})
				if err != nil {
					t.Fatalf("Replay: %v", err)
				}
				if n != 0 {
					t.Fatalf("failed republish must not count as replayed, got %d", n)
				}
				got, err := store.Get(ctx, "f1")
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if got.RetriedAt != nil {
					t.Fatal("a failed replay must leave RetriedAt nil so the message stays replayable")
				}
				pending, err := store.List(ctx, Filter{ExcludeRetried: true})
				if err != nil {
					t.Fatalf("List: %v", err)
				}
				if len(pending) != 1 {
					t.Fatalf("message must remain pending after failed replay, got %d", len(pending))
				}
			})

			t.Run("CleanupRemovesOld", func(t *testing.T) {
				store := mk(t)
				old := &Message{ID: "old1", EventName: "e", OriginalID: "oo1", Payload: []byte(`{}`), CreatedAt: time.Now().Add(-48 * time.Hour)}
				recent := &Message{ID: "new1", EventName: "e", OriginalID: "on1", Payload: []byte(`{}`), CreatedAt: time.Now()}
				if err := store.Store(ctx, old); err != nil {
					t.Fatalf("seed old: %v", err)
				}
				if err := store.Store(ctx, recent); err != nil {
					t.Fatalf("seed recent: %v", err)
				}
				mgr, err := NewManager(store, &capturingRepublisher{})
				if err != nil {
					t.Fatalf("NewManager: %v", err)
				}
				deleted, err := mgr.Cleanup(ctx, 24*time.Hour)
				if err != nil {
					t.Fatalf("Cleanup: %v", err)
				}
				if deleted < 1 {
					t.Fatalf("Cleanup must remove the 48h-old message, deleted=%d", deleted)
				}
				if _, err := store.Get(ctx, "new1"); err != nil {
					t.Fatalf("recent message must survive cleanup: %v", err)
				}
			})

			t.Run("StatsReflectsState", func(t *testing.T) {
				store := mk(t)
				for _, m := range []*Message{
					{ID: "st1", EventName: "order.created", OriginalID: "sto1", Payload: []byte(`{}`), CreatedAt: time.Now()},
					{ID: "st2", EventName: "order.created", OriginalID: "sto2", Payload: []byte(`{}`), CreatedAt: time.Now()},
					{ID: "st3", EventName: "payment.failed", OriginalID: "sto3", Payload: []byte(`{}`), CreatedAt: time.Now()},
				} {
					if err := store.Store(ctx, m); err != nil {
						t.Fatalf("seed Store: %v", err)
					}
				}
				if err := store.MarkRetried(ctx, "st1"); err != nil {
					t.Fatalf("MarkRetried: %v", err)
				}
				mgr, err := NewManager(store, &capturingRepublisher{})
				if err != nil {
					t.Fatalf("NewManager: %v", err)
				}
				stats, err := mgr.Stats(ctx)
				if err != nil {
					t.Fatalf("Stats: %v", err)
				}
				if stats.TotalMessages != 3 {
					t.Fatalf("TotalMessages: want 3, got %d", stats.TotalMessages)
				}
				if stats.RetriedMessages != 1 {
					t.Fatalf("RetriedMessages: want 1, got %d", stats.RetriedMessages)
				}
			})

			t.Run("TerminalErrorQuarantines", func(t *testing.T) {
				store := mk(t)
				if err := store.Store(ctx, &Message{
					ID: "t1", EventName: "order.created", OriginalID: "to1",
					Payload: []byte(`{}`), Error: "fatal: schema removed", CreatedAt: time.Now(),
				}); err != nil {
					t.Fatalf("seed Store: %v", err)
				}
				mgr, err := NewManager(store, &capturingRepublisher{},
					WithTerminalError(TerminalErrorMatching("fatal:")))
				if err != nil {
					t.Fatalf("NewManager: %v", err)
				}
				n, err := mgr.Replay(ctx, Filter{})
				if err != nil {
					t.Fatalf("Replay: %v", err)
				}
				if n != 0 {
					t.Fatalf("terminal message must not be replayed, got %d", n)
				}
				got, err := store.Get(ctx, "t1")
				if err != nil {
					t.Fatalf("Get: %v", err)
				}
				if got.QuarantinedAt == nil {
					t.Fatal("terminal-error message must have QuarantinedAt persisted")
				}
			})
		})
	}
}

// TestManager_ConcurrentReplayAndStore_RealBackend exercises a replayer running
// concurrently with producers writing new failures, asserting the backend stays
// consistent (no error, no lost messages: every message is either retried or
// still pending) under contention.
func TestManager_ConcurrentReplayAndStore_RealBackend(t *testing.T) {
	const producers = 8

	backends := map[string]func(t *testing.T) Store{
		"postgres": func(t *testing.T) Store { return newPostgresDedupStore(t) },
		"mongo":    func(t *testing.T) Store { return newMongoDedupStore(t) },
	}

	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := mk(t)
			mgr, err := NewManager(store, &capturingRepublisher{})
			if err != nil {
				t.Fatalf("NewManager: %v", err)
			}

			var wg sync.WaitGroup
			// Producers store distinct messages.
			for i := range producers {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					_ = store.Store(ctx, &Message{
						ID:         fmt.Sprintf("c-%d", i),
						EventName:  "order.created",
						OriginalID: fmt.Sprintf("co-%d", i),
						Payload:    []byte(`{}`),
						CreatedAt:  time.Now(),
					})
				}(i)
			}
			// A concurrent replayer sweeps while producers write.
			wg.Add(1)
			go func() {
				defer wg.Done()
				for range 5 {
					if _, err := mgr.Replay(ctx, Filter{}); err != nil {
						t.Errorf("concurrent Replay failed: %v", err)
						return
					}
				}
			}()
			wg.Wait()

			// Final sweep to drain anything stored after the last concurrent sweep.
			if _, err := mgr.Replay(ctx, Filter{}); err != nil {
				t.Fatalf("final Replay: %v", err)
			}

			// Every produced message must be accounted for: retried or pending,
			// never lost or duplicated.
			total, err := store.Count(ctx, Filter{})
			if err != nil {
				t.Fatalf("Count: %v", err)
			}
			if total != producers {
				t.Fatalf("message accounting: want %d rows, got %d", producers, total)
			}
		})
	}
}

// TestManager_Metrics_RealBackend validates that OpenTelemetry instrumentation is
// driven end-to-end on a real backend: storing a message moves the pending gauge
// up and a successful replay moves it back down. Uses the in-test ManualReader
// helpers from metrics_test.go.
func TestManager_Metrics_RealBackend(t *testing.T) {
	backends := map[string]func(t *testing.T) Store{
		"postgres": func(t *testing.T) Store { return newPostgresDedupStore(t) },
		"mongo":    func(t *testing.T) Store { return newMongoDedupStore(t) },
	}

	for name, mk := range backends {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			store := mk(t)
			m, reader := newTestMetrics(t)
			mgr, err := NewManager(store, &capturingRepublisher{}, WithMetrics(m))
			if err != nil {
				t.Fatalf("NewManager: %v", err)
			}

			if err := mgr.Store(ctx, StoreParams{EventName: "order.created", OriginalID: "mo1"}); err != nil {
				t.Fatalf("Store: %v", err)
			}
			if got := readPending(t, reader); got != 1 {
				t.Fatalf("pending gauge after Store = %d, want 1", got)
			}

			n, err := mgr.Replay(ctx, Filter{})
			if err != nil {
				t.Fatalf("Replay: %v", err)
			}
			if n != 1 {
				t.Fatalf("replayed = %d, want 1", n)
			}
			if got := readPending(t, reader); got != 0 {
				t.Fatalf("pending gauge after Replay = %d, want 0", got)
			}
		})
	}
}
