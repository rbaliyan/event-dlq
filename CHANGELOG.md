# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Summary

All changes are **additive and off by default**. Existing code that does not opt in to the new options retains identical behaviour. No breaking changes to the `Store` interface, `Manager`, or any store constructor.

### Added

#### Max replay attempts (error-agnostic loop cap)

- `WithMaxReplayAttempts(n int) ManagerOption` — caps how many times a single message may be replayed before it is quarantined, **regardless of why it fails**. Default `0` = unlimited (prior behaviour). The generic backstop against a permanently-failing message (decode error, a future type change, a bad payload) looping forever through republish → re-DLQ → republish.
- The replay count rides in message metadata under the exported key `MetadataReplayCount` (`dlq_replay_count`); `replayMessage` increments it on each republish, so it accumulates across the republish→re-DLQ cycle **without** requiring dedup or a stable DB row.
- Prefer this over `WithTerminalError` for guaranteed termination — it does not depend on matching error strings. The two compose; the quarantine reason (`max_replay_attempts` vs `terminal_error`) is logged.
- Recommended for any auto-replay loop (a small value such as 3–5).

#### Terminal-error quarantine

- `WithTerminalError(func(*Message) bool) ManagerOption` — attach a predicate to the Manager. During `Replay` and `ReplaySingle`, messages for which the predicate returns true are quarantined instead of republished, preventing poison-message replay loops.
- `TerminalErrorMatching(patterns ...string) func(*Message) bool` — convenience predicate; performs a case-sensitive substring match against `Message.Error`; an empty error never matches.
- `Quarantiner` optional interface (`Quarantine(ctx, id) error`), implemented by all four stores (`MemoryStore`, `MongoStore`, `PostgresStore`, `RedisStore`). Detected via type assertion, the same pattern as `StatsProvider`; existing `Store` implementations do not need to change.
- `Message.QuarantinedAt *time.Time` — set by `Quarantine`; nil for non-quarantined messages.
- `Filter.ExcludeQuarantined bool` — exclude quarantined messages from `List`/`Count`. `Manager.Replay` sets this flag internally so quarantined messages are never re-evaluated.
- `Stats.QuarantinedMessages int64` — count of quarantined messages; `Stats.PendingMessages` now excludes quarantined rows so the two counts reconcile with `TotalMessages`.
- OTel counter `dlq_messages_quarantined_total{event}` increments on each quarantine.

#### Opt-in deduplication

- `WithMemoryDedup() MemoryStoreOption` — enable upsert-on-(EventName, OriginalID) in the in-memory store.
- `WithMongoDedup() MongoStoreOption` — enable dedup in `MongoStore`; a unique index on `(event_name, original_id)` is created in `EnsureIndexes` (failure-tolerant; see migration note below).
- `WithPostgresDedup() PostgresStoreOption` — enable dedup in `PostgresStore`; a unique index on `(event_name, original_id)` is created in `EnsureTable` (failure-tolerant). Falls back to a `SELECT FOR UPDATE` upsert if the constraint is not yet present, so upgrades never cause write outages.
- `WithRedisDedup() RedisStoreOption` — enable dedup in `RedisStore` via a Lua-based composite-key upsert.
- When dedup is enabled: re-storing a message with the same non-empty `(EventName, OriginalID)` pair upserts — increments `retry_count`, updates error/payload/metadata to the latest values, preserves `created_at` (first-seen timestamp), and clears `retried_at`. Messages with an empty `OriginalID` always insert as distinct rows.
- When dedup is **disabled** (the default): behaviour is identical to previous releases — every `Store` call inserts a distinct row.

#### Historical-duplicate migration

- `MigrateDedup(ctx, ...MigrateDedupOption) (int64, error)` on `MongoStore`, `PostgresStore`, and `RedisStore`. Collapses existing duplicate `(event_name, original_id)` groups: keeps the newest row, sums `retry_count`, preserves the oldest `created_at`, OR-s the quarantine flag. Then creates the unique index. Postgres uses `CREATE UNIQUE INDEX CONCURRENTLY` to avoid table locks.
- `MigrateDedupOption` / `WithForce() MigrateDedupOption` — bypass the safety guard that refuses to delete more than 50 % of all rows; use this only after verifying the target collection/table is correct.
- `MigrateDedup` is idempotent: a second call on an already-clean store is a no-op.

### Migration notes

**Operators enabling dedup on a pre-existing table or collection must run `MigrateDedup` before (or immediately after) enabling the option.** If legacy duplicates are present when `With*Dedup()` is first used, the unique index creation is skipped with a warning and upsert semantics will not fully apply until the index exists.

Recommended deployment sequence:

1. Deploy the new binary with `With*Dedup()` enabled in the store constructor.
2. Call `store.MigrateDedup(ctx)` once during startup or via a one-off migration job. Pass `WithForce()` if more than half of the existing rows are duplicates and you have verified the correct store is targeted.
3. Confirm the logged output reports the expected number of collapsed rows.
4. The unique index is now in place; subsequent `Store` calls upsert correctly.

**Stopping an active poison-replay loop (operational runbook):**

1. Disable the scheduled `Replay` job to halt further republishing.
2. Identify and remove the poison rows using `DeleteByFilter` or a direct query scoped to the known error pattern.
3. Fix the upstream decode or schema error that caused the permanent failures.
4. Re-deploy with `WithTerminalError` wired into the Manager.
5. Re-enable the `Replay` job.

[Unreleased]: https://github.com/rbaliyan/event-dlq/compare/HEAD...HEAD
