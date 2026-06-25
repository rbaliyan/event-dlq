# Fuzzing

This package is fuzzed at two layers:

- **Per-PR (fast):** the `Fuzz` CI matrix runs each target for `-fuzztime=45s`,
  catching crash regressions before merge.
- **Continuous (deep):** the ClusterFuzzLite workflow (`.clusterfuzzlite/`)
  builds the targets with sanitizers and fuzzes them in batch on a schedule,
  persisting and pruning a corpus.

A `Fuzz Target Sync` CI job verifies that every `Fuzz*` function in the source
appears in both the CI matrix and the ClusterFuzzLite `build.sh`, so a new
target can't be silently left un-fuzzed.

## Targets

| Target | Exercises | Key invariant |
|--------|-----------|---------------|
| `FuzzMatchesFilter` | `MemoryStore.matchesFilter` | empty filter matches all; a positive match implies every set field agrees; deterministic |
| `FuzzNormalizeErrorType` | `normalizeErrorType` (metric attribute) | never empty; never turns valid UTF-8 into invalid UTF-8 (rune-split guard) |
| `FuzzStoreAndFilterMessages` | `Store` + `List`/`Count` | `List` honors `Limit`; every result matches the predicate; `len(List) <= Count` |
| `FuzzReplayCount` | `replayCount` (`strconv.Atoi` of `dlq_replay_count`) | never negative; clean integers round-trip |
| `FuzzTerminalErrorMatching` | `TerminalErrorMatching` predicate | matches iff error is non-empty and contains a pattern; empty error never matches |
| `FuzzRedisParseMessage` | `RedisStore.parseMessage` (JSON + int decode of Redis hash data) | never panics; on success the mapped fields, parsed counts, and timestamps round-trip |
| `FuzzMemoryDedupUpsert` | dedup re-store path (`MemoryStore.Store` with dedup on) | second store of the same key collapses to one row: retry_count+1, latest error/payload/metadata, created_at preserved, retried_at cleared; empty OriginalID inserts distinct |

## Running locally

```bash
just fuzz          # fuzz every target for 30s each (auto-discovered)
just fuzz 2m       # override the per-target time
just fuzz-sync-check   # verify the target lists are in sync

# A single target:
go test -run '^$' -fuzz='^FuzzNormalizeErrorType$' -fuzztime=30s .
```

Seed and regression corpora live under `testdata/fuzz/<Target>/` and are
committed so a discovered crasher is replayed on every run.
