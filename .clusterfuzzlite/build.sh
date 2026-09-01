#!/bin/bash -eu
compile_native_go_fuzzer github.com/rbaliyan/event-dlq FuzzMatchesFilter fuzz_matches_filter
compile_native_go_fuzzer github.com/rbaliyan/event-dlq FuzzNormalizeErrorType fuzz_normalize_error_type
compile_native_go_fuzzer github.com/rbaliyan/event-dlq FuzzStoreAndFilterMessages fuzz_store_and_filter_messages
compile_native_go_fuzzer github.com/rbaliyan/event-dlq FuzzReplayCount fuzz_replay_count
compile_native_go_fuzzer github.com/rbaliyan/event-dlq FuzzTerminalErrorMatching fuzz_terminal_error_matching
compile_native_go_fuzzer github.com/rbaliyan/event-dlq FuzzRedisParseMessage fuzz_redis_parse_message
compile_native_go_fuzzer github.com/rbaliyan/event-dlq FuzzMemoryDedupUpsert fuzz_memory_dedup_upsert
compile_native_go_fuzzer github.com/rbaliyan/event-dlq FuzzMongoDecode fuzz_mongo_decode
compile_native_go_fuzzer github.com/rbaliyan/event-dlq FuzzPostgresDecode fuzz_postgres_decode

# --- Per-input timeout for the byte-oriented decoders ------------------------
# These two targets feed arbitrary bytes into a decoder. libFuzzer's default
# 25s per-input timeout fires on them under AddressSanitizer even though the
# decode work itself is cheap, because the cost sits in the instrumented
# go-118-fuzz-build harness rather than in the code under test. Left at the
# default, the batch job reports "Bug found" on harness overhead and fails.
#
# 90s keeps a genuine hang fatal while leaving ~3x headroom over the worst
# observed run (28s) and staying well inside the per-target fuzzing budget.
# Scalar-argument targets (e.g. FuzzRedisParseMessage) are unaffected and keep
# the stricter default.
for fuzzer in fuzz_mongo_decode fuzz_postgres_decode; do
  cat > "$OUT"/"$fuzzer".options <<EOF
[libfuzzer]
timeout = 90
EOF
done
