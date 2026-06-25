# Default recipe
default:
    @just --list

# Build all packages
build:
    go build ./...

# Run tests
test:
    go test ./...

# Run tests with verbose output
test-v:
    go test -v ./...

# Run tests with race detector
test-race:
    go test -race ./...

# Run fast hermetic smoke tests (critical-path sanity, no external services)
smoke:
    go test -run 'TestSmoke|Contract' -race ./...

# Run all benchmarks (executes them and reports allocations)
bench:
    go test -run '^$' -bench=. -benchmem ./...

# Run benchmarks repeatedly so `benchstat` can assess variance/significance.
# Local only (shared CI runners are too noisy to gate on timing):
#   just bench-stat 10 | tee new.txt && benchstat old.txt new.txt
bench-stat count="10":
    go test -run '^$' -bench=. -benchmem -count={{count}} ./...

# Fuzz every target briefly; override the per-target time with `just fuzz 1m`.
# Targets are discovered from the source so this never drifts.
fuzz time="30s":
    #!/usr/bin/env bash
    set -euo pipefail
    for t in $(grep -ohE '^func (Fuzz[A-Za-z0-9_]+)' *_test.go | awk '{print $2}'); do
        echo "== fuzzing $t =="
        go test -run '^$' -fuzz="^$t\$" -fuzztime={{time}} .
    done

# Verify the fuzz target lists in CI and ClusterFuzzLite match the source.
fuzz-sync-check:
    #!/usr/bin/env bash
    set -euo pipefail
    src=$(grep -ohE '^func (Fuzz[A-Za-z0-9_]+)' *_test.go | awk '{print $2}' | sort -u)
    ci=$(grep -oE '^[[:space:]]*- Fuzz[A-Za-z0-9_]+' .github/workflows/ci.yml | grep -oE 'Fuzz[A-Za-z0-9_]+' | sort -u)
    bsh=$(grep -oE ' Fuzz[A-Za-z0-9_]+ ' .clusterfuzzlite/build.sh | grep -oE 'Fuzz[A-Za-z0-9_]+' | sort -u)
    if [ "$src" != "$ci" ]; then echo "drift: source vs ci.yml fuzz matrix"; diff <(echo "$src") <(echo "$ci"); exit 1; fi
    if [ "$src" != "$bsh" ]; then echo "drift: source vs ClusterFuzzLite build.sh"; diff <(echo "$src") <(echo "$bsh"); exit 1; fi
    echo "fuzz targets in sync: $(echo "$src" | tr '\n' ' ')"

# Run tests with coverage
test-cover:
    go test -cover ./...

# Format code
fmt:
    go fmt ./...

# Lint code
lint:
    golangci-lint run ./...

# Tidy dependencies
tidy:
    go mod tidy

# Run vulnerability check
vulncheck:
    go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# Check for outdated dependencies
depcheck:
    go list -m -u all | grep '\[' || echo "All dependencies are up to date"

# Create and push a new release tag (bumps patch version)
release:
    ./scripts/release.sh
