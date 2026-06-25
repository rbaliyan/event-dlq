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

# Fuzz each target briefly; override the per-target time with `just fuzz 1m`
fuzz time="30s":
    #!/usr/bin/env bash
    set -euo pipefail
    for t in FuzzMatchesFilter FuzzNormalizeErrorType FuzzStoreAndFilterMessages; do
        echo "== fuzzing $t =="
        go test -run '^$' -fuzz="^$t\$" -fuzztime={{time}} .
    done

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
