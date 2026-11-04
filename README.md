# mds-controlplane

The control plane of MDS: metadata, topics, consumer groups, and the replicated log that stores them.

Extracted from `github.com/manifesterio/mds` (branch `refactor/controlplane-ddd`, PR #34).

## Build

    mise install      # go, golangci-lint, buf; installs the pre-commit hook
    make build        # proto, fmt, ascii, vet, lint, compile, unit tests

Run: `go run ./cmd/mdscontrolplane`. Docker and Helm: see `deploy/README.md`.

## Layout

- `cmd/mdscontrolplane` - the binary
- `internal/controlplane` - the control plane modules
- `internal/core`, `internal/system` - shared core types and system adapters (copied from the mds repo)
- `internal/verify/speccheck` - spec checker used by the control plane tests
