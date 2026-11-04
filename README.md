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
- `internal/runtime` - the deterministic state-machine runtime (events, actions, clock, ids)
- `internal/controlplane/contract` - request and response types of the control plane API
- `internal/system` - system adapters (clock, S3, run loop)
- `internal/verify/speccheck` - spec checker used by the control plane tests
