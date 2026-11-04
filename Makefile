# Build for the MDS control plane. `make` / `make build` is the full gate:
#   proto, fmt, ascii, vet, lint, modernize, compile, unit tests, determinism, simulation.
# Toolchain + commit hook: `mise install` (pins in mise.toml; postinstall runs make install-hooks).

BUF             ?= buf
PROTOC_GEN_GO   ?= protoc-gen-go
PROTOC_GEN_GRPC ?= protoc-gen-go-grpc
PROTO_GO_VER    ?= v1.35.2
PROTO_GRPC_VER  ?= v1.5.1
# Image for make it-postgres (pinned; see ops/scripts/postgres-test.sh).
POSTGRES_IMAGE  ?= postgres:17.11-alpine
PG_TEST_ARGS    ?=
# Seeds per scenario for make sim-campaign (see docs/simulation.md).
SIM_SEEDS       ?= 300

.PHONY: build compile test proto proto-tools fmt-check vet lint modernize-check modernize \
	ascii-check determinism sim sim-campaign install-hooks it-postgres

build: proto fmt-check ascii-check vet lint modernize-check compile test determinism sim sim-campaign

# Generate Go from internal/controlplane/api/v1/*.proto into ignored *.pb.go files.
proto: proto-tools
	@if command -v mise >/dev/null 2>&1; then \
		cd internal/controlplane/api && mise exec -- $(BUF) generate; \
	elif command -v $(BUF) >/dev/null 2>&1; then \
		cd internal/controlplane/api && $(BUF) generate; \
	else \
		echo "buf not found: run \`mise install\` (pins buf in mise.toml)" >&2; \
		exit 1; \
	fi

proto-tools:
	@command -v $(PROTOC_GEN_GO) >/dev/null || go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTO_GO_VER)
	@command -v $(PROTOC_GEN_GRPC) >/dev/null || go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTO_GRPC_VER)

install-hooks:
	@mkdir -p .git/hooks
	@ln -sfn ../../ops/scripts/githooks/pre-commit .git/hooks/pre-commit
	@chmod +x ops/scripts/githooks/pre-commit
	@echo "installed .git/hooks/pre-commit -> ops/scripts/githooks/pre-commit"

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	go vet ./...

lint:
	golangci-lint run ./...

modernize-check:
	go fix -diff ./...

modernize:
	go fix ./...

ascii-check:
	@ops/scripts/ascii-check.sh

# Determinism gate for internal/runtime, the contract and the deterministic domain packages.
determinism:
	bash ops/scripts/ci-determinism.sh

compile:
	go build ./...

test:
	go test ./...

# Deterministic simulation of the replicated control plane: its own tests, then
# a campaign of SIM_SEEDS seeds for each scenario. A failure prints the command
# that runs the seed again.
sim:
	go test ./internal/verify/simulator/...

sim-campaign:
	go run ./cmd/cpsim -seeds $(SIM_SEEDS) -scenario all

# Log tests against a real PostgreSQL in Docker (needs Docker). Not part of build.
it-postgres:
	POSTGRES_IMAGE=$(POSTGRES_IMAGE) bash ops/scripts/postgres-test.sh $(PG_TEST_ARGS)
