# DDD refactor brief: mds-controlplane

Audience: the lead (one agent or person) and the subagents they hand tasks to.
Goal: move the control plane to explicit bounded contexts, aggregates, value objects,
domain events and repositories, with no change in behaviour.

## 0. Standing rules (every task)

- No behaviour change. Each task is a refactor: same inputs give the same outputs and the same log bytes.
- Never push (see memory: dst-process migration, standing rule). Commit locally, one commit per task.
- One task, one branch-sized commit. Do not mix two tasks in one commit.
- Wire format and the replicated log encoding must not change. If a task needs it, stop and ask.
- Deterministic packages (`internal/runtime`, domain packages after task 4) may not import
  `time`, `math/rand`, `net`, goroutines or new dependencies without `audit-go-dependency`.
- Simple English names (docs/simple-english-names.md). ASCII only (`make ascii-check`).
- Read `docs/refactoring_rules.md` and `internal/controlplane/doc.go` first.

## 1. The green gate (run after EVERY task, from repo root)

A task is done only when all of this passes. Paste the final lines of the output in the task report.

    make build            # proto, wire-check, fmt, ascii, vet, lint, modernize, compile, unit tests,
                          # determinism, sim, sim-campaign
    make test-race RACE_RUNS=3
    go test ./internal/controlplane/ -run Architecture -count=1    # import rules

Extra checks, by task type:

| Change touches | Also run |
|---|---|
| `contract`, `wire`, `api/v1`, proto | `make proto && make wire-check`; `git diff --stat` must show no unexpected generated change |
| apply / replay / commit code | `go test ./internal/controlplane/... -run 'Crash|Failure|Conform' -count=1` |
| anything the simulator runs | `make sim-campaign SIM_SEEDS=1000` |
| `internal/spec` | `make spec-long` |
| storage (`kv`, `groups/storage`) | `make it-postgres` if Docker is available, else say it was skipped |

Behaviour-unchanged proof (do this once in task 1 and re-check at the end of each phase):
1. Record golden values on `main`: `go run ./cmd/cpsim -scenario all -seeds 300 -first 1 > build/golden-before.txt`
2. After the task: same command to `build/golden-after.txt`.
3. `diff build/golden-before.txt build/golden-after.txt` must be empty (same trace hashes).
   A changed hash means behaviour changed. Fix the code, do not regenerate the golden.

Planted-bug check (the suite must still catch bugs): `go test ./internal/verify/... ./internal/controlplane/controlplanetest/... -run 'Planted|Bugs' -count=1`
must show all 8 planted bugs found (5 rapid, 3 simulator). If one is no longer found, the refactor broke a test.

Known flake: `TestConformReplica` timed out once on the 3 s call limit under `-race`. If it is the only
failure, re-run it once alone and report it as the known flake. Any second failure is real.

## 2. Ordered plan

Phases run in order. Inside a phase, tasks marked PARALLEL can go to separate subagents at the same time.
Tasks marked SERIAL must wait for the one before. Each subagent works in its own git worktree
(`isolation: worktree`) and the lead merges one at a time, running the green gate after each merge.

### Phase A: safety net (do first, no production code changes)

**A1 (SERIAL) Freeze the baseline.**
Write `build/golden-before.txt` (command above). Run the full green gate on `main` and save the output
to `build/gate-baseline.txt`. List the 8 planted bugs and the tests that find them in `docs/ddd-baseline.md`.
Done when: the gate is green on `main` and both files exist.

**A2 (PARALLEL) Write the glossary.**
Create `docs/glossary.md`: Topic, Partition, Segment, Broker, Group, Member, Generation, Assignment, Offset,
Checkpoint, Command, Event. One line each, plus the Go name that carries it today.
List every name that differs from the glossary (`Inc`, `Gen`, `module`, `executor`, ...) as a rename proposal,
do not rename yet. Done when: the file exists, `make ascii-check` is green.

**A3 (PARALLEL) Add import-boundary tests for the target shape.**
Extend `internal/controlplane/architecture_test.go` with rules that are allowed to fail only as `t.Skip`
with a TODO naming the task that will switch them on. Rules: domain packages import no `api/v1`, `server`,
`replica`, `executor`, `kv`; `server` and `replica` import domain, not the reverse.
Done when: the gate is green and each skipped rule names its task.

### Phase B: vocabulary (small, safe)

**B1 (PARALLEL) Value objects for identifiers.** DDD: Value Object.
Add `TopicName`, `GroupID`, `MemberID`, `PartitionID`, `Generation` in the owning package with a
constructor that validates. Use them inside one context at a time, converting at the edge. First do
topics only. Keep the contract types unchanged. Done when: gate green, golden diff empty,
no new validation rejects input the old code accepted (add a table test of old-accepted values).

**B2 (PARALLEL, after B1 topics lands) Same for groups, then membership.** One subagent per context.

**B3 (SERIAL, after B1 and B2) Renames from the glossary.** Mechanical renames only, one rename per commit
(`gopls rename` or `gofmt -r`). Done when: gate green, `git diff` shows renames only.

### Phase C: aggregates (the core change)

**C1 (PARALLEL) Topic aggregate.** DDD: Aggregate Root.
Create `Topic` type in the topics context holding state and invariants (partition count only grows,
offsets only move forward). Move the rule checks out of the apply/commit functions into methods.
The apply function becomes: load, call method, save. Methods are pure (no IO, no clock).
Done when: every old error path still gives the same `ErrCode`, a table test proves it, gate green,
`-run Crash|Failure|Conform` green, golden diff empty.

**C2 (PARALLEL) Group aggregate.** Same for `Group` (join, install, assign, remove, offset commit,
generation rules). Same done-when.

**C3 (PARALLEL) Broker membership aggregate.** Same for the broker set and heartbeat.
Start by merging the scattered `brokers.go` files' logic into the `membership` package.

C1, C2, C3 touch different packages and may run in parallel. If two touch `kv` keys or `replica/commit.go`,
the lead merges them one at a time and re-runs the gate.

### Phase D: events and repositories

**D1 (SERIAL, after C) Domain events.** Aggregate methods return events (`TopicCreated`, `MemberJoined`,
`OffsetCommitted`, ...). The apply code turns events into the same state writes as before.
Do NOT change what is written to the log. Done when: golden diff empty, replay of an old log file
from `build/` (saved in A1) gives the same state hash.

**D2 (PARALLEL, after D1) Repository interfaces.** DDD: Repository.
Define `TopicRepository`, `GroupRepository`, `BrokerRepository` in the domain packages. Implement them
over `kv` in a new adapter package. Move `controlplanetest/memorystore` to implement the same interfaces.
Switch on the skipped import rule from A3 for `kv`. Done when: domain packages no longer import `kv`.

### Phase E: layers and edges

**E1 (SERIAL, after D2) Application layer.** DDD: Application Service.
Extract from `replica` and `module` one service per command: take command, load aggregate, call, save,
return events. `replica` keeps only log, roles, checkpoint and catch-up. Done when: the `replica` package
no longer contains rule checks, gate green, `make sim-campaign SIM_SEEDS=1000` green.

**E2 (PARALLEL, after E1) Split `contract` per context.** DDD: Published Language.
Per-context request and response types generated from the same proto. Keep `contract` as a thin shared kernel
(`Offset`, `TopicPartition`, error codes). Needs `make proto && make wire-check`; generated diff reviewed by hand.

**E3 (PARALLEL, after E1) Anti-corruption layer.** DDD: ACL.
Make `wire` the only place that converts `api/v1` messages. Remove conversions at the end of `server/*.go`.
Switch on the `no api/v1 in domain` import rule.

### Phase F: verify rewrite (optional, only after A to E are merged)

**F1 (SERIAL) Oracle replays through the aggregates.** Make `internal/verify/simulator/oracle.go` use the
aggregates instead of `internal/spec`. Run both and compare for 1000 seeds before removing `spec`.
**F2 (SERIAL) Split the simulator** into `world`, `workload`, `oracle` as described in the earlier answer.
Gate: all 3 simulator planted bugs found within `SIM_SEEDS=300` per scenario.
**F3 (SERIAL) Delete `internal/spec`** only when F1 shows zero differences and `make spec-long` is no longer needed.

## 3. Subagent task template

Copy this into each delegation. Fill the braces.

    Task {ID}: {title}. Read DDD-REFACTOR-BRIEF.md sections 0 and 1 and the task text for {ID}.
    Work in your own worktree. Touch only: {packages}. Do not push.
    Steps: 1) read the packages and list what you will move; 2) write the characterisation test
    first (old behaviour, must pass before and after); 3) make the change in small commits;
    4) run the full green gate and the extra checks for your change type.
    Report: files changed, the last 15 lines of `make build`, golden diff result (must be empty),
    planted-bug result, anything skipped and why. If any check is red, say so, do not hide it.

Suggested effort: `lower` for A2, B3 (mechanical); default for B1, B2, C, D, E; `higher` for C2, D1, E1, F1.

## 4. Lead checklist before each merge

1. Subagent report includes real gate output, not a summary.
2. Re-run `make build` yourself on the merged result.
3. `diff build/golden-before.txt build/golden-after.txt` is empty.
4. `git diff --stat main` touches only the packages named in the task.
5. No new `t.Skip`, no deleted test, no loosened assertion (`git diff -- '*_test.go'` read by hand).
6. Test line count did not drop without a note (the repo is also cutting tests, see docs/test-cuts.md).

## 5. Stop and ask the user if

- a task would change wire bytes, log bytes, or a persisted key;
- a golden hash changes and the cause is not obvious in 15 minutes;
- a planted bug is no longer found;
- two subagents need the same file;
- the known flake fails twice in a row.

## 6. Final acceptance (end of phase E)

- `make build` and `make test-race RACE_RUNS=5` green.
- `make sim-campaign SIM_SEEDS=5000` green.
- golden diff empty against A1, all 8 planted bugs found.
- Domain packages (topics, groups, membership) import none of: `api/v1`, `server`, `replica`, `executor`, `kv`, `runtime` I/O.
- `docs/glossary.md` matches the code names; `internal/controlplane/doc.go` map updated.
