# Refactor progress and audit log

Branch: refactor/greenfield (local only, never pushed). Plan: docs/greenfield-refactor-plan.md.
Format per task: `ID | state | commit | gate result | note`.

## Tasks

T0.1 | done | cb4ff89 | make build green, cpsim 300 seeds green (hashes in build/baseline-cpsim-300.txt) | Docker ON
T0.2 | done | 96f8fca | make build green, cpsim hashes equal baseline | real path was internal/controlplane/api/v1/proto_test.go
T0.3 | done | cherry-picked 26bb715,d446235 | make build green on merged result, hashes equal baseline | also touched internal/controlplane/doc.go comments; doc.go has a pre-existing wrong claim (map at top of server/brokers.go), T7.1 fixes it
T0.4 | done | cherry-picked 2e59c36 | make build green on merged tree; agent also did fresh-clone build green | generated files untracked, `make generate` added, wire-check removed
T0.5 | done | cherry-picked ac1a639 | make build green on merged tree | brief deleted, note added to docs/test-cuts.md
T1.1 | done | cherry-picked 1067a50,9449231 | make build green, make mutants 8/8 killed (~26s) on merged tree, golden unchanged | see notes below
T1.2 | done | cherry-picked 23abd70 | make build green, make mutants 8/8, conformance pkg 6.5s, golden unchanged | 607 lines (target ~500), files named *_test.go not ops.go
T1.3 | done | cherry-picked 2cb69b3 | make build green, make mutants 8/8 (37s, old suite), -race x3 green, golden unchanged | replica target 269 lines, TestReplica 8.4s vs old 8.7s
T1.4 | done | cherry-picked ac33ff4,31939d6 | make build green on merged tree (with T1.3), make mutants 8/8 | grpc_test.go 484 lines; added Target.Lacks (4 lines in shared harness); TestGRPC ~18s

## Deferred items

- T1.3: old must-reach answers sweep.find/OK and sweep.describeGroup/OK are not required in new harness (sweeps still compared to model). T1.5 should decide whether to restore them.
- T1.4: gRPC target cannot send group ops (no group install/assign/remove on wire); group mutants must be killed by the metadata Memory target (T1.5). gRPC and replica targets each have their own copy of disk/process wrapper (grpcProcess vs process): merge in T1.5 if cheap.
- T1.4/T1.3: test doubles forgetful and staleReads are to be rebuilt by wrapping Target closures (T1.3 verified both are caught by hand).
- Process note: fmt-check in main checkout scans .claude/worktrees; make build in main checkout fails while a task worktree has unformatted files. Build in main only when no agent is mid-edit.

- T1.2: Target has no way to say "topics only"; T1.4 (gRPC has no group install/assign yet) needs a small addition. Left to T1.4 agent, minimal change, reported.
- T1.2: DirtyReboot(keep) draws keep 0..2; trimmed-log rule (keep 0 only) belongs in the T1.3 replica target.
- T1.2: new Pebble target is weaker than memory on two group mutants (fewer checks); relevant to T1.5 when mutants are pointed at the new suite (use Memory target for those).

- T1.1 DEVIATION (accepted): added test-only file change internal/controlplane/replica/commit_test.go (TestWindowForgetsKeysOnlyOnLogStride), because nothing tested WindowSlots so per-replica-window survived. No replica behaviour change. Rule 4 spirit kept; user should know.
- T1.1: rapid-based mutants use -rapid.seed=1 (unseeded kills are by chance). After T1.2/T1.5 the seeds may need re-choosing.
- T1.1: planted-bug flags (InjectedBug*, WriterBases, commitWithBases) still in production until T1.6.

- Dockerfile and CI workflow changes of T0.4 (buf-setup-action@v1 pinned buf 1.47.2, make generate) are NOT run: dst replace is a local path. Check when dst is published.
- make lint can fail with "parallel golangci-lint is running" when two builds run at once. Retry; do not run gates in parallel with agents.

(none yet)

## Audit log (newest last)

- 2026-11-05 start: branch refactor/greenfield made from main; plan committed as cb4ff89. User away ~2h, told me to continue on my own and keep this audit trail.
- T0.1 done: baseline green. Starting Group A (T0.2 T0.3 T0.4 T0.5) in worktrees.
- T0.2 merged (ff). Plan path api/v1 is really internal/controlplane/api/v1; harmless. Method: rebase each task branch onto refactor/greenfield, then ff-merge.
- T0.3 merged by cherry-pick (task branch is checked out in its worktree, so rebase is impossible; use cherry-pick from now on). make build re-run on merged tree: green.
- T0.4, T0.5 merged by cherry-pick; make build green after each. P0 tasks all done. Next: P0 phase gate (race, campaign), then P1 (T1.1 serial).
- P0 phase gate green: build, race x3 (build/p0-race.txt), campaign 2000 seeds all ok (build/p0-campaign.txt). Starting T1.1 (serial).
- T1.1 merged. Next: T1.2 conformance harness (serial, opus).
- T1.2 merged. Next: T1.3 (replica target) and T1.4 (gRPC target) in parallel, Group B; both touch test/conformance, harness edits must be minimal.
- T1.3 and T1.4 merged; make build + make mutants green on merged tree. Next: T1.5 (serial, sonnet, higher): point mutants/doubles at new suite, then delete old suite.
