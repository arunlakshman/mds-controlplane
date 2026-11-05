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
T1.5 | done | cherry-picked 8 commits 6e003d8..ff1e25b | make build green, make mutants 8/8 (26.6s, all on new suite, no rapid seed), golden unchanged | old suite + memorystore deleted (-1256 lines); test/conformance 1364 -> 1488 lines; diff 237+/1355-
T1.6 | done | cherry-picked 30cf03b | make build green, make mutants 8/8 (35s), golden hashes unchanged, grep for removed names empty | 13 files +14/-130; also edited topics_test.go and ops/mutants.txt (outside Touch, needed)
T1.7 | done | cherry-picked 5 commits (86e56b8..f605b62) | task: make build + make mutants 8/8 (62s) green in worktree; merged tree: vet + go test ./internal/controlplane/... green, full make build pending until T1.9 merged | groupfix,kvtest,metadatastore,storecall deleted (-242 lines); coordinator tests use spec.New via small mutex modelStore wrapper
T1.8 | done | cherry-picked 072fbe9,e8f2f35,6e16227 | task: make build green; make fuzz 8 targets x10s no crashes (~2m19s); merged tree go test green | fuzz_test.go in metadata, topics, groups/storage (+Makefile fuzz target)
T1.9 | done | cherry-picked 597ee75,57128a5,2a403e3 | make build green, make mutants 8/8 (28.5s) on merged tree incl. T1.7+T1.8; agent ran campaign 2000 seeds green; 300-seed hashes equal baseline | cpsim -report/-json/-until-stable; expectedReachable kept (nothing in data names a lost path); report only touches cmd/cpsim + docs

## Deferred items

- T1.9: coverage curve flattens at seed 4 (21 assertions all seen at once), so -until-stable is only useful once T1.10 adds more sites. ~4 reachable paths are not in expectedReachable; list could be extended (not done).
- ENV: ~/tmp/dst was checked out on `main` (== origin/dst-process == origin/main, same commit 4e036b2, clean). Created local branch `dst-process` there for T1.10 so main stays untouched. Never pushed.

- T1.7: dropped metadatastore/store_test.go (adapter-only tests; lost cases: unknown-op and cancelled-ctx of the adapter itself). docs/test-cuts.md still names deleted helpers (historical).
- T1.7: remaining users of controlplanetest: cmd/mdscontrolplane/{logstore,postgres_failover}_test.go (Client), executor/dynamolog (fakedynamodb). Handled in P2.
- T1.8: intentional fail-stop panics kept: groups/storage corruptGroup() via decodeHeader/Member/Offset (fuzz tests recover only this one); topics/commit.go replay panics (~215, 221); mustStore panics. FUZZ_TARGETS in Makefile is hand-kept.

- T1.6 FIDELITY NOTE: writer-offsets mutant is now a "gap" edit in topics/commit.go assign (base +1, every commit leaves a gap), not literally "writer decides outcomes from stale state before append". Killed by TestScenariosPass. Revisit when rebalance/decide-at-apply work (P5) lands, if a closer mutant is wanted.
- T1.6: docs may still mention removed names (InjectedBug etc.); T7.1 sweep.

- T1.5 TRADEOFF (accepted): to keep new suite no slower than old, gRPC target is now 1 variant (trim on, crashes only, no clean Reboot, 10 checks x 40 steps); TestReplica keeps all 4 variants. Restoring gRPC restarts costs 3-5s.
- T1.5: controlplanetest.Client has no direct test now (TestRoundTrip gone); StartMem/Start in controlplanetest/grpcserver.go have no users - T1.7/T2.5 decide.
- T1.5: grpcProcess and process wrappers not merged (differ in locking/bookkeeping).
- T1.5: edited internal/controlplane/controlplanetest/doc.go (outside Touch, comment only).
- T1.5: agent ran `go clean -cache` because disk was nearly full (mutant runs fill the cache). Watch disk.
- New-suite run time: ~10.5s median vs old ~14.5s.

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
- T1.5 merged, old suite deleted, worktrees cleaned. Next: T1.6 (serial; the rule-4 exception in replica), then Group C: T1.7, T1.8, T1.9.
- T1.6 merged. replica diff vs main so far: commit.go/config.go/replica.go/role.go deletions (rule 4 exception) + commit_test.go test. Next: Group C T1.7, T1.8, T1.9 in parallel.
- T1.7 and T1.8 merged. T1.9 still running; full make build + mutants on merged tree after it.
- T1.7, T1.8, T1.9 all merged; worktrees cleaned. Full make build + make mutants green on merged tree. Starting T1.10 (serial, RE-PIN, edits dst on local branch dst-process).
