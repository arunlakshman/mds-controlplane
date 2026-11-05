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
T1.10 | done | cherry-picked abb2f8f,51fa0ff,4fb2d3c(RE-PIN),160261b,f6d191b; dst commits e0a59b9,7cf72fe,67d894b on local branch dst-process (unpushed) | make build + make mutants 8/8 green on merged tree; race + campaign see P1 gate | RE-PIN: golden basic/17 f79ae0cb6ac19047->08bfa86385d8e8ae, chaos/42 841e92a10f1559f3->45d210abda0e2bed; 300-seed hashes now da8028bc bc9f0863 23603524 67c48ce5 ba61ad2c
T2.1 | done | cherry-picked 7bd9a27,36f7a2f | make build green, make it green (~41s, Docker ON), go list -deps ./cmd/mdscontrolplane has 0 testcontainers, golden unchanged | internal/testinfra: Main(m), DynamoDB(t), Postgres(t), S3(t); testcontainers-go v0.44.0 core only
T2.3 | done | cherry-picked b0d2ab7,519caff | task: make build + make it green (1m02s), sqllog 8 conformance subtests pass on real Postgres; merged tree vet ok, full gate after Group D | sqllog tests under integration tag via testinfra.Postgres; it-postgres, postgres-test.sh, POSTGRES_IMAGE, PG_TEST_ARGS removed
T2.2 | done | cherry-picked (1 commit, "Run dynamolog tests on DynamoDB Local") | task: make build + make it green; dynamolog 13 test cases + 8 conformance subtests pass on DynamoDB Local; merged tree vet ok, full gate after Group D | dynamolog has no normal-build tests now (all need a service/failure injection)
T2.4 | done | cherry-picked 390422e->bd7c002, 5f6cc7a->172fe41 (one conflict in testinfra.go, resolved by hand: SeaweedFSImage const + T2.3 comment removal) | merged tree: make build green, make it green (all pkgs), make mutants 8/8, golden unchanged | S3 tests on chrislusf/seaweedfs:4.48 (`weed server -dir=/data -s3 -s3.port=8333 -volume.max=50`), failures via faultTransport http.RoundTripper; fakes3_test.go deleted; no assertion changed vs fake; S3(t) no longer needs t.Setenv (parallel ok)
T2.5 | done | cherry-picked a734faa,48961b6,6d68f8b | merged tree: make build green, make it green (test/integration ~50s, real binary, dynamolog+postgres, 3-node takeover with SIGKILL, restart from real checkpoint), make mutants 8/8; agent also ran race x3 + campaign 2000 green on identical tree | controlplanetest deleted (-1519 lines incl fakedynamodb 1211); deps seam removed; 19 files +840/-2197; no leftover processes/containers
T3.1 | done | cherry-picked 9beaf14,07a28a1,1af3c31,7489a49 | merged tree: make build green, make mutants 8/8, go vet -tags integration clean; agent also ran make it green | kv -> metadb/engine (+engine/memory, engine/pebble); Store->Engine, Transaction->Txn; engine added to determinism gate (listed without /..., so memory and pebble stay outside); golden unchanged
T3.2 | done | cherry-picked aee78d3,1c2655c,3fef79b,cc235ba | merged tree: make build green, make mutants 8/8, vet -tags integration clean; agent make it green | metadata -> metadb, Store -> DB; 47 files +234/-234; golden unchanged
T3.3 | done | cherry-picked 95a60d7,9e23946 | merged tree: make build green, make mutants 8/8, golden unchanged; agent: vet -tags integration + make it green | metadb pruned: Exec, Command, CommitBatch, Commit, Lookup, Describe, CreateTopicIf, End, Window, Assigned, AssignState, OffsetOwner*, OffsetFetch, DescribeGroup, ListGroups, SnapshotAt, CreateEntry deleted; production +28/-165, tests +119/-93
T4.1 | done | cherry-picked 2031464,c13557e,933a843,59d5aa0 | merged tree: make build green, make mutants 8/8, vet -tags integration clean; agent: make it green + fresh clone make build green; golden unchanged | api/controlplane/{shared/v1 (sharedv1), v1 (controlplanev1/cpv1), log/v1 (logv1)}; Log prefix dropped; PartitionOffset+GroupSummary merged into shared; log pkg has 0 grpc deps; wiregen adapted (duplicate short-name check)
T4.2 | done | cherry-picked 571a32a,3a4be29,372d61a (commit trailer rewritten from Opus to Sonnet 5.5 per session rule) | merged tree: make build green, make mutants 8/8, vet -tags integration clean; agent: make it green + fresh clone make build green; golden unchanged | wiregen (536 lines) deleted; ops/tools/protoc-gen-mdswire (497+38 lines) + options.proto (6 options: no_converters, go_struct, go_existing, go_name, go_type, go_embed); run via `go run` from buf.gen.yaml
T4.3 | done | cherry-picked 0d802bf,74b1bda,f241908,80bd5fe (trailer normalised to Sonnet 5.5) | merged tree: make build green, make mutants 8/8, vet -tags integration clean; agent: make it green, race x1 green; golden unchanged | RPC renames: Lookup->FindSegments, GroupHeartbeat->Heartbeat, GroupOffsetCommit->OffsetCommit, GroupOffsetFetch->OffsetFetch (+Request/Response); GroupDescription reshaped (GroupMemberJoin, GroupMemberInfo, GroupDetails new nested messages); server converters deleted (-200 lines), wire/rpcerror.go has OutcomeFromCore+ToRPCError
T4.4 | done | cherry-picked b5ee9c5,5f49ee1,f69a194,21c3764(RE-PIN) + my fix cf5d3cb | merged tree: make build green, make mutants 8/8; agent: make it, fuzz (6 targets, no failure), fresh-clone build, race (see note), cpsim 2000 green | log entries in protobuf: log/v1 Entry{id, oneof command}; metadb/entry.go + entry_codec.go; Encode Deterministic:true; Decode rejects unknown fields; topics/codec.go and groups/storage/codec.go and module Kinds/KindOf/Encode/Decode deleted (-~315 lines); audit record docs/dep-audits/google.golang.org/protobuf@v1.35.2.md ACCEPTED WITH RULES

## Deferred items

- T4.4 CORRECTION: simulator golden hashes and cpsim hashes did NOT move (trace hash never folds entry bytes). Only the metadb entry pin moved (TestGoldenEntryBytes -> TestEntryBytesPinned, pin 0a0278321a0c0a066f726465727310031801). Current hashes unchanged: 300 seeds da8028bc bc9f0863 23603524 67c48ce5 ba61ad2c; 2000 seeds 97838934 211cf873 81de9625 f8a3617b 440a0f5a.
- T4.4 AUDIT (for the user to review): verdict ACCEPTED WITH RULES, no real problem found; proof: 8 differently built binaries x2 runs gave identical entry bytes. Rules: marshal only via metadb.Encode; no map/extension/lazy fields in log messages; every logged string valid UTF-8; never compare entry bytes across builds or feed them to the state hash; Decode refuses unknown fields. DOUBT 1: Encode now PANICS on invalid UTF-8 strings (old codec accepted any bytes); gRPC enforces UTF-8 at ingress so no known bypass, but it is a crash not an error. DOUBT 2: simulator decode uses protoreflect Range whose order differs per build; only a yes/no comes out, believed safe. allow.txt gained google.golang.org/protobuf/proto and .../reflect/protoreflect.
- T4.4: stale comment replica/commit.go:16 ("kinds 0..2") left (replica out of scope). audit-go-dependency skill names ops/scripts/audit-dep.sh and docs/decisions.md which do not exist.
- DATA RACE FOUND AND FIXED (cf5d3cb): rare make test-race failure was a race in test/conformance/grpc_test.go (Serve goroutine read n.gs while the next reboot overwrote it). Probably the 'unexplained failure under -race with heavy parallel load' of plan section 3. Fixed by capturing the server in a local.

- T4.3: commit (b) 74b1bda alone does not build (fixed by (c)); history is not bisectable at that commit. GetStatus still builds cpv1.GetStatusResponse by hand in server (not a contract conversion; would need a contract type + uint64 support in plugin). Old design docs still say Lookup.
- T4.3: wire API now nests group description (cost of mapping to contract).

- T4.2: generated output same types/docs/bodies but NOT textual zero-diff: declarations now in proto file order (sorted-declaration diff IDENTICAL). Field comments not copied (would add docs). Some proto comments reworded to become Go docs.
- T4.2: plugin run by `go run` (not make proto-tools) so it never goes stale. architecture_test.go edited: log pkg may import options/v1; options/v1 imports nothing of this module.
- T4.2: GroupDescription and GroupMemberInfo (service pkg) have no wiring options yet: T4.3 needs go_existing/go_struct (+ go_embed or flatten).

- T4.1: log OffsetFetch/ListGroups messages duplicate service messages (same fields, different names); merging = API rename, left to T4.3. ErrCode/Outcome/ErrorDetail stay in service package. SegmentRef/PendingBatch/BatchRange/TopicInfo in shared by design for T4.4.
- T4.1: edge list in ci-determinism.sh not exercised until T4.4 makes metadb import logv1. buf lint warnings (service suffix, enum zero value) pre-existing.

- T3.3: plan was wrong: replica does NOT use CommitBatch (applies entry by entry); it was dead in production. Two tests deleted with their features (TestCommitBatchEqualsSequential, TestSnapshotAtTagsTheViewsSlot); no mutant covers metadb unit tests so judged by hand.
- T3.3: topics.End, Window, Assigned, AssignState, OffsetOwner* now have no production caller (only TestAssignedStates). Prune with the T5.1 topic context work.
- T3.3: one-line edit in replica/commit_test.go (End read via Read) needed to compile; comments edited in groups/, topics/, runtime/. "decision N"/CP-DSM-N references left.

- T3.2: test/var names (TestMetadata*, localStore, newGroupStore, Metadata fields) left as is (churn). ops/mutants.txt needed no edit.

- T3.1: architecture_test.go allows metadata -> metadb/engine/memory (metadata.NewMem uses it). Test names like TestMemoryStore* and local vars named `transaction` not renamed (churn). pebble.DB type name kept.

- T2.5: make lint does not use the integration build tag, so tagged code (test/integration, internal/testinfra, *_it tests) is never linted by make build. Agent ran golangci-lint --build-tags integration by hand: 0 issues. Consider adding a lint-it step (small Makefile change) - not in plan, ask/decide later.
- T2.5: edited architecture_test.go and controlplane/doc.go (outside Touch, needed after package deletion). mise.toml and docs/test-cuts.md still stale -> T7.1.
- T2.5: restart-from-checkpoint test now truly tests a checkpoint (old one had checkpoints off via -checkpoint-every -1s).
- Docker deploy (kind/helm) not tested, per plan section 7.

- T2.4: SeaweedFS port answers before filer/volume ready: CreateBucket retried up to 60s, TestS3RoundTrip ~10s. UniqueBucket() in s3store/store.go is now only used by a test assertion (non-test code, left).
- T2.1 MinIO item above is superseded by SeaweedFS (user instruction). Plan text still says MinIO.

- T2.3 KNOWN SKIPS until T2.5: cmd/mdscontrolplane/logstore_test.go:515-519 and postgres_failover_test.go:24-28 still read MDS_TEST_POSTGRES_DSN and t.Skip; nothing sets it now, so they always skip until T2.5 moves them. (Rule 2.5 breach is temporary; T2.5 must fix.)
- T2.3: stale references: mise.toml:4 (mentions make it-postgres / POSTGRES_IMAGE), docs/test-cuts.md:65. Fix in T7.1 or earlier.
- T2.3: testinfra pins postgres:17.11-alpine itself (PostgresImage const), Makefile var removed.

- T2.1: MinIO image: minio/minio and quay.io/minio/minio can no longer be pulled; used frozen, unmaintained bitnamilegacy/minio:2025.7.23-debian-12-r3. Revisit (user may want another image or a source build).
- T2.1: S3(t) uses t.Setenv for AWS creds (MinIO rejects s3store's dummy mds/mds), so tests that call it cannot t.Parallel.
- T2.1: DynamoDB(t) pre-creates the table (key schema copied, not dynamolog.CreateTable, to avoid import cycle); no helper yet for testing CreateTable itself.
- T2.1: Go-side Docker-off failure path not proven (testcontainers falls back to /var/run/docker.sock); Makefile `docker info` check guards `make it`.

- T1.10: proved with sites stubbed off, old golden + old cpsim hashes came back exactly, so only the 3 sites moved hashes.
- T1.10: no object-store short-page site (ObjectList has no page limit; would be illegal). Sites also fire in the quiet tail (legal). Sites use simulator's own injector (env.inj).
- T1.10: dst changes live ONLY on local branch dst-process in ~/tmp/dst (3 commits). Nothing pushed. User must push/publish when moving dst to manifesterio.

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
- T1.10 merged (RE-PIN done). New baseline hashes (300 seeds): basic da8028bc, log-faults bc9f0863, crash-restart 23603524, leader-fencing 67c48ce5, chaos ba61ad2c. P1 gate: build ok, mutants 8/8 ok, race running, campaign next.
- P1 PHASE GATE GREEN: build, mutants 8/8, race x3, campaign 2000 seeds (build/p1-*.txt). P1 complete. Starting T2.1 (serial, sonnet).
- T2.1 merged; make build + make it green on merged tree. Next: Group D T2.2 (dynamolog), T2.3 (sqllog), T2.4 (s3store) in parallel.
- USER INSTRUCTION (during T2.4): use the SeaweedFS image for S3 tests instead of MinIO (bitnamilegacy/minio was unmaintained anyway). T2.4 agent told to change internal/testinfra/s3.go + smoke test. Supersedes the MinIO wording in plan T2.1/T2.4 (plan text not edited).
- Group D (T2.2, T2.3, T2.4) merged; full make build + make it + make mutants green on merged tree. Makefile comment updated to SeaweedFS. Next: T2.5 (serial, opus): end-to-end tests on the real binary; also must fix the two permanent Postgres skips in cmd/mdscontrolplane.
- T2.5 merged. P2 PHASE GATE GREEN: build, mutants 8/8, race x3 (agent run on identical tree), campaign 2000 (agent run), make it (real Docker). Next: P3 renames, serial: T3.1 kv -> metadb/engine (haiku).
- T3.1 merged. Next: T3.2 metadata -> metadb (Store -> DB), serial.
- T3.2 merged. Next: T3.3 prune metadb (sonnet).
- T3.3 merged; P3 gate: build ok, mutants 8/8 ok; race x3 + campaign 2000 running (build/p3-*.txt).
- P3 PHASE GATE GREEN: build, mutants 8/8, race x3, campaign 2000 (build/p3-*.txt); make it green per agents. P3 complete. Starting P4: T4.1 split proto files (serial, sonnet).
- T4.1 merged. Next: T4.2 wiregen -> protoc plugin (serial, opus, higher).
- T4.2 merged. Next: T4.3 clean up the API (sonnet).
- T4.3 merged. P4 so far green. Next: T4.4 log entries in protobuf (opus, higher, RE-PIN, needs audit-go-dependency for protobuf in deterministic path; stop and ask if the audit finds a real problem).
- T4.4 merged + race fix. P4 gate: build ok, mutants 8/8 ok; race x3 and campaign 2000 running (build/p4-*.txt).
