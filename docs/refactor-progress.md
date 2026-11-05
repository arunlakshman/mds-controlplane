# Refactor progress and audit log

Branch: refactor/greenfield (local only, never pushed). Plan: docs/greenfield-refactor-plan.md.
Format per task: `ID | state | commit | gate result | note`.

## Tasks

T0.1 | done | cb4ff89 | make build green, cpsim 300 seeds green (hashes in build/baseline-cpsim-300.txt) | Docker ON
T0.2 | done | 96f8fca | make build green, cpsim hashes equal baseline | real path was internal/controlplane/api/v1/proto_test.go
T0.3 | done | cherry-picked 26bb715,d446235 | make build green on merged result, hashes equal baseline | also touched internal/controlplane/doc.go comments; doc.go has a pre-existing wrong claim (map at top of server/brokers.go), T7.1 fixes it
T0.4 | done | cherry-picked 2e59c36 | make build green on merged tree; agent also did fresh-clone build green | generated files untracked, `make generate` added, wire-check removed
T0.5 | done | cherry-picked ac1a639 | make build green on merged tree | brief deleted, note added to docs/test-cuts.md

## Deferred items

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
