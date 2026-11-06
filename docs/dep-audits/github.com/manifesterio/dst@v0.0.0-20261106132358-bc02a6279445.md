# github.com/manifesterio/dst@v0.0.0-20261106132358-bc02a6279445

- Date / auditor: 2026-11-04, retarget of mds-controlplane cpsim onto OSS dst (module moved from `github.com/arunlakshman/dst`, v0.0.0, to the manifesterio org).
- Requires Go: 1.26+
- Verdict: **ACCEPTED** for `internal/verify/simulator` (and future deterministic packages that need the same primitives).
- v0.2.0 added `check` (generic oracle checks over a log; imports only `assert` and `fmt`; no goroutines, time or OS). Inside the deterministic gate of dst.
- 2026-11-06: dst now comes from the public repo `github.com/manifesterio/dst` (through the default Go proxy, no credentials), at the tip of `main` (commit `bc02a6279445`, Go pseudo-version `v0.0.0-20261106132358-bc02a6279445`). There are no tags; we pin the pseudo-version. The audited code is the old `v0.3.0` plus the three T1.10 commits (`fault.Injector.Sites`; `campaign` `Result.Sites`, `Summary.Sites`, `NeverFired`; the first-seen order list), squashed into one `Initial commit`, with the `.github` folder of dst removed. It is the same tree that was audited before (dst branch `dst-process`, `67d894b`); only the history changed. No new import, no new module, no change to the allow-list: the packages we import are still the same twelve. The local `replace` in `go.mod` is gone.
- Earlier, 2026-11-05 (T1.10): three local commits on dst branch `dst-process` (`e0a59b9`, `7cf72fe`, `67d894b`) on top of the tag `v0.3.0`; they are now in the one initial commit. `fault` gains `Injector.Sites()` and `SiteCount{On, Calls, Fired}` (counts of `Maybe`/`MaybeP` sites). `campaign` gains `Result.Sites`, `Summary.Sites`, `SiteTotal`, `NeverFired`, and `Merge` adds sites. No new import in our code, no change to the allow-list, no new module. `campaign` now imports `fault` inside dst (both already on the list). The counts draw nothing from the stream. See the finding below.
- Import paths allowed (`ops/tools/determinismcheck/allow.txt`), one by one: `assert`, `campaign`, `check`, `clock`, `eng`, `fault`, `golden`, `mutant`, `rng`, `sched`, `sim`, `trace`.

## Findings
| Finding | Matters? | Why |
|---|---|---|
| `sim` (runner) has no goroutines, time or OS; dst's determinismcheck passes on it. v0.3.0 adds `Context.Wall`, `Crash{Who,Cause}`, `Exit`, `Restart{New}` with incarnation drop, `Config.OnStep`, `Runner.RunUntil` | No | Allowed. |
| `eng` is only the engine interface (types and an error value) | No | Allowed. dst's determinismcheck passes on it. |
| Allowed packages use no wall clock, OS, network or goroutines | No | Same SplitMix / heap / FNV / Maybe latch as the former CP-local bodies. |
| `syncclock` imports `sync`; its own doc forbids use inside a deterministic run | Yes | Not on the allow-list. Only test code outside the gate (`internal/system/fakeclock`) imports it. |
| `realclock` reads the real clock | Yes | Not on the allow-list. Only production adapter `internal/system/realclock` imports it. |
| `model`, `examples/...` not reviewed for determinism | Yes | Not on the allow-list until someone checks them. |
| T1.10: `fault.Injector` keeps a per-site counter and a first-seen name list (`sites`, `order`). `Sites()` copies them without ranging over a map (dst's determinismcheck rejects that, and `fault` is on the list). No time, OS, goroutine or new import; the counters do not draw from the rng | No | Allowed. With our new sites switched off, every trace hash is the same as before the change (checked on 5 scenarios x 300 seeds and on the golden seeds). |
| T1.10: `campaign` imports `fault` (type of the counts) | No | `campaign` is not used by the simulator, only by `cmd/cpsim`, which is outside the determinism gate. |
| dst has no tags; we pin a pseudo-version of the `main` tip | Yes (process) | A pseudo-version names one commit, so the code cannot change under it (go.sum pins the hash). Re-audit on every bump of the pseudo-version. The old tag `v0.3.0` and the three commits are not in that repo (squashed); the code is the same as audited. |

## Rules for our code
1. Import only the twelve packages above from the simulator and other determinism-gated code. Do not pull examples.
2. Keep CP scenario / node / oracle control flow local; dst owns the shared stream primitives only.
3. Quiet must go through `fault.Injector.SetQuiet` (suppresses `Fault` only).

## Evidence
- Corpus gate (since T6.5, with the rebalance machine and the consumers in the simulator): `cpsim -scenario basic -seed 17` -> `hash=b8160450304fcc49`; `cpsim -scenario chaos -seed 42` -> `hash=fe0e8c6763239d00`. From T1.10 to T6.5 these were `08bfa86385d8e8ae` and `45d210abda0e2bed` (they still are with the consumer groups left out, see TestGoldenWithoutGroups), and before T1.10 `f79ae0cb6ac19047` and `841e92a10f1559f3`; each move is caused only by what its re-pin commit names.
- `go test ./...` in dst and `go run ./ops/tools/determinismcheck ./fault/...` in dst passed on `67d894b` (the tree before the squash).
- 2026-11-06, with the pseudo-version and no `replace`: the module has the same twelve importable packages (checked with `go list -deps` in the module cache); the golden hashes of `cpsim -seeds 300` did not move (basic aaf01c86, log-faults 4032d6fb, crash-restart f1e59c13, leader-fencing 0609269d, chaos 951ef86c). That is the check that the squashed code is the audited code.
- `go test ./internal/verify/simulator/...` and `make determinism` after the move.
