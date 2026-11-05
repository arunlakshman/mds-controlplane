# github.com/manifesterio/dst@v0.3.0

- Date / auditor: 2026-11-04, retarget of mds-controlplane cpsim onto OSS dst (module moved from `github.com/arunlakshman/dst`, v0.0.0, to the manifesterio org).
- Requires Go: 1.26+
- Verdict: **ACCEPTED** for `internal/verify/simulator` (and future deterministic packages that need the same primitives).
- v0.2.0 added `check` (generic oracle checks over a log; imports only `assert` and `fmt`; no goroutines, time or OS). Inside the deterministic gate of dst.
- 2026-11-05 (T1.10): three local commits on dst branch `dst-process` (`e0a59b9`, `7cf72fe`, `67d894b`), still tagged v0.3.0 and not pushed. `fault` gains `Injector.Sites()` and `SiteCount{On, Calls, Fired}` (counts of `Maybe`/`MaybeP` sites). `campaign` gains `Result.Sites`, `Summary.Sites`, `SiteTotal`, `NeverFired`, and `Merge` adds sites. No new import in our code, no change to the allow-list, no new module. `campaign` now imports `fault` inside dst (both already on the list). The counts draw nothing from the stream. See the finding below.
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
| Tag `v0.3.0` is local; `go.mod` carries a temporary `replace` to `/Users/arun.lakshman/tmp/dst` | Yes (process) | The repo is not pushed to manifesterio yet. Remove the `replace` once the tag is published; re-audit on any version bump. |

## Rules for our code
1. Import only the twelve packages above from the simulator and other determinism-gated code. Do not pull examples.
2. Keep CP scenario / node / oracle control flow local; dst owns the shared stream primitives only.
3. Quiet must go through `fault.Injector.SetQuiet` (suppresses `Fault` only).

## Evidence
- Corpus gate (since T1.10, with the simulator buggify sites): `cpsim -scenario basic -seed 17` -> `hash=08bfa86385d8e8ae`; `cpsim -scenario chaos -seed 42` -> `hash=45d210abda0e2bed`. Before T1.10 these were `f79ae0cb6ac19047` and `841e92a10f1559f3`; the move is caused only by the new sites, see the re-pin commit.
- `go test ./...` in dst and `go run ./ops/tools/determinismcheck ./fault/...` in dst pass on `67d894b`.
- `go test ./internal/verify/simulator/...` and `make determinism` after the move.
