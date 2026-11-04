# github.com/manifesterio/dst@v0.3.0

- Date / auditor: 2026-11-04, retarget of mds-controlplane cpsim onto OSS dst (module moved from `github.com/arunlakshman/dst`, v0.0.0, to the manifesterio org).
- Requires Go: 1.26+
- Verdict: **ACCEPTED** for `internal/verify/simulator` (and future deterministic packages that need the same primitives).
- v0.2.0 added `check` (generic oracle checks over a log; imports only `assert` and `fmt`; no goroutines, time or OS). Inside the deterministic gate of dst.
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
| Tag `v0.3.0` is local; `go.mod` carries a temporary `replace` to `/Users/arun.lakshman/tmp/dst` | Yes (process) | The repo is not pushed to manifesterio yet. Remove the `replace` once the tag is published; re-audit on any version bump. |

## Rules for our code
1. Import only the twelve packages above from the simulator and other determinism-gated code. Do not pull examples.
2. Keep CP scenario / node / oracle control flow local; dst owns the shared stream primitives only.
3. Quiet must go through `fault.Injector.SetQuiet` (suppresses `Fault` only).

## Evidence
- Corpus gate: `cpsim -scenario basic -seed 17` -> `hash=180fbfc68e5f9ab8`; `cpsim -scenario chaos -seed 42` -> `hash=e5384ad2691ef87e`.
- `go test ./internal/verify/simulator/...` and `make determinism` after the move.
