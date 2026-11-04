# github.com/arunlakshman/dst@v0.0.0

- Date / auditor: 2026-11-04, retarget of mds-controlplane cpsim onto OSS dst.
- Requires Go: 1.22+
- Verdict: **ACCEPTED** for `internal/verify/simulator` (and future deterministic packages that need the same primitives).
- Import paths allowed (`ops/tools/determinismcheck/allow.txt`): `github.com/arunlakshman/dst/...`.

## Findings
| Finding | Matters? | Why |
|---|---|---|
| Packages are `rng`, `sched`, `trace`, `assert`, `fault` only | No | No wall clock, OS, sync, or network. Same SplitMix / heap / FNV / Maybe latch as the former CP-local bodies. |
| Local `replace` to `/Users/arun.lakshman/tmp/dst` at commit `452e9bd` | Yes (process) | Module is not yet published; pin via replace until a tagged release. Re-audit on version bump. |

## Rules for our code
1. Import only the five packages above from the simulator (and other determinism-gated code). Do not pull examples.
2. Keep CP scenario / node / oracle / campaign control flow local; dst owns the shared stream primitives only.
3. Quiet must go through `fault.Injector.SetQuiet` (suppresses `Fault` only).

## Evidence
- Graft 1 corpus gate: `cpsim -scenario basic -seed 17` → `hash=180fbfc68e5f9ab8`; `cpsim -scenario chaos -seed 42` → `hash=e5384ad2691ef87e`.
- `go test ./internal/verify/simulator/...` and `make determinism` after retarget.
