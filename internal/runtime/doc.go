// Package runtime is the deterministic state-machine runtime: plain-data Events and
// Actions, the StateMachine contract, and the Environment through which the only
// permitted nondeterminism (randomness, buggify) and assertion recording enter.
//
// Rules (enforced by depguard/forbidigo in CI):
//   - no imports of time, math/rand, crypto/rand, os, net, sync, or any SDK;
//   - no goroutines, channels or select;
//   - never let iteration over a map decide behavior;
//   - Events and Actions are plain data; byte slices are immutable once sent.
//
// The control plane's own request and response types are not here. They are in
// internal/controlplane/contract, which imports this package, never the reverse.
//
// See docs/lessons.md and docs/decisions.md.
package runtime
