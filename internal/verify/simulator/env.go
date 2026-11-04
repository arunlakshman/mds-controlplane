package simulator

import (
	"fmt"
	"strings"

	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Assertion kinds, as they appear in Coverage.
const (
	kindAlways    = "always"
	kindSometimes = "sometimes"
	kindReachable = "reachable"
)

// Coverage counts how often one property was evaluated. A campaign adds the
// numbers of all its runs.
type Coverage struct {
	Kind   string
	Checks uint64
	Passes uint64
}

// env is the runtime.Environment of a run: the random stream, the fault
// points, and the assertions. Assertions never panic: a false Always is
// recorded as a failure and the run goes on.
type env struct {
	rand  *Rand
	fail  func(property, detail string)
	quiet bool // true in the quiet tail: Fault never fires
	stats map[runtime.PropertyID]*Coverage
	seen  map[string]bool // activation of Maybe sites, per run
}

func newEnv(rand *Rand, fail func(property, detail string)) *env {
	return &env{rand: rand, fail: fail, stats: map[runtime.PropertyID]*Coverage{}, seen: map[string]bool{}}
}

var _ runtime.Environment = (*env)(nil)

// Uint64 implements runtime.Rand.
func (e *env) Uint64() uint64 { return e.rand.Uint64() }

// Intn implements runtime.Rand.
func (e *env) Intn(n int) int { return e.rand.Intn(n) }

// NewID implements runtime.Rand.
func (e *env) NewID() runtime.ID { return e.rand.NewID() }

// Maybe implements runtime.FaultInjector. A site is switched on for the whole
// run with probability one half, and then fires on 5 percent of the calls.
func (e *env) Maybe(site runtime.FaultPointID) bool {
	on, known := e.seen[string(site)]
	if !known {
		on = e.rand.Chance(0.5)
		e.seen[string(site)] = on
	}
	return on && e.rand.Chance(0.05)
}

// MaybeP implements runtime.FaultInjector.
func (e *env) MaybeP(_ runtime.FaultPointID, p float64) bool { return e.rand.Chance(p) }

// Fault implements runtime.FaultInjector. It never fires in the quiet tail.
func (e *env) Fault(_ runtime.FaultPointID, p float64) bool {
	return !e.quiet && e.rand.Chance(p)
}

func (e *env) stat(p runtime.PropertyID, kind string) *Coverage {
	s := e.stats[p]
	if s == nil {
		s = &Coverage{Kind: kind}
		e.stats[p] = s
	}
	return s
}

func detailText(details []runtime.ContextPair) string {
	var b strings.Builder
	for _, d := range details {
		fmt.Fprintf(&b, " %s=%v", d.Key, d.Value)
	}
	return strings.TrimSpace(b.String())
}

// Always implements runtime.Assertions.
func (e *env) Always(cond bool, p runtime.PropertyID, details ...runtime.ContextPair) {
	s := e.stat(p, kindAlways)
	s.Checks++
	if cond {
		s.Passes++
		return
	}
	e.fail(string(p), detailText(details))
}

// AlwaysOrUnreachable implements runtime.Assertions.
func (e *env) AlwaysOrUnreachable(cond bool, p runtime.PropertyID, details ...runtime.ContextPair) {
	e.Always(cond, p, details...)
}

// Sometimes implements runtime.Assertions.
func (e *env) Sometimes(cond bool, p runtime.PropertyID) {
	s := e.stat(p, kindSometimes)
	s.Checks++
	if cond {
		s.Passes++
	}
}

// Reachable implements runtime.Assertions.
func (e *env) Reachable(p runtime.PropertyID) {
	s := e.stat(p, kindReachable)
	s.Checks++
	s.Passes++
}

// Unreachable implements runtime.Assertions.
func (e *env) Unreachable(p runtime.PropertyID, details ...runtime.ContextPair) {
	e.fail(string(p), "reached an Unreachable point "+detailText(details))
}

// coverage returns a copy of the stats. Callers sort the keys when they print.
func (e *env) coverage() map[string]Coverage {
	out := make(map[string]Coverage, len(e.stats))
	for p, s := range e.stats { //determinismcheck:ok copy only; the order of a map copy is not observable
		out[string(p)] = *s
	}
	return out
}
