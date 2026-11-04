package simulator

import (
	"fmt"

	"github.com/arunlakshman/dst/trace"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Config is one run.
type Config struct {
	Seed     uint64
	Scenario string            // a name from ScenarioNames; empty means "basic"
	Nodes    int               // number of replicas; zero means 3
	Steps    int               // events in the fault phase; zero means DefaultSteps
	Bug      Bug               // a planted bug; normally BugNone
	Trace    func(line string) // when set, every step is passed to it
}

// DefaultSteps is the length of the fault phase of a run, in scheduler events.
const DefaultSteps = 4000

// Failure is one broken property.
type Failure struct {
	Property string
	Detail   string
	Step     int
	TimeMs   int64
	Repeats  int // later hits of the same property in the same run
}

// Stats counts what happened in a run.
type Stats struct {
	Ops, Acked, Rejected, GaveUp, Retries, ReadsOK int
	Crashes, Trips, Pauses, Partitions, Boots      int
	FailedCalls, LostReplies, CutCalls             int
	LogSlots                                       int
	QuietMs                                        int64
}

// Result is the outcome of one run.
type Result struct {
	Seed     uint64
	Scenario string
	Hash     uint64 // the trace hash: same seed, same hash
	Steps    int
	EndMs    int64
	Failures []Failure
	Stats    Stats
	Coverage map[string]Coverage
	Tail     []string // the last trace lines, kept when the run fails
}

// OK reports that no property was broken.
func (r Result) OK() bool { return len(r.Failures) == 0 }

// Reproduce is the command that runs this seed again, with a trace.
func (r Result) Reproduce() string {
	return fmt.Sprintf("go run ./cmd/cpsim -scenario %s -seed %d", r.Scenario, r.Seed)
}

func nodeWriter(idx int) string { return fmt.Sprintf("cp-%d", idx) }

// seenPrefix is the first state hash that a node showed at some log prefix.
type seenPrefix struct {
	hash uint64
	node int
}

// sim is the state of one run.
type sim struct {
	cfg      Config
	scenario Scenario
	rand     *Rand
	sched    *scheduler
	env      *env
	store    *storage
	nodes    []*simNode
	clients  *clientPool
	oracle   *oracle

	faults Faults // the faults that are on now; zero in the quiet tail
	quiet  bool
	bug    Bug

	hash     trace.Hash
	stats    Stats
	failures []Failure
	byProp   map[string]int // property -> index in failures

	holders  map[uint64]int // writer epoch -> node that held it
	prefixes map[runtime.LogPosition]seenPrefix
	applied  []runtime.LogPosition // last applied slot seen for each node
	fresh    []bool                // the node has a new incarnation, so its hash must be checked again

	ring [traceRing]traceLine
	rpos int
}

func newSim(cfg Config) (*sim, error) {
	sc, ok := ScenarioByName(cfg.Scenario)
	if !ok {
		return nil, fmt.Errorf("unknown scenario %q (have %v)", cfg.Scenario, ScenarioNames())
	}
	if cfg.Nodes <= 0 {
		cfg.Nodes = 3
	}
	if cfg.Steps <= 0 {
		cfg.Steps = DefaultSteps
	}
	cfg.Scenario = sc.Name
	s := &sim{
		cfg: cfg, scenario: sc, rand: NewRand(cfg.Seed), faults: sc.Faults, bug: cfg.Bug,
		byProp: map[string]int{}, holders: map[uint64]int{}, prefixes: map[runtime.LogPosition]seenPrefix{},
		applied: make([]runtime.LogPosition, cfg.Nodes), fresh: make([]bool, cfg.Nodes),
		hash: trace.New(),
	}
	s.sched = newScheduler(s.rand)
	s.env = newEnv(s.rand, s.addFailure)
	s.store = newStorage(s)
	s.oracle = newOracle(s)
	s.clients = newClientPool(s, 3)
	for i := range cfg.Nodes {
		s.nodes = append(s.nodes, newNode(s, i, replicaFactory(i, sc.Faults, cfg.Bug)))
	}
	return s, nil
}

func (s *sim) addFailure(property, detail string) {
	if i, seen := s.byProp[property]; seen {
		s.failures[i].Repeats++
		return
	}
	s.byProp[property] = len(s.failures)
	s.failures = append(s.failures, Failure{Property: property, Detail: detail, Step: s.sched.steps(), TimeMs: s.sched.now() / int64(runtime.Millisecond)})
}

// violation records a broken property. The run goes on.
func (s *sim) violation(property, format string, args ...any) {
	s.env.Always(false, runtime.PropertyID("sim: "+property), runtime.ContextPair{Key: "detail", Value: fmt.Sprintf(format, args...)})
}

// verdict records a check. It returns ok, so the caller can skip work after a failure.
// Callers on a hot path test the condition first and call violation themselves.
func (s *sim) verdict(ok bool, property, format string, args ...any) bool {
	if ok {
		s.env.Always(true, runtime.PropertyID("sim: "+property))
		return true
	}
	s.violation(property, format, args...)
	return false
}

func (s *sim) noteBoot(n *simNode) {
	s.stats.Boots++
	s.fresh[n.idx] = true
	s.tracef("%s boots (boot %d)", n.name(), n.boots)
}

// observe runs after every Step: it folds the state into the trace hash and
// checks the properties that can be checked on one node.
func (s *sim) observe(n *simNode, body runtime.EventData, acts []runtime.Action) {
	h := &s.hash
	h.Uint64(uint64(s.sched.now()))
	h.Uint64(uint64(n.idx))
	h.Uint64(n.m.StateHash())
	h.Uint64(uint64(len(acts)))
	s.ring[s.rpos%traceRing] = traceLine{step: s.sched.steps(), at: s.sched.now(), node: n.idx, body: body, acts: len(acts)}
	s.rpos++
	if s.cfg.Trace != nil {
		s.cfg.Trace(s.ring[(s.rpos-1)%traceRing].String())
	}
	s.checkNode(n)
}

// checkNode checks the epoch and the state of one node. An epoch has one
// holder. After the same number of applied slots, all nodes have the same state.
func (s *sim) checkNode(n *simNode) {
	r := n.replica()
	if r == nil {
		return
	}
	if e := r.Epoch(); e > 0 {
		if first, seen := s.holders[e]; seen && first != n.idx {
			s.violation("one writer per epoch", "epoch %d is held by cp-%d and by cp-%d", e, first, n.idx)
		} else {
			s.holders[e] = n.idx
		}
	}
	applied := r.Applied()
	if applied == s.applied[n.idx] && !s.fresh[n.idx] {
		return
	}
	s.applied[n.idx], s.fresh[n.idx] = applied, false
	h := r.Metadata().StateHash()
	prev, seen := s.prefixes[applied]
	switch {
	case !seen:
		s.prefixes[applied] = seenPrefix{hash: h, node: n.idx}
	case prev.hash != h:
		s.violation("replicas agree on every slot", "after %d slots, cp-%d has state %x but cp-%d had %x", applied, n.idx, h, prev.node, prev.hash)
	}
}
