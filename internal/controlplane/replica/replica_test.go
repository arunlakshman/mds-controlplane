package replica_test

import (
	"github.com/manifesterio/dst/assert"
	"github.com/manifesterio/dst/fault"
	"github.com/manifesterio/dst/rng"
	"github.com/manifesterio/dst/sched"
	"github.com/manifesterio/dst/sim"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadata"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

type alwaysHit struct {
	cond    bool
	prop    runtime.PropertyID
	details []runtime.ContextPair
}

type testEnv struct {
	n      uint64
	always []alwaysHit
}

func (e *testEnv) Uint64() uint64 {
	e.n++
	return e.n
}
func (e *testEnv) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(e.Uint64() % uint64(n))
}
func (e *testEnv) NewID() runtime.ID {
	var id runtime.ID
	u := e.Uint64()
	for i := range id {
		id[i] = byte(u >> (8 * (i % 8)))
	}
	return id
}
func (e *testEnv) Maybe(runtime.FaultPointID) bool           { return false }
func (e *testEnv) MaybeP(runtime.FaultPointID, float64) bool { return false }
func (e *testEnv) Fault(runtime.FaultPointID, float64) bool  { return false }
func (e *testEnv) Always(cond bool, p runtime.PropertyID, details ...runtime.ContextPair) {
	e.always = append(e.always, alwaysHit{cond: cond, prop: p, details: details})
}
func (e *testEnv) AlwaysOrUnreachable(bool, runtime.PropertyID, ...runtime.ContextPair) {}
func (e *testEnv) Sometimes(bool, runtime.PropertyID)                                   {}
func (e *testEnv) Reachable(runtime.PropertyID)                                         {}
func (e *testEnv) Unreachable(runtime.PropertyID, ...runtime.ContextPair)               {}

// timerView is what a test reads from a timer request: which timer, its
// generation, and when it fires.
type timerView struct {
	ID    runtime.TimerID
	Gen   runtime.TimerVersion
	After runtime.Duration
}

// asTimer reads a timer request. The replica is moving from runtime.SetTimer
// to dst's sim.SetTimer one concern at a time, so both are read for now.
func asTimer(a any) (timerView, bool) {
	switch st := a.(type) {
	case sim.SetTimer:
		fired, _ := st.Body.(runtime.TimerFired)
		return timerView{ID: fired.ID, Gen: fired.Gen, After: runtime.Duration(st.After)}, true
	case runtime.SetTimer:
		return timerView{ID: st.ID, Gen: st.Gen, After: st.After}, true
	}
	return timerView{}, false
}

func mustTimer(t *testing.T, a any) timerView {
	t.Helper()
	tv, ok := asTimer(a)
	if !ok {
		t.Fatalf("%T is not a timer request", a)
	}
	return tv
}

func newMachine(t *testing.T, config replica.Config) *replica.Replica {
	t.Helper()
	m, _ := newMachineEnv(t, config)
	return m
}

func newMachineEnv(t *testing.T, config replica.Config) (*replica.Replica, *testEnv) {
	t.Helper()
	env := &testEnv{}
	return replica.New(config)(env, runtime.NodeInstance{Node: "cp-0"}), env
}

// testContext is the sim.Context of a unit test. The replica only reads the time from it.
type testContext struct{ now sched.TimeNs }

func (c testContext) Now() sched.TimeNs     { return c.now }
func (testContext) Wall() sched.TimeNs      { return 0 }
func (testContext) Rand() *rng.Rand         { return rng.New(1) }
func (testContext) Faults() *fault.Injector { return fault.New(rng.New(1)) }
func (testContext) Assert() *assert.Log     { return assert.New(nil) }

// stepAt gives body to the replica at time at.
func stepAt(m *replica.Replica, at runtime.NodeTime, body runtime.EventData) []any {
	return m.Step(testContext{now: sched.TimeNs(at)}, body)
}

// step gives body to the replica at time zero.
func step(m *replica.Replica, body runtime.EventData) []any { return stepAt(m, 0, body) }

func noop(id string) []byte {
	return metadata.Encode(metadata.Entry{ID: id, Kind: metadata.KindNoop})
}

func TestStartedAndStop(t *testing.T) {
	m := newMachine(t, replica.Config{})
	if acts := step(m, runtime.NodeStarted{Self: runtime.NodeInstance{Node: "cp-0"}}); acts != nil {
		t.Fatalf("Started returned actions %v", acts)
	}
	beforeStop := m.StateHash()
	acts := step(m, runtime.StopRequest{})
	if len(acts) != 1 {
		t.Fatalf("Stop actions = %v, want [Exit]", acts)
	}
	if _, ok := acts[0].(sim.Exit); !ok {
		t.Fatalf("Stop action = %T, want sim.Exit", acts[0])
	}
	if m.StateHash() == beforeStop {
		t.Fatal("Stop left StateHash unchanged")
	}
	after := m.StateHash()
	if acts := step(m, runtime.StopRequest{}); acts != nil {
		t.Fatalf("second Stop after Exit returned %v", acts)
	}
	if m.StateHash() != after {
		t.Fatal("post-Exit Stop changed StateHash")
	}
}

func TestStateHashStableAcrossNoopIOResults(t *testing.T) {
	m := newMachine(t, replica.Config{})
	step(m, runtime.NodeStarted{})
	before := m.StateHash()
	noops := []runtime.EventData{
		runtime.ControlPlaneDone{Req: 999},
		runtime.ObjectPutDone{Req: 999},
		runtime.ObjectGetDone{Req: 999},
		runtime.LogAppendDone{Req: 999},
		runtime.LogReadDone{Req: 999},
		runtime.LogGetEndDone{Req: 999},
		runtime.LogDeleteOldDone{Req: 999},
		runtime.TimerFired{ID: 77, Gen: 3},
	}
	for _, body := range noops {
		if acts := step(m, body); acts != nil {
			t.Fatalf("%T returned actions %v", body, acts)
		}
		if m.StateHash() != before {
			t.Fatalf("%T changed StateHash", body)
		}
	}
	step(m, runtime.NodeStarted{})
	if m.StateHash() != before {
		t.Fatal("repeat Started changed StateHash")
	}
}

func TestStateHashDeterministicAcrossBoots(t *testing.T) {
	hash := func() uint64 {
		m := newMachine(t, replica.Config{})
		step(m, runtime.NodeStarted{})
		return m.StateHash()
	}
	if a, b := hash(), hash(); a != b {
		t.Fatalf("fresh boots hashed differently: %d vs %d", a, b)
	}
}

func TestNewImplementsFactory(t *testing.T) {
	f := replica.New(replica.Config{})
	if f(&testEnv{}, runtime.NodeInstance{Node: "cp-1"}) == nil {
		t.Fatal("Factory returned nil")
	}
}

func TestCatchUpAppliesContiguousReads(t *testing.T) {
	m := newMachine(t, replica.Config{CatchUpEvery: runtime.Millisecond, ReadLimit: 8})

	acts := step(m, runtime.NodeStarted{})
	if len(acts) != 1 {
		t.Fatalf("Started actions = %v, want [SetTimer]", acts)
	}
	timer, ok := asTimer(acts[0])
	if !ok || timer.ID == 0 {
		t.Fatalf("Started action = %T %#v, want SetTimer", acts[0], acts[0])
	}

	acts = step(m, runtime.TimerFired{ID: timer.ID, Gen: timer.Gen})
	if len(acts) != 1 {
		t.Fatalf("TimerFired actions = %v, want [LogCheckTail]", acts)
	}
	tail, ok := acts[0].(runtime.LogGetEnd)
	if !ok || tail.Req == 0 {
		t.Fatalf("TimerFired action = %T %#v, want LogCheckTail", acts[0], acts[0])
	}

	acts = step(m, runtime.LogGetEndDone{Req: tail.Req, Tail: 2, Err: runtime.OK, Outcome: runtime.Applied})
	if len(acts) != 1 {
		t.Fatalf("CheckTailDone actions = %v, want [LogRead]", acts)
	}
	read, ok := acts[0].(runtime.LogRead)
	if !ok || read.From != 0 || read.Limit != 8 {
		t.Fatalf("CheckTailDone action = %#v, want LogRead From=0 Limit=8", acts[0])
	}

	acts = step(m, runtime.LogReadDone{
		Req: read.Req, Err: runtime.OK, Outcome: runtime.Applied,
		Entries: []runtime.LogEntry{
			{Slot: 0, Data: noop("a")},
			{Slot: 1, Data: noop("b")},
		},
	})
	if len(acts) != 1 {
		t.Fatalf("ReadDone actions = %v, want [SetTimer]", acts)
	}
	if _, ok := asTimer(acts[0]); !ok {
		t.Fatalf("ReadDone action = %T, want SetTimer", acts[0])
	}
	if m.Applied() != 2 {
		t.Fatalf("Applied = %d, want 2", m.Applied())
	}

	before := m.StateHash()
	if acts := step(m, runtime.LogGetEndDone{Req: 999, Tail: 99, Err: runtime.OK, Outcome: runtime.Applied}); acts != nil {
		t.Fatalf("foreign CheckTailDone returned %v", acts)
	}
	if m.StateHash() != before {
		t.Fatal("foreign completion changed StateHash")
	}
}

func TestCatchUpDisabledByDefault(t *testing.T) {
	m := newMachine(t, replica.Config{})
	if acts := step(m, runtime.NodeStarted{}); acts != nil {
		t.Fatalf("default Started returned %v", acts)
	}
}
