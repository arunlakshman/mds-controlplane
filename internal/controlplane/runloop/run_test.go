package runloop_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/runloop"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
	"github.com/manifesterio/mds-controlplane/internal/system/fakeclock"
)

type fakeClock struct{}

func (fakeClock) Mono() runtime.NodeTime { return 7 }
func (fakeClock) Wall() runtime.RealTime { return 9 }
func (fakeClock) AfterFunc(d runtime.Duration, f func()) runtime.Timer {
	return time.AfterFunc(time.Duration(d), f)
}

type fakeExec struct {
	mu  sync.Mutex
	got []runtime.IOCall
}

func (e *fakeExec) Execute(a runtime.IOCall) {
	e.mu.Lock()
	e.got = append(e.got, a)
	e.mu.Unlock()
}

type fakeMachine struct {
	mu     sync.Mutex
	events []runtime.Event
	script func(n int, b runtime.EventData) []runtime.Action
}

func (m *fakeMachine) Step(e runtime.Event) []runtime.Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := len(m.events)
	m.events = append(m.events, e)
	if m.script == nil {
		return nil
	}
	return m.script(n, e.Body)
}
func (m *fakeMachine) StateHash() uint64 { return 0 }

type testEnv struct{}

func (testEnv) Uint64() uint64                                                       { return 1 }
func (testEnv) Intn(n int) int                                                       { return 0 }
func (testEnv) NewID() runtime.ID                                                    { return runtime.ID{} }
func (testEnv) Maybe(runtime.FaultPointID) bool                                      { return false }
func (testEnv) MaybeP(runtime.FaultPointID, float64) bool                            { return false }
func (testEnv) Fault(runtime.FaultPointID, float64) bool                             { return false }
func (testEnv) Always(bool, runtime.PropertyID, ...runtime.ContextPair)              {}
func (testEnv) AlwaysOrUnreachable(bool, runtime.PropertyID, ...runtime.ContextPair) {}
func (testEnv) Sometimes(bool, runtime.PropertyID)                                   {}
func (testEnv) Reachable(runtime.PropertyID)                                         {}
func (testEnv) Unreachable(runtime.PropertyID, ...runtime.ContextPair)               {}

func TestStartedThenExit(t *testing.T) {
	m := &fakeMachine{}
	m.script = func(n int, _ runtime.EventData) []runtime.Action {
		if n == 0 {
			return []runtime.Action{runtime.ShutdownDone{}}
		}
		return nil
	}
	b := runloop.Dependencies{Clock: fakeClock{}, Env: testEnv{}, Inbox: runloop.NewInbox(4)}
	f := func(runtime.Environment, runtime.NodeInstance) runtime.Machine { return m }
	if err := runloop.Run(t.Context(), b, &fakeExec{}, f, runtime.NodeInstance{Node: "cp"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.events[0].Body.(runtime.NodeStarted); !ok {
		t.Fatalf("first = %T", m.events[0].Body)
	}
}

func TestInjectRunsOnLoop(t *testing.T) {
	m := &fakeMachine{}
	inject := make(chan runloop.InjectFunc, 1)
	var saw bool
	m.script = func(n int, body runtime.EventData) []runtime.Action {
		if n == 1 {
			return []runtime.Action{runtime.ShutdownDone{}}
		}
		return nil
	}
	b := runloop.Dependencies{
		Clock: fakeClock{}, Env: testEnv{}, Inbox: runloop.NewInbox(4), Inject: inject,
	}
	f := func(runtime.Environment, runtime.NodeInstance) runtime.Machine { return m }
	done := make(chan error, 1)
	go func() { done <- runloop.Run(t.Context(), b, &fakeExec{}, f, runtime.NodeInstance{}) }()
	inject <- func() []runtime.Action {
		saw = true
		b.Inbox.DeliverControl(runtime.TimerFired{ID: 1})
		return nil
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	if !saw {
		t.Fatal("inject did not run")
	}
}

func TestWaitIdleThenExit(t *testing.T) {
	m := &fakeMachine{}
	inject := make(chan runloop.InjectFunc)
	b := runloop.Dependencies{Clock: fakeClock{}, Env: testEnv{}, Inbox: runloop.NewInbox(4), Inject: inject}
	f := func(runtime.Environment, runtime.NodeInstance) runtime.Machine { return m }
	done := make(chan error, 1)
	go func() { done <- runloop.Run(t.Context(), b, &fakeExec{}, f, runtime.NodeInstance{}) }()
	time.Sleep(20 * time.Millisecond) // park in waitIdle
	inject <- func() []runtime.Action { return []runtime.Action{runtime.ShutdownDone{}} }
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestDeliverDataAndDispatch(t *testing.T) {
	in := runloop.NewInbox(2)
	in.DeliverData(runtime.TimerFired{ID: 9})
	if in.Data().Len() != 1 {
		t.Fatal("data not queued")
	}
	m := &fakeMachine{}
	m.script = func(n int, body runtime.EventData) []runtime.Action {
		switch n {
		case 0: // Started
			return []runtime.Action{
				runtime.SetTimer{ID: 1, Gen: 1, After: runtime.Duration(time.Hour)},
				runtime.CancelTimer{ID: 1},
				runtime.LogAppend{Req: 1},
			}
		case 1:
			return []runtime.Action{runtime.ShutdownDone{}}
		}
		return nil
	}
	b := runloop.Dependencies{
		Clock: fakeClock{}, Env: testEnv{}, Inbox: in,
	}
	ex := &fakeExec{}
	if err := runloop.Run(t.Context(), b, ex, func(runtime.Environment, runtime.NodeInstance) runtime.Machine { return m }, runtime.NodeInstance{}); err != nil {
		t.Fatal(err)
	}
	if len(ex.got) != 1 {
		t.Fatalf("io=%d", len(ex.got))
	}
}

// A machine that ignores the stop request is killed when the grace time ends.
// The grace timer comes from the clock, so the test moves it by hand.
func TestShutdownGraceExpiryKills(t *testing.T) {
	clock := fakeclock.New(time.Unix(1000, 0))
	shutdown := make(chan struct{})
	b := runloop.Dependencies{
		Clock: clock, Env: testEnv{}, Inbox: runloop.NewInbox(4),
		Shutdown: shutdown, StopGrace: runtime.Duration(time.Second),
	}
	f := func(runtime.Environment, runtime.NodeInstance) runtime.Machine { return &fakeMachine{} }
	done := make(chan error, 1)
	go func() { done <- runloop.Run(t.Context(), b, &fakeExec{}, f, runtime.NodeInstance{Node: "cp"}) }()
	close(shutdown)
	for clock.Pending() == 0 { // the loop has armed the grace timer
		time.Sleep(time.Millisecond)
	}
	clock.Advance(2 * time.Second)
	if err := <-done; !errors.Is(err, runloop.ErrKilledByRuntime) {
		t.Fatalf("got %v, want ErrKilledByRuntime", err)
	}
}
