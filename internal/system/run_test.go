package system

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/manifesterio/mds-controlplane/internal/runtime"
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

func (e *fakeExec) Execute(a runtime.IOCall) { e.mu.Lock(); e.got = append(e.got, a); e.mu.Unlock() }

// fakeMachine records events and replays script(event index, body).
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

func setup(m *fakeMachine) (Dependencies, *fakeExec, runtime.MachineFactory) {
	b := Dependencies{Clock: fakeClock{}, Inbox: NewInbox(8)}
	return b, &fakeExec{}, func(runtime.Environment, runtime.NodeInstance) runtime.Machine { return m }
}

func TestStartedFirstControlBeatsData(t *testing.T) {
	m := &fakeMachine{}
	b, ex, f := setup(m)
	// Preload both queues before Run: data first, then control.
	b.Inbox.DeliverData(runtime.ObjectGetDone{Req: 1})
	b.Inbox.DeliverControl(runtime.TimerFired{ID: 1})
	m.script = func(n int, body runtime.EventData) []runtime.Action {
		if n == 2 {
			return []runtime.Action{runtime.ShutdownDone{}}
		}
		return nil
	}
	if err := Run(t.Context(), b, ex, f, runtime.NodeInstance{}); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.events[0].Body.(runtime.NodeStarted); !ok {
		t.Fatalf("first = %T", m.events[0].Body)
	}
	if _, ok := m.events[1].Body.(runtime.TimerFired); !ok {
		t.Fatalf("second = %T, control must beat data", m.events[1].Body)
	}
	if m.events[1].At != 7 || m.events[1].Wall != 9 {
		t.Fatalf("not stamped: %+v", m.events[1])
	}
}

func TestTripRunsEarlierActions(t *testing.T) {
	m := &fakeMachine{script: func(n int, _ runtime.EventData) []runtime.Action {
		return []runtime.Action{runtime.ObjectPut{Req: 1}, runtime.StopForRestart{Cause: "x"}, runtime.ObjectPut{Req: 2}}
	}}
	b, ex, f := setup(m)
	err := Run(t.Context(), b, ex, f, runtime.NodeInstance{})
	if !errors.Is(err, ErrStoppedForRestart) {
		t.Fatalf("err = %v", err)
	}
	if len(ex.got) != 1 || ex.got[0].RequestID() != 1 {
		t.Fatalf("got %v", ex.got)
	}
}

func TestExitReturnsNilAndLateDeliveryDropped(t *testing.T) {
	m := &fakeMachine{script: func(int, runtime.EventData) []runtime.Action { return []runtime.Action{runtime.ShutdownDone{}} }}
	b, ex, f := setup(m)
	if err := Run(t.Context(), b, ex, f, runtime.NodeInstance{}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		b.Inbox.DeliverControl(runtime.ObjectPutDone{Req: 1})
		for range 100 { // would block on a full data queue if not dropped
			b.Inbox.DeliverData(runtime.ObjectGetDone{})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("delivery after Run blocked")
	}
	select {
	case <-b.Inbox.Closed():
	default:
		t.Fatal("sink not closed")
	}
}

func TestCtxCancelStops(t *testing.T) {
	m := &fakeMachine{}
	b, ex, f := setup(m)
	ctx, cancel := context.WithCancel(t.Context())
	res := make(chan error, 1)
	go func() { res <- Run(ctx, b, ex, f, runtime.NodeInstance{}) }()
	cancel()
	select {
	case err := <-res:
		if !errors.Is(err, ErrStopped) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestTimerFiresAndEmitFact(t *testing.T) {
	var facts []runtime.Fact
	m := &fakeMachine{}
	b, ex, f := setup(m)
	b.Facts = func(f runtime.Fact) { facts = append(facts, f) }
	m.script = func(n int, body runtime.EventData) []runtime.Action {
		switch n {
		case 0:
			return []runtime.Action{runtime.RecordFact{}, runtime.SetTimer{ID: 3, Gen: 4, After: runtime.Duration(time.Millisecond)}}
		default:
			if tf, ok := body.(runtime.TimerFired); ok && tf.ID == 3 && tf.Gen == 4 {
				return []runtime.Action{runtime.ShutdownDone{}}
			}
		}
		return nil
	}
	if err := Run(t.Context(), b, ex, f, runtime.NodeInstance{}); err != nil {
		t.Fatal(err)
	}
	if len(facts) != 1 {
		t.Fatalf("facts = %v", facts)
	}
}

func TestDispatchTimers(t *testing.T) {
	m := &fakeMachine{}
	b, ex, f := setup(m)
	m.script = func(n int, _ runtime.EventData) []runtime.Action {
		if n != 0 {
			return nil
		}
		return []runtime.Action{
			runtime.SetTimer{ID: 1, After: runtime.Duration(time.Hour)},
			runtime.SetTimer{ID: 1, After: runtime.Duration(time.Hour)}, // replace
			runtime.CancelTimer{ID: 1},
			runtime.CancelTimer{ID: 99}, // missing
			runtime.RecordFact{},        // Facts is nil
			runtime.ShutdownDone{},
		}
	}
	if err := Run(t.Context(), b, ex, f, runtime.NodeInstance{}); err != nil {
		t.Fatal(err)
	}
}

func stops(m *fakeMachine) (int, runtime.StopRequest) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var n int
	var last runtime.StopRequest
	for _, e := range m.events {
		if s, ok := e.Body.(runtime.StopRequest); ok {
			n++
			last = s
		}
	}
	return n, last
}

func TestShutdownStopThenExit(t *testing.T) {
	m := &fakeMachine{}
	m.script = func(_ int, body runtime.EventData) []runtime.Action {
		if _, ok := body.(runtime.StopRequest); ok {
			return []runtime.Action{runtime.ShutdownDone{}}
		}
		return nil
	}
	b, ex, f := setup(m)
	sd := make(chan struct{})
	b.Shutdown, b.StopGrace = sd, runtime.Duration(time.Second)
	res := make(chan error, 1)
	go func() { res <- Run(t.Context(), b, ex, f, runtime.NodeInstance{}) }()
	close(sd)
	if err := <-res; err != nil {
		t.Fatal(err)
	}
	n, s := stops(m)
	if n != 1 || s.Deadline != 7+runtime.NodeTime(time.Second) {
		t.Fatalf("stops=%d deadline=%d", n, s.Deadline)
	}
}

func TestShutdownIgnoredKilled(t *testing.T) {
	m := &fakeMachine{}
	b, ex, f := setup(m)
	sd := make(chan struct{})
	b.Shutdown, b.StopGrace = sd, runtime.Duration(50*time.Millisecond)
	res := make(chan error, 1)
	go func() { res <- Run(t.Context(), b, ex, f, runtime.NodeInstance{}) }()
	close(sd)
	start := time.Now()
	if err := <-res; !errors.Is(err, ErrKilledByRuntime) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("killed before grace elapsed")
	}
	select {
	case <-b.Inbox.Closed():
	default:
		t.Fatal("inbox not closed")
	}
}

func TestShutdownStopDeliveredOnce(t *testing.T) {
	m := &fakeMachine{}
	b, ex, f := setup(m)
	sd := make(chan struct{})
	b.Shutdown, b.StopGrace = sd, runtime.Duration(100*time.Millisecond)
	close(sd)
	for i := range 5 {
		b.Inbox.DeliverControl(runtime.TimerFired{ID: runtime.TimerID(i)})
	}
	err := Run(t.Context(), b, ex, f, runtime.NodeInstance{})
	if !errors.Is(err, ErrKilledByRuntime) {
		t.Fatalf("err = %v", err)
	}
	if n, _ := stops(m); n != 1 {
		t.Fatalf("stops = %d", n)
	}
}
