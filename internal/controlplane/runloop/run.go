package runloop

import (
	"context"
	"errors"
	"fmt"

	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

var (
	// ErrStoppedForRestart is returned when the run was stopped by a tripped guard.
	ErrStoppedForRestart = errors.New("tripped")
	// ErrStopped is returned when Run ends because its context was done.
	ErrStopped = errors.New("stopped")
	// ErrKilledByRuntime is returned when a graceful stop missed its deadline.
	ErrKilledByRuntime = errors.New("killed")
)

// Executor runs I/O only. Exactly one IOResult per IOCall, delivered to
// the InboxWriter and never on the caller's goroutine. Execute MUST NOT block.
type Executor interface {
	Execute(a runtime.IOCall)
}

// InjectFunc runs on the loop goroutine and returns actions to dispatch
// (Offer, TakeAnswers bookkeeping). Optional on Dependencies.Inject.
type InjectFunc func() []runtime.Action

// Dependencies is everything nondeterministic a control-plane process needs.
type Dependencies struct {
	Clock     runtime.Clock
	Env       runtime.Environment
	Inbox     *Inbox
	Facts     func(runtime.Fact)
	Shutdown  <-chan struct{}
	StopGrace runtime.Duration
	// Inject, if non-nil, carries work that must run on the loop goroutine
	// (gRPC adapter Offer/reads). Checked every turn and while waiting.
	Inject <-chan InjectFunc
	// AfterStep runs on the loop goroutine after each Step or inject
	// dispatch. Used to drain TakeAnswers to waiting RPCs.
	AfterStep func(m runtime.Machine)
}

// Run delivers Started, then loops: ControlFirst picks, At/Wall stamped at
// dequeue, Step, actions dispatched in slice order. Returns ErrStoppedForRestart
// (wrapped) on Trip, nil on Exit, ErrStopped on ctx done, ErrKilledByRuntime when
// StopGrace expires after Shutdown. Closes b.Inbox on return.
func Run(ctx context.Context, b Dependencies, exec Executor, f runtime.MachineFactory, self runtime.NodeInstance) error {
	in := b.Inbox
	if in == nil {
		return errors.New("controlplane/runloop: Bundle.Inbox is nil")
	}
	defer in.Close()
	m := f(b.Env, self)
	r := &eventLoop{b: b, in: in, exec: exec, m: m, timers: map[runtime.TimerID]runtime.Timer{}}
	defer func() {
		for _, t := range r.timers {
			t.Stop()
		}
	}()

	step := func(body runtime.EventData) (bool, error) {
		acts := m.Step(runtime.Event{At: b.Clock.Mono(), Wall: b.Clock.Wall(), Body: body})
		return r.dispatch(acts)
	}
	if done, err := step(runtime.NodeStarted{Self: self}); done {
		return err
	}
	var policy runtime.InboxPicker = runtime.ControlFirstPicker{}
	ctrl, data := in.Control(), in.Data()
	shutdown := b.Shutdown
	var kill <-chan struct{}
	for {
		if ctx.Err() != nil {
			return ErrStopped
		}
		select {
		case <-shutdown:
			shutdown = nil
			in.DeliverControl(runtime.StopRequest{Deadline: b.Clock.Mono() + runtime.NodeTime(b.StopGrace)})
			killed := make(chan struct{})
			t := b.Clock.AfterFunc(runtime.Duration(b.StopGrace), func() { close(killed) })
			defer t.Stop()
			kill = killed
		default:
		}
		select {
		case <-kill:
			return ErrKilledByRuntime
		default:
		}
		if done, err := r.drainInject(); done {
			return err
		}
		body, ok := policy.Next(ctrl, data)
		if !ok {
			if done, err := r.waitIdle(ctx, &shutdown, kill); done {
				return err
			}
			continue
		}
		if done, err := step(body); done {
			return err
		}
		if done, err := r.drainInject(); done {
			return err
		}
	}
}

func (r *eventLoop) waitIdle(ctx context.Context, shutdown *<-chan struct{}, kill <-chan struct{}) (bool, error) {
	select {
	case <-ctx.Done():
		return true, ErrStopped
	case <-*shutdown:
		return false, nil
	case <-kill:
		return true, ErrKilledByRuntime
	case fn := <-r.b.Inject:
		return r.runInject(fn)
	case <-r.in.wake:
		return false, nil
	}
}

type eventLoop struct {
	b      Dependencies
	in     *Inbox
	exec   Executor
	m      runtime.Machine
	timers map[runtime.TimerID]runtime.Timer
}

func (r *eventLoop) after() {
	if r.b.AfterStep != nil {
		r.b.AfterStep(r.m)
	}
}

func (r *eventLoop) drainInject() (bool, error) {
	if r.b.Inject == nil {
		return false, nil
	}
	for {
		select {
		case fn := <-r.b.Inject:
			if done, err := r.runInject(fn); done {
				return true, err
			}
		default:
			return false, nil
		}
	}
}

func (r *eventLoop) runInject(fn InjectFunc) (bool, error) {
	if fn == nil {
		return false, nil
	}
	return r.dispatch(fn())
}

func (r *eventLoop) dispatch(acts []runtime.Action) (bool, error) {
	for _, a := range acts {
		switch a := a.(type) {
		case runtime.IOCall:
			r.exec.Execute(a)
		case runtime.RecordFact:
			if r.b.Facts != nil {
				r.b.Facts(a.Fact)
			}
		case runtime.SetTimer:
			r.setTimer(a)
		case runtime.CancelTimer:
			r.cancelTimer(a.ID)
		case runtime.StopForRestart:
			r.after()
			return true, fmt.Errorf("%w: %s", ErrStoppedForRestart, a.Cause)
		case runtime.ShutdownDone:
			r.after()
			return true, nil
		default:
			r.after()
			return true, fmt.Errorf("controlplane/runloop: unknown action %T", a)
		}
	}
	r.after()
	return false, nil
}

func (r *eventLoop) setTimer(a runtime.SetTimer) {
	if old := r.timers[a.ID]; old != nil {
		old.Stop()
	}
	id, gen := a.ID, a.Gen
	r.timers[id] = r.b.Clock.AfterFunc(a.After, func() {
		r.in.DeliverControl(runtime.TimerFired{ID: id, Gen: gen})
	})
}

func (r *eventLoop) cancelTimer(id runtime.TimerID) {
	if t := r.timers[id]; t != nil {
		t.Stop()
		delete(r.timers, id)
	}
}
