package replica

import (
	"github.com/manifesterio/dst/sched"
	"github.com/manifesterio/dst/sim"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Process runs a Replica as a dst sim.Process. It is an adapter while the
// replica still speaks runtime.Event and runtime.Action: Step stamps the
// event with the process time, calls the replica, and turns the actions into
// dst values (Outbound). When the replica becomes a Process itself, this file goes.
type Process struct {
	id sim.ProcessID
	m  *Replica
}

var _ sim.Process = (*Process)(nil)

// AsProcess wraps m. id names the process in logs and in Send.
func AsProcess(id string, m *Replica) *Process { return &Process{id: sim.ProcessID(id), m: m} }

// ID implements sim.Process.
func (p *Process) ID() sim.ProcessID { return p.id }

// StateHash implements sim.Process.
func (p *Process) StateHash() uint64 { return p.m.StateHash() }

// Step implements sim.Process. The inbox value must be a runtime.EventData.
func (p *Process) Step(cx sim.Context, inbox any) []any {
	body, ok := inbox.(runtime.EventData)
	if !ok {
		return nil
	}
	return Outbound(p.m.Step(runtime.Event{At: runtime.NodeTime(cx.Now()), Wall: runtime.RealTime(cx.Wall()), Body: body}))
}

// Outbound turns actions into dst values, in order. Timers become sim timers
// (the timer's Gen goes back inside the TimerFired value). StopForRestart
// becomes sim.Crash and ShutdownDone becomes sim.Exit; nothing after them runs.
// IO calls and replies pass through for the runner's executor.
func Outbound(acts []runtime.Action) []any {
	var out []any
	for _, a := range acts {
		switch a := a.(type) {
		case runtime.SetTimer:
			out = append(out, sim.SetTimer{ID: uint64(a.ID), After: sched.TimeNs(a.After), Body: runtime.TimerFired{ID: a.ID, Gen: a.Gen}})
		case runtime.CancelTimer:
			out = append(out, sim.CancelTimer{ID: uint64(a.ID)})
		case runtime.StopForRestart:
			return append(out, sim.Crash{Cause: a.Cause})
		case runtime.ShutdownDone:
			return append(out, sim.Exit{})
		default:
			out = append(out, a)
		}
	}
	return out
}
