package simulator

import (
	"fmt"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// simNode wraps one replica machine. It feeds events to Step like the real run
// loop does (At and Wall are stamped when the event is taken), runs the
// actions that Step returns through the simulated services, and can crash, pause
// and restart.
type simNode struct {
	s       *sim
	idx     int
	factory runtime.MachineFactory
	inst    runtime.NodeInstance
	m       runtime.Machine

	up     bool
	paused bool
	cut    bool // cut off from the log and the object store
	held   []func()

	timers map[runtime.TimerID]uint64 // timer id -> token of the live timer
	tokens uint64
	boots  int
}

func newNode(s *sim, idx int, factory runtime.MachineFactory) *simNode {
	return &simNode{
		s: s, idx: idx, factory: factory,
		inst: runtime.NodeInstance{Node: runtime.NodeID(fmt.Sprintf("cp-%d", idx))},
	}
}

func (n *simNode) name() string { return string(n.inst.Node) }

// replica is the machine as a *replica.Replica, or nil while the node is down.
func (n *simNode) replica() *replica.Replica {
	r, _ := n.m.(*replica.Replica)
	return r
}

// boot starts a new incarnation with a fresh boot number. All memory of the old
// one is gone.
func (n *simNode) boot() {
	n.boots++
	n.inst.Inc = runtime.BootNumber(n.s.env.NewID())
	n.m = n.factory(n.s.env, n.inst)
	n.up, n.paused, n.held = true, false, nil
	n.timers = map[runtime.TimerID]uint64{}
	n.s.noteBoot(n)
	n.step(runtime.NodeStarted{Self: n.inst})
}

// crash kills the incarnation. Events that are on their way to it are dropped
// when they arrive.
func (n *simNode) crash(why string) {
	if !n.up {
		return
	}
	n.s.stats.Crashes++
	n.s.tracef("%s crashes (%s)", n.name(), why)
	n.up, n.paused, n.held, n.m = false, false, nil, nil
}

// restartAfter boots the node again after d, unless it is already up.
func (n *simNode) restartAfter(d runtime.Duration) {
	n.s.sched.after(d, "restart", func() {
		if !n.up {
			n.boot()
		}
	})
}

// pause stops the node from taking events. They wait and run in order when it resumes.
func (n *simNode) pause() {
	if n.up && !n.paused {
		n.s.stats.Pauses++
		n.paused = true
	}
}

func (n *simNode) resume() {
	if !n.paused {
		return
	}
	n.paused = false
	held := n.held
	n.held = nil
	for _, fn := range held {
		n.s.sched.after(0, "resume", func() { n.run(fn) })
	}
}

// run calls fn now, or keeps it for later while the node is paused.
func (n *simNode) run(fn func()) {
	if n.paused {
		n.held = append(n.held, fn)
		return
	}
	fn()
}

// deliver gives body to the incarnation inc. A node that is down, or has a newer
// incarnation, drops it.
func (n *simNode) deliver(inc runtime.BootNumber, body runtime.EventData) {
	if !n.up || n.inst.Inc != inc {
		return
	}
	n.run(func() {
		if n.up && n.inst.Inc == inc {
			n.step(body)
		}
	})
}

// deliverNow gives body to whatever incarnation is up (a client request).
func (n *simNode) deliverNow(body runtime.EventData) {
	if n.up {
		n.deliver(n.inst.Inc, body)
	}
}

func (n *simNode) step(body runtime.EventData) {
	ev := runtime.Event{At: n.s.sched.Mono(), Wall: n.s.sched.Wall(), Body: body}
	acts := n.m.Step(ev)
	n.s.observe(n, body, acts)
	n.exec(acts)
}

// exec starts the actions in order, as the run loop does. StopForRestart ends
// the incarnation and nothing after it runs.
func (n *simNode) exec(acts []runtime.Action) {
	for _, a := range acts {
		switch a := a.(type) {
		case runtime.SetTimer:
			n.setTimer(a)
		case runtime.CancelTimer:
			delete(n.timers, a.ID)
		case runtime.ControlPlaneReply:
			n.s.clients.reply(n, a)
		case runtime.RecordFact:
		case runtime.StopForRestart:
			n.s.stats.Trips++
			n.crash("stop for restart: " + a.Cause)
			n.restartAfter(runtime.Duration(n.s.rand.Between(int64(100*runtime.Millisecond), int64(500*runtime.Millisecond))))
			return
		case runtime.ShutdownDone:
			n.crash("shutdown")
			return
		case runtime.IOCall:
			n.s.store.dispatch(n, a)
		default:
			n.s.violation("known action", "node %s returned %T", n.name(), a)
		}
	}
}

func (n *simNode) setTimer(a runtime.SetTimer) {
	n.tokens++
	token, inc := n.tokens, n.inst.Inc
	n.timers[a.ID] = token
	n.s.sched.after(a.After, "timer", func() {
		if n.up && n.inst.Inc == inc && n.timers[a.ID] == token {
			n.deliver(inc, runtime.TimerFired{ID: a.ID, Gen: a.Gen})
		}
	})
}
