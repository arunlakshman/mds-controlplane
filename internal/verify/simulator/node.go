package simulator

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/manifesterio/dst/eng"
	"github.com/manifesterio/dst/sched"
	dstsim "github.com/manifesterio/dst/sim"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// simNode is one replica of the simulation. It lives for the whole run. Each
// boot gives it a new dst process (nodeProc) and a new incarnation, so nothing
// of the old boot reaches the new one. The node feeds events to the replica
// machine like the real run loop does, and turns the actions that Step returns
// into dst values: timers, calls to the storage, replies to clients. It can
// crash, pause and restart.
type simNode struct {
	s       *sim
	idx     int
	factory replica.Factory
	inst    runtime.NodeInstance
	m       *replica.Replica

	up     bool
	paused bool
	cut    bool  // cut off from the log and the object store
	held   []any // inbox values that wait while the node is paused

	boots int
}

// nodeProc is the dst process of one incarnation of a node.
type nodeProc struct{ n *simNode }

func (p *nodeProc) ID() dstsim.ProcessID { return nodeID(p.n.idx) }

func (p *nodeProc) StateHash() uint64 {
	if p.n.m == nil {
		return 0
	}
	return p.n.m.StateHash()
}

func (p *nodeProc) Step(cx dstsim.Context, inbox any) []any { return p.n.step(cx, inbox) }

func nodeID(idx int) dstsim.ProcessID { return dstsim.ProcessID(fmt.Sprintf("cp-%d", idx)) }

// nodeOf finds the node of an actor id such as "cp-2".
func (s *sim) nodeOf(a eng.ActorID) *simNode {
	i, err := strconv.Atoi(strings.TrimPrefix(string(a), "cp-"))
	if err != nil || i < 0 || i >= len(s.nodes) {
		panic("simulator: unknown actor " + string(a))
	}
	return s.nodes[i]
}

func newNode(s *sim, idx int, factory replica.Factory) *simNode {
	return &simNode{
		s: s, idx: idx, factory: factory,
		inst: runtime.NodeInstance{Node: runtime.NodeID(fmt.Sprintf("cp-%d", idx))},
	}
}

func (n *simNode) name() string { return string(n.inst.Node) }

// replica is the machine as a *replica.Replica, or nil while the node is down.
func (n *simNode) replica() *replica.Replica { return n.m }

// boot asks the controller to start a new incarnation, unless the node is up.
func (n *simNode) boot() { n.s.runner.Inject(ctlID, 0, bootNode{idx: n.idx}) }

// up starts a new incarnation with a fresh boot number. All memory of the old
// one is gone. The controller calls it.
func (n *simNode) start() {
	n.boots++
	n.inst.Inc = runtime.BootNumber(n.s.env.NewID())
	n.m = n.factory(n.s.env, n.inst)
	n.up, n.paused, n.held = true, false, nil
	n.s.noteBoot(n)
}

// down marks the incarnation dead. Events that are on their way to it are
// dropped when they arrive.
func (n *simNode) down(why string) {
	if !n.up {
		return
	}
	n.s.stats.Crashes++
	n.s.tracef("%s crashes (%s)", n.name(), why)
	n.up, n.paused, n.held, n.m = false, false, nil, nil
}

// crash asks the controller to kill the node (the nemesis) and restart it
// after d. The controller does it in one step, so a boot can not slip in
// between the "down" and the runner's crash.
func (n *simNode) crash(why string, d runtime.Duration) {
	if n.up {
		n.s.runner.Inject(ctlID, 0, killNode{idx: n.idx, why: why, restartIn: d})
	}
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
	for _, body := range held {
		n.s.runner.Inject(nodeID(n.idx), 0, body)
	}
}

// deliverNow gives body to whatever incarnation is up (a client request).
func (n *simNode) deliverNow(body runtime.EventData) {
	if n.up {
		n.s.runner.Inject(nodeID(n.idx), 0, body)
	}
}

// step is one Step of the replica: it runs when the node is up and not paused.
func (n *simNode) step(cx dstsim.Context, body any) []any {
	if !n.up {
		return nil
	}
	if n.paused {
		n.held = append(n.held, body)
		return nil
	}
	acts := n.m.Step(cx, body)
	return n.outbound(acts)
}

// clientReply is the answer of a node to a client. The client pool takes it.
type clientReply struct {
	node  int
	reply runtime.ControlPlaneReply
}

// outbound turns what one Step returned into what the runner takes, in order.
// Timers pass as they are. A Crash or an Exit ends the incarnation and nothing
// after it runs; the controller is told so it can restart or mark the node down.
// A reply to a client goes to the client pool, an IO call to the storage.
func (n *simNode) outbound(acts []any) []any {
	var out []any
	for _, a := range acts {
		switch a := a.(type) {
		case dstsim.SetTimer, dstsim.CancelTimer:
			out = append(out, a) // the replica speaks dst already
		case dstsim.Crash: // the replica tripped
			d := runtime.Duration(n.s.rand.Between(int64(100*runtime.Millisecond), int64(500*runtime.Millisecond)))
			return append(out, a, dstsim.Send{To: ctlID, Body: tripped{idx: n.idx, cause: a.Cause, in: d}})
		case dstsim.Exit: // the replica ended on purpose
			return append(out, a, dstsim.Send{To: ctlID, Body: shutDown{idx: n.idx}})
		case runtime.ControlPlaneReply:
			out = append(out, clientReply{node: n.idx, reply: a})
		case runtime.IOCall:
			out = append(out, a)
		default:
			n.s.violation("known action", "node %s returned %T", n.name(), a)
		}
	}
	return out
}

// ctlID is the controller: the process that starts, kills and restarts nodes.
const ctlID dstsim.ProcessID = "sim-ctl"

// Messages to the controller.
type (
	bootNode struct{ idx int } // start a new incarnation now
	tripped  struct {
		idx   int
		cause string
		in    runtime.Duration
	} // the node asked to be stopped for a restart; start it again after in
	shutDown struct{ idx int } // the node ended on purpose
	killNode struct {
		idx       int
		why       string
		restartIn runtime.Duration
	} // the nemesis killed the node
)

type controller struct{ s *sim }

func (controller) ID() dstsim.ProcessID { return ctlID }
func (controller) StateHash() uint64    { return 0 }

func (c controller) Step(_ dstsim.Context, inbox any) []any {
	s := c.s
	switch m := inbox.(type) {
	case bootNode:
		n := s.nodes[m.idx]
		if n.up {
			return nil
		}
		n.start()
		return []any{dstsim.Restart{Who: nodeID(m.idx), New: &nodeProc{n: n}, Boot: runtime.NodeStarted{Self: n.inst}}}
	case tripped:
		s.stats.Trips++
		s.nodes[m.idx].down("stop for restart: " + m.cause)
		return []any{dstsim.SetTimer{ID: uint64(m.idx), After: sched.TimeNs(m.in), Body: bootNode{idx: m.idx}}}
	case shutDown:
		s.nodes[m.idx].down("shutdown")
		return nil
	case killNode:
		if !s.nodes[m.idx].up {
			return nil
		}
		s.nodes[m.idx].down(m.why)
		return []any{
			dstsim.Crash{Who: nodeID(m.idx), Cause: m.why},
			dstsim.SetTimer{ID: uint64(m.idx), After: sched.TimeNs(m.restartIn), Body: bootNode{idx: m.idx}},
		}
	}
	return nil
}

// replyEngine hands node replies to the client pool.
type replyEngine struct{ s *sim }

func (replyEngine) ID() eng.ID              { return "client-replies" }
func (replyEngine) StateHash() uint64       { return 0 }
func (replyEngine) OnSelf(eng.Context, any) {}
func (e replyEngine) Handle(_ eng.Context, act any) error {
	r, ok := act.(clientReply)
	if !ok {
		return eng.ErrNotMine
	}
	e.s.clients.reply(e.s.nodes[r.node], r.reply)
	return nil
}
