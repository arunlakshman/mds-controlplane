package simulator

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/manifesterio/dst/assert"
	"github.com/manifesterio/dst/eng"
	"github.com/manifesterio/dst/fault"
	"github.com/manifesterio/dst/rng"
	"github.com/manifesterio/dst/sched"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// engCtx is the eng.Context that the simulator lends to the storage engine
// while the nodes are not yet dst processes. It is for one call of one
// incarnation of one node: a reply to a dead incarnation is dropped.
type engCtx struct {
	s   *sim
	n   *simNode
	inc runtime.BootNumber
}

func (n *simNode) engineContext() eng.Context {
	return engCtx{s: n.s, n: n, inc: n.inst.Inc}
}

func nodeActor(idx int) eng.ActorID { return eng.ActorID(fmt.Sprintf("cp-%d", idx)) }

// nodeOf finds the node of an actor id such as "cp-2".
func (s *sim) nodeOf(a eng.ActorID) *simNode {
	i, err := strconv.Atoi(strings.TrimPrefix(string(a), "cp-"))
	if err != nil || i < 0 || i >= len(s.nodes) {
		panic("simulator: unknown actor " + string(a))
	}
	return s.nodes[i]
}

func (c engCtx) Now() sched.TimeNs       { return sched.TimeNs(c.s.now()) }
func (c engCtx) Rand() *rng.Rand         { return c.s.rand }
func (c engCtx) Faults() *fault.Injector { return c.s.env.inj }
func (c engCtx) Assert() *assert.Log     { return c.s.env.log }
func (c engCtx) From() eng.ActorID       { return nodeActor(c.n.idx) }

// Deliver sends a result back to the node that made the call.
func (c engCtx) Deliver(_ eng.ActorID, after sched.TimeNs, body any) {
	c.s.after(runtime.Duration(after), "io-done", func() { c.n.deliver(c.inc, body.(runtime.EventData)) })
}

// Self queues the effect of a call.
func (c engCtx) Self(after sched.TimeNs, body any) {
	c.s.after(runtime.Duration(after), "io-exec", func() { c.s.store.OnSelf(c, body) })
}
