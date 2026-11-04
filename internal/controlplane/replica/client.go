package replica

import (
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadata"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/module"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// onControlPlaneDeliver handles one simulator-bridge ControlPlaneDeliver: reads wait for a fresh tail and answer from the fold when
// Ready; commits and creates go through Offer and ControlPlaneReply after apply.
func (m *Replica) onControlPlaneDeliver(o runtime.ControlPlaneDeliver) []runtime.Action {
	operation := o.Op
	switch m.meta.Mode(operation) {
	case module.Logged:
		cmd, _ := metadata.EntryOf(operation)
		return m.withReplies(m.Offer(ClientOperation{Req: o.Req, From: o.From, Cmd: cmd}))
	case module.ReadOnly:
		return m.offerRead(o, func() (runtime.ControlPlaneDone, error) { return m.meta.Read(operation) })
	default:
		return []runtime.Action{runtime.ControlPlaneReply{To: o.From, Done: runtime.ControlPlaneDone{Req: o.Req, Err: runtime.ErrInvalid}}}
	}
}

// offerRead queues a read behind a fresh tail check. A node that is not Ready
// refuses at once so the bridge tries the other replica.
func (m *Replica) offerRead(o runtime.ControlPlaneDeliver, read func() (runtime.ControlPlaneDone, error)) []runtime.Action {
	if !m.Ready() {
		return []runtime.Action{runtime.ControlPlaneReply{To: o.From, Done: runtime.ControlPlaneDone{Req: o.Req, Err: runtime.ErrUnavailable}}}
	}
	m.queueRead(o.Req, o.From, read)
	return nil
}

// withReplies turns answers that have a broker From into ControlPlaneReply actions.
// Unit-test Offers without From stay in m.answers for TakeAnswers.
func (m *Replica) withReplies(acts []runtime.Action) []runtime.Action {
	var kept []ReplyToClient
	for _, a := range m.answers {
		if a.From.Node == "" {
			kept = append(kept, a)
			continue
		}
		acts = append(acts, runtime.ControlPlaneReply{To: a.From, Done: a.Done})
	}
	m.answers = kept
	return acts
}
