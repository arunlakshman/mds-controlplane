package server

import (
	"context"
	"time"

	"github.com/manifesterio/mds-controlplane/internal/controlplane"
	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/groups"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/wire"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Where the pieces of the consumer group feature live:
//
//   - groups/coordinator.go, group.go, clock.go: the coordinator
//     logic. It keeps the phase of each group, the parked calls and the timers.
//   - server/groups_handler.go: this file. It holds the gRPC group handlers and the
//     log and read side of the coordinator (nodeGroups, over Node.Exec in
//     server/topics_handler.go).
//   - wire/wire_gen.go: maps group requests and responses to and from the wire
//     types.
//   - domain/group/schema.go, decide.go, tables.go: the group commands: the rules
//     of GroupInstall, GroupAssign, GroupRemove and OffsetCommit (Decide), and
//     their writes on the key-value store (Apply).
//   - domain/group/tables.go: also the group reads (OffsetFetch, DescribeGroup
//     and ListGroups).
//   - metadb/metadb.go: the group schema; the kernel routes each group operation to its row.
//   - metadb/entry.go: the log format of the group entries.
//   - replica/client.go: passes the group calls to the replica.
//   - replica/commit.go: encodes and decodes the group entries in the log.
//   - replica/replay.go: decodes group entries when it replays the log.
//   - controlplane/contract/contract.go: the group types (GroupInstall, GroupAssign,
//     GroupRemove, OffsetCommit and the others).

// groupWatchEvery is how often the node checks whether it is the active
// appender, and so whether the group coordinator should serve.
const groupWatchEvery = 100 * time.Millisecond

// nodeGroups is the log and read side of the group coordinator. It is Node.Exec
// plus one rule: a node that is not ready answers a read with an error, so the
// coordinator refuses the call.
type nodeGroups struct{ n *Node }

var _ groups.Store = nodeGroups{}

// Exec runs one group operation through Node.Exec.
func (s nodeGroups) Exec(ctx context.Context, operation runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	d, err := s.n.Exec(ctx, operation)
	if isCommand(operation) {
		return d, err
	}
	if err != nil {
		return runtime.ControlPlaneDone{}, err
	}
	if d.Err != runtime.OK {
		return runtime.ControlPlaneDone{}, controlplane.ErrUnavailable
	}
	return d, nil
}

// groupRole reports whether this node is the active appender and ready.
func (n *Node) groupRole(ctx context.Context) (bool, error) {
	active := false
	err := n.onLoop(ctx, func(m *replica.Replica) []any {
		active = m.IsActive() && m.Ready()
		return nil
	})
	return active, err
}

// watchGroups keeps the group coordinator in step with the active role. While
// this node is active and the coordinator is not serving (at start, after a
// takeover, or after a broken write), it rebuilds from the log. A failed
// rebuild is tried again on the next tick. A standby drops its memory.
func (n *Node) watchGroups(ctx context.Context) {
	for {
		tick := make(chan struct{})
		timer := n.config.Clock.AfterFunc(runtime.Duration(groupWatchEvery), func() { close(tick) })
		select {
		case <-ctx.Done():
			timer.Stop()
			n.groups.Stop()
			return
		case <-tick:
		}
		active, err := n.groupRole(ctx)
		if err != nil {
			continue // the loop is stopping; ctx ends the watch
		}
		if !active {
			n.groups.Stop()
			continue
		}
		if !n.groups.Serving() {
			n.groups.Stop()
			_ = n.groups.Rebuild(ctx) // a failure is tried again on the next tick
		}
	}
}

// groupCoordinator returns the coordinator when this node serves groups now.
// Otherwise it answers Unavailable, so the client tries the next address.
func (n *Node) groupCoordinator(ctx context.Context) (*groups.Coordinator, error) {
	active, err := n.groupRole(ctx)
	if err != nil {
		return nil, wire.ToRPCError(runtime.ErrIO, runtime.NotApplied)
	}
	if !active || !n.groups.Serving() {
		return nil, wire.ToRPCError(runtime.ErrUnavailable, runtime.NotApplied)
	}
	return n.groups, nil
}

// groupOutcome is the outcome of a group answer that failed. An unavailable
// answer may come after a log write, so it is MaybeApplied.
func groupOutcome(code runtime.ErrCode) runtime.Outcome {
	if code == runtime.ErrUnavailable {
		return runtime.MaybeApplied
	}
	return runtime.NotApplied
}

// groupCtxErr maps a call that ended with its context.
func groupCtxErr() error {
	return wire.ToRPCError(runtime.ErrTimeout, runtime.MaybeApplied)
}

// groupReplyErr maps a coordinator answer to an RPC error. A call that
// ended with its context gives a timeout. A failed answer gives its code.
func groupReplyErr(code runtime.ErrCode, err error) error {
	if err != nil {
		return groupCtxErr()
	}
	if code != runtime.OK {
		return wire.ToRPCError(code, groupOutcome(code))
	}
	return nil
}

// groupReplyCall describes one group RPC whose coordinator call returns the
// answer and an error, with the business code inside the answer. The error means
// that the call ended with its context. codeOf reads the code, and convert makes
// the wire type Out.
type groupReplyCall[Op, R, Out any] struct {
	call    func(*groups.Coordinator, context.Context, Op) (R, error)
	codeOf  func(R) runtime.ErrCode
	convert func(R) Out
}

// run checks that this node serves groups, runs the call, maps a failed answer
// to an RPC error and converts the answer.
func (g groupReplyCall[Op, R, Out]) run(ctx context.Context, n *Node, op Op) (Out, error) {
	var zero Out
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return zero, err
	}
	r, err := g.call(c, ctx, op)
	if err := groupReplyErr(g.codeOf(r), err); err != nil {
		return zero, err
	}
	return g.convert(r), nil
}

// groupCodeCall describes one group RPC whose coordinator call returns the
// answer and a code: how to call the coordinator, what the outcome of a failed
// code is (it differs between a write and a read), and how to convert the answer
// to the wire type Out.
type groupCodeCall[Op, R, Out any] struct {
	call    func(*groups.Coordinator, context.Context, Op) (R, runtime.ErrCode)
	outcome func(runtime.ErrCode) runtime.Outcome
	convert func(R) Out
}

// run checks that this node serves groups, runs the call, maps a failed code to
// an RPC error and converts the answer.
func (g groupCodeCall[Op, R, Out]) run(ctx context.Context, n *Node, op Op) (Out, error) {
	var zero Out
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return zero, err
	}
	r, code := g.call(c, ctx, op)
	if code != runtime.OK {
		return zero, wire.ToRPCError(code, g.outcome(code))
	}
	return g.convert(r), nil
}

var (
	groupJoin = groupReplyCall[groups.JoinReq, groups.JoinResp, *cpv1.JoinGroupResponse]{
		(*groups.Coordinator).JoinGroup, func(r groups.JoinResp) runtime.ErrCode { return r.Err }, wire.JoinGroupResponseFromCore}
	groupSync = groupReplyCall[groups.SyncReq, groups.SyncResp, *cpv1.SyncGroupResponse]{
		(*groups.Coordinator).SyncGroup, func(r groups.SyncResp) runtime.ErrCode { return r.Err }, wire.SyncGroupResponseFromCore}
	groupOffsetCommit = groupCodeCall[contract.OffsetCommit, contract.OffsetCommitResp, *cpv1.OffsetCommitResponse]{
		(*groups.Coordinator).OffsetCommit, groupOutcome, wire.OffsetCommitResponseFromCore}
	groupOffsetFetch = groupCodeCall[contract.OffsetFetch, contract.OffsetFetchResp, *cpv1.OffsetFetchResponse]{
		(*groups.Coordinator).OffsetFetch, notApplied, wire.OffsetFetchResponseFromCore}
	groupList = groupCodeCall[struct{}, []contract.GroupSummary, *cpv1.ListGroupsResponse]{
		func(c *groups.Coordinator, ctx context.Context, _ struct{}) ([]contract.GroupSummary, runtime.ErrCode) {
			return c.ListGroups(ctx)
		}, notApplied, func(g []contract.GroupSummary) *cpv1.ListGroupsResponse {
			return wire.ListGroupsResponseFromCore(contract.ListGroupsResp{Groups: g})
		}}
)

// notApplied is the outcome of a failed read.
func notApplied(runtime.ErrCode) runtime.Outcome { return runtime.NotApplied }

// JoinGroup implements ControlPlane.
func (n *Node) JoinGroup(ctx context.Context, request *cpv1.JoinGroupRequest) (*cpv1.JoinGroupResponse, error) {
	return groupJoin.run(ctx, n, wire.JoinGroupRequestToCore(request))
}

// SyncGroup implements ControlPlane.
func (n *Node) SyncGroup(ctx context.Context, request *cpv1.SyncGroupRequest) (*cpv1.SyncGroupResponse, error) {
	return groupSync.run(ctx, n, wire.SyncGroupRequestToCore(request))
}

// Heartbeat implements ControlPlane.
func (n *Node) Heartbeat(ctx context.Context, request *cpv1.HeartbeatRequest) (*cpv1.HeartbeatResponse, error) {
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	if code := c.Heartbeat(ctx, wire.HeartbeatRequestToCore(request)); code != runtime.OK {
		return nil, wire.ToRPCError(code, groupOutcome(code))
	}
	return &cpv1.HeartbeatResponse{}, nil
}

// LeaveGroup implements ControlPlane. The answer has one code per member.
func (n *Node) LeaveGroup(ctx context.Context, request *cpv1.LeaveGroupRequest) (*cpv1.LeaveGroupResponse, error) {
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	return wire.LeaveGroupResponseFromCore(groups.LeaveResp{Errs: c.LeaveGroup(ctx, wire.LeaveGroupRequestToCore(request))}), nil
}

// OffsetCommit implements ControlPlane. The answer has one code per partition.
func (n *Node) OffsetCommit(ctx context.Context, request *cpv1.OffsetCommitRequest) (*cpv1.OffsetCommitResponse, error) {
	return groupOffsetCommit.run(ctx, n, wire.OffsetCommitRequestToCore(request))
}

// OffsetFetch implements ControlPlane.
func (n *Node) OffsetFetch(ctx context.Context, request *cpv1.OffsetFetchRequest) (*cpv1.OffsetFetchResponse, error) {
	return groupOffsetFetch.run(ctx, n, wire.OffsetFetchRequestToCore(request))
}

// DescribeGroups implements ControlPlane. Any group that fails fails the call.
func (n *Node) DescribeGroups(ctx context.Context, request *cpv1.DescribeGroupsRequest) (*cpv1.DescribeGroupsResponse, error) {
	c, err := n.groupCoordinator(ctx)
	if err != nil {
		return nil, err
	}
	var out []groups.GroupDescription
	for _, name := range wire.DescribeGroupsRequestToCore(request).Groups {
		d, code := c.DescribeGroup(ctx, name)
		if code != runtime.OK {
			return nil, wire.ToRPCError(code, runtime.NotApplied)
		}
		out = append(out, groups.GroupDescription{Name: name, DescribeGroupResp: d})
	}
	return wire.DescribeGroupsResponseFromCore(groups.DescribeGroupsResp{Groups: out}), nil
}

// ListGroups implements ControlPlane.
func (n *Node) ListGroups(ctx context.Context, _ *cpv1.ListGroupsRequest) (*cpv1.ListGroupsResponse, error) {
	return groupList.run(ctx, n, struct{}{})
}
