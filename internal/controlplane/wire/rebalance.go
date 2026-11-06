package wire

import (
	"slices"

	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/domain/rebalance"
)

// The converters between the group RPCs and the inputs and answers of the
// rebalance machine (domain/rebalance). They are written by hand because the
// call types of the machine are written by hand; T6.6 generates both into
// domain/rebalance and this file goes. A request takes the call id that the
// server gives the RPC.

// JoinToMachine is the Join input of a JoinGroup request.
func JoinToMachine(call rebalance.CallID, m *cpv1.JoinGroupRequest) rebalance.Join {
	return rebalance.Join{
		Call:               call,
		Group:              m.GetGroup(),
		MemberID:           m.GetMemberId(),
		GroupInstanceID:    m.GetGroupInstanceId(),
		ClientID:           m.GetClientId(),
		ClientHost:         m.GetClientHost(),
		SessionTimeoutMs:   m.GetSessionTimeoutMs(),
		RebalanceTimeoutMs: m.GetRebalanceTimeoutMs(),
		ProtocolType:       m.GetProtocolType(),
		Protocols: mapSlice(m.GetProtocols(), func(p *cpv1.GroupProtocol) rebalance.Protocol {
			return rebalance.Protocol{Name: p.GetName(), Metadata: slices.Clone(p.GetMetadata())}
		}),
	}
}

// SyncToMachine is the Sync input of a SyncGroup request.
func SyncToMachine(call rebalance.CallID, m *cpv1.SyncGroupRequest) rebalance.Sync {
	return rebalance.Sync{
		Call:       call,
		Group:      m.GetGroup(),
		MemberID:   m.GetMemberId(),
		Generation: m.GetGeneration(),
		Assignments: mapSlice(m.GetAssignments(), func(a *cpv1.GroupAssignment) rebalance.Assignment {
			return rebalance.Assignment{Member: a.GetMemberId(), Bytes: slices.Clone(a.GetAssignment())}
		}),
	}
}

// HeartbeatToMachine is the Heartbeat input of a Heartbeat request.
func HeartbeatToMachine(call rebalance.CallID, m *cpv1.HeartbeatRequest) rebalance.Heartbeat {
	return rebalance.Heartbeat{Call: call, Group: m.GetGroup(), MemberID: m.GetMemberId(), Generation: m.GetGeneration()}
}

// LeaveToMachine is the Leave input of a LeaveGroup request.
func LeaveToMachine(call rebalance.CallID, m *cpv1.LeaveGroupRequest) rebalance.Leave {
	return rebalance.Leave{Call: call, Group: m.GetGroup(), Members: slices.Clone(m.GetMembers())}
}

// OffsetCommitToMachine is the OffsetCommit input of an OffsetCommit request.
func OffsetCommitToMachine(call rebalance.CallID, m *cpv1.OffsetCommitRequest) rebalance.OffsetCommit {
	return rebalance.OffsetCommit{Call: call, Op: OffsetCommitRequestToCore(m)}
}

// DescribeToMachine is the Describe input of a DescribeGroups request.
func DescribeToMachine(call rebalance.CallID, m *cpv1.DescribeGroupsRequest) rebalance.Describe {
	return rebalance.Describe{Call: call, Groups: slices.Clone(m.GetGroups())}
}

// ListToMachine is the List input of a ListGroups request.
func ListToMachine(call rebalance.CallID, _ *cpv1.ListGroupsRequest) rebalance.List {
	return rebalance.List{Call: call}
}

// JoinAnswerToWire is the JoinGroup response of a join answer.
func JoinAnswerToWire(a rebalance.JoinAnswer) *cpv1.JoinGroupResponse {
	return &cpv1.JoinGroupResponse{
		MemberId:   a.MemberID,
		Generation: a.Generation,
		Protocol:   a.Protocol,
		Leader:     a.Leader,
		Members: mapSlice(a.Members, func(m rebalance.Member) *cpv1.GroupMember {
			return &cpv1.GroupMember{MemberId: m.ID, GroupInstanceId: m.InstanceID, Metadata: slices.Clone(m.Metadata)}
		}),
	}
}

// SyncAnswerToWire is the SyncGroup response of a sync answer.
func SyncAnswerToWire(a rebalance.SyncAnswer) *cpv1.SyncGroupResponse {
	return &cpv1.SyncGroupResponse{Assignment: slices.Clone(a.Assignment)}
}

// LeaveAnswerToWire is the LeaveGroup response of a leave answer.
func LeaveAnswerToWire(a rebalance.LeaveAnswer) *cpv1.LeaveGroupResponse {
	return &cpv1.LeaveGroupResponse{Errs: mapSlice(a.Errs, ErrCodeFromCore)}
}

// CommitAnswerToWire is the OffsetCommit response of a commit answer.
func CommitAnswerToWire(a rebalance.CommitAnswer) *cpv1.OffsetCommitResponse {
	return OffsetCommitResponseFromCore(a.OffsetCommitResp)
}

// DescribeAnswerToWire is the DescribeGroups response of a describe answer.
func DescribeAnswerToWire(a rebalance.DescribeAnswer) *cpv1.DescribeGroupsResponse {
	return &cpv1.DescribeGroupsResponse{
		Groups: mapSlice(a.Groups, func(g rebalance.GroupDescription) *cpv1.GroupDescription {
			return &cpv1.GroupDescription{Name: g.Name, Details: GroupDetailsFromCore(g.DescribeGroupResp)}
		}),
	}
}

// ListAnswerToWire is the ListGroups response of a list answer.
func ListAnswerToWire(a rebalance.ListAnswer) *cpv1.ListGroupsResponse {
	return ListGroupsResponseFromCore(contract.ListGroupsResp{Groups: a.Groups})
}

// HeartbeatAnswerToWire is the empty Heartbeat response: a heartbeat answer has no body.
func HeartbeatAnswerToWire(rebalance.Answer) *cpv1.HeartbeatResponse {
	return &cpv1.HeartbeatResponse{}
}
