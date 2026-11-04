package server

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/groups"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Conversions that only the tests need: the other direction of the group and
// broker calls (the server reads requests and builds answers; a client does the
// opposite). The round-trip tests below pair them with the server's own.

// brokerHeartbeatFromCore builds a BrokerHeartbeatRequest for a broker's own address.
func brokerHeartbeatFromCore(b contract.BrokerAddr) *cpv1.BrokerHeartbeatRequest {
	return &cpv1.BrokerHeartbeatRequest{Broker: brokerInfoFromCore(b)}
}

// brokerHeartbeatRespToCore rebuilds the live set from a response.
func brokerHeartbeatRespToCore(r *cpv1.BrokerHeartbeatResponse) []contract.BrokerAddr {
	return mapSlice(r.GetBrokers(), brokerInfoToCore)
}

// joinGroupFromCore builds a JoinGroupRequest from a join call.
func joinGroupFromCore(r groups.JoinReq) *cpv1.JoinGroupRequest {
	return &cpv1.JoinGroupRequest{
		Group: r.Group, MemberId: r.MemberID, GroupInstanceId: r.GroupInstanceID,
		ClientId: r.ClientID, ClientHost: r.ClientHost,
		SessionTimeoutMs: r.SessionTimeoutMs, RebalanceTimeoutMs: r.RebalanceTimeoutMs,
		ProtocolType: r.ProtocolType,
		Protocols: mapSlice(r.Protocols, func(p groups.Protocol) *cpv1.GroupProtocol {
			return &cpv1.GroupProtocol{Name: p.Name, Metadata: p.Metadata}
		}),
	}
}

// joinGroupRespToCore rebuilds a join answer from a response.
func joinGroupRespToCore(r *cpv1.JoinGroupResponse) groups.JoinResp {
	return groups.JoinResp{
		MemberID: r.GetMemberId(), Generation: r.GetGeneration(), Protocol: r.GetProtocol(), Leader: r.GetLeader(),
		Members: mapSlice(r.GetMembers(), func(m *cpv1.GroupMember) groups.Member {
			return groups.Member{ID: m.GetMemberId(), InstanceID: m.GetGroupInstanceId(), Metadata: m.GetMetadata()}
		}),
	}
}

// syncGroupFromCore builds a SyncGroupRequest from a sync call.
func syncGroupFromCore(r groups.SyncReq) *cpv1.SyncGroupRequest {
	return &cpv1.SyncGroupRequest{
		Group: r.Group, MemberId: r.MemberID, Generation: r.Generation,
		Assignments: mapSlice(r.Assignments, func(a groups.Assignment) *cpv1.GroupAssignment {
			return &cpv1.GroupAssignment{MemberId: a.Member, Assignment: a.Bytes}
		}),
	}
}

// syncGroupRespToCore rebuilds a sync answer from a response.
func syncGroupRespToCore(r *cpv1.SyncGroupResponse) groups.SyncResp {
	return groups.SyncResp{Assignment: r.GetAssignment()}
}

// heartbeatFromCore builds a GroupHeartbeatRequest.
func heartbeatFromCore(r groups.HeartbeatReq) *cpv1.GroupHeartbeatRequest {
	return &cpv1.GroupHeartbeatRequest{Group: r.Group, MemberId: r.MemberID, Generation: r.Generation}
}

// leaveGroupFromCore builds a LeaveGroupRequest.
func leaveGroupFromCore(r groups.LeaveReq) *cpv1.LeaveGroupRequest {
	return &cpv1.LeaveGroupRequest{Group: r.Group, Members: slices.Clone(r.Members)}
}

// leaveGroupRespToCore rebuilds the codes of a leave answer.
func leaveGroupRespToCore(r *cpv1.LeaveGroupResponse) []runtime.ErrCode {
	return mapSlice(r.GetErrs(), toCoreErr)
}

// offsetCommitFromCore builds a GroupOffsetCommitRequest from a commit.
func offsetCommitFromCore(operation contract.OffsetCommit) *cpv1.GroupOffsetCommitRequest {
	return &cpv1.GroupOffsetCommitRequest{
		Group: operation.Group, MemberId: operation.Member, Generation: operation.Generation,
		Offsets: mapSlice(operation.Offsets, partitionOffsetCommitFromCore),
	}
}

// offsetCommitRespToCore rebuilds the codes of a commit answer.
func offsetCommitRespToCore(r *cpv1.GroupOffsetCommitResponse) contract.OffsetCommitResp {
	return contract.OffsetCommitResp{Errs: mapSlice(r.GetErrs(), toCoreErr)}
}

// offsetFetchFromCore builds a GroupOffsetFetchRequest.
func offsetFetchFromCore(operation contract.OffsetFetch) *cpv1.GroupOffsetFetchRequest {
	return &cpv1.GroupOffsetFetchRequest{
		Group:      operation.Group,
		Partitions: mapSlice(operation.Partitions, topicPartitionFromCore),
	}
}

// offsetFetchRespToCore rebuilds an offset fetch answer.
func offsetFetchRespToCore(r *cpv1.GroupOffsetFetchResponse) contract.OffsetFetchResp {
	return contract.OffsetFetchResp{Offsets: mapSlice(r.GetOffsets(), partitionOffsetToCore)}
}

// The commit request and the fetch answer carry the same fields in two wire
// types, PartitionOffsetCommit and PartitionOffset.
func partitionOffsetCommitFromCore(o contract.PartitionOffset) *cpv1.PartitionOffsetCommit {
	return &cpv1.PartitionOffsetCommit{
		Tp: topicPartitionFromCore(o.TopicPartition), Offset: o.Offset, LeaderEpoch: o.LeaderEpoch, Metadata: o.Metadata,
	}
}

// describeGroupsFromCore builds a DescribeGroupsRequest from group names.
func describeGroupsFromCore(names []string) *cpv1.DescribeGroupsRequest {
	return &cpv1.DescribeGroupsRequest{Groups: slices.Clone(names)}
}

// describeGroupsRespToCore rebuilds the described groups from a response.
func describeGroupsRespToCore(r *cpv1.DescribeGroupsResponse) []groups.GroupDescription {
	return mapSlice(r.GetGroups(), groupDescriptionToCore)
}

// groupDescriptionToCore fills the embedded DescribeGroupResp by name:
// Go 1.26 does not allow promoted fields in a struct literal.
func groupDescriptionToCore(d *cpv1.GroupDescription) groups.GroupDescription {
	return groups.GroupDescription{
		Name: d.GetName(),
		DescribeGroupResp: contract.DescribeGroupResp{
			Exists: d.GetExists(), Generation: d.GetGeneration(), ProtocolType: d.GetProtocolType(),
			Protocol: d.GetProtocol(), Leader: d.GetLeader(), AssignedGeneration: d.GetAssignedGeneration(),
			State: d.GetState(),
			Members: mapSlice(d.GetMembers(), func(m *cpv1.GroupMemberInfo) contract.GroupMemberInfo {
				return contract.GroupMemberInfo{
					GroupMember: contract.GroupMember{
						MemberID: m.GetMemberId(), ClientID: m.GetClientId(), ClientHost: m.GetClientHost(),
						GroupInstanceID: m.GetGroupInstanceId(), SessionTimeoutMs: m.GetSessionTimeoutMs(),
						RebalanceTimeoutMs: m.GetRebalanceTimeoutMs(), Metadata: m.GetMetadata(),
					},
					JoinedTimeMs: m.GetJoinedTimeMs(),
					Assignment:   m.GetAssignment(),
				}
			}),
		},
	}
}

// listGroupsRespToCore rebuilds the group list from a response.
func listGroupsRespToCore(r *cpv1.ListGroupsResponse) []contract.GroupSummary {
	return mapSlice(r.GetGroups(), func(g *cpv1.GroupSummary) contract.GroupSummary {
		return contract.GroupSummary{Name: g.GetName(), ProtocolType: g.GetProtocolType(), State: g.GetState()}
	})
}

// toCoreErr maps a wire ErrCode to runtime.ErrCode. Unknown values become ErrIO.
func toCoreErr(c cpv1.ErrCode) runtime.ErrCode {
	for _, pair := range errCodePairs {
		if pair.wire == c {
			return pair.core
		}
	}
	return runtime.ErrIO
}

// toCoreOutcome maps a wire Outcome to runtime.Outcome. Unknown values become NotApplied.
func toCoreOutcome(o cpv1.Outcome) runtime.Outcome {
	switch o {
	case cpv1.Outcome_OUTCOME_MAYBE_APPLIED:
		return runtime.MaybeApplied
	case cpv1.Outcome_OUTCOME_APPLIED:
		return runtime.Applied
	default:
		return runtime.NotApplied
	}
}

func TestErrOutcomeMaps(t *testing.T) {
	for _, c := range []runtime.ErrCode{
		runtime.OK, runtime.ErrTimeout, runtime.ErrUnavailable, runtime.ErrNotFound,
		runtime.ErrConflict, runtime.ErrConnClosed, runtime.ErrTooLarge, runtime.ErrCorrupt,
		runtime.ErrInvalid, runtime.ErrIO,
		runtime.ErrUnknownMember, runtime.ErrIllegalGeneration, runtime.ErrUnknownPartition,
		runtime.ErrRebalanceInProgress, runtime.ErrInconsistentGroupProtocol, runtime.ErrTopicExists,
	} {
		if toCoreErr(fromCoreErr(c)) != c {
			t.Fatalf("err %v", c)
		}
	}
	for _, o := range []runtime.Outcome{runtime.NotApplied, runtime.MaybeApplied, runtime.Applied} {
		if toCoreOutcome(fromCoreOutcome(o)) != o {
			t.Fatalf("outcome %v", o)
		}
	}
}

// sameValue fails the test when a value does not survive the trip through the wire type.
func sameValue(t *testing.T, name string, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s\n got  %+v\n want %+v", name, got, want)
	}
}

func TestGroupRequestsRoundTrip(t *testing.T) {
	join := groups.JoinReq{
		Group: "g", MemberID: "m", GroupInstanceID: "i", ClientID: "c", ClientHost: "h",
		SessionTimeoutMs: 1, RebalanceTimeoutMs: 2, ProtocolType: "consumer",
		Protocols: []groups.Protocol{{Name: "range", Metadata: []byte{1}}, {Name: "rr", Metadata: []byte{2}}},
	}
	sameValue(t, "join", joinGroupToCore(joinGroupFromCore(join)), join)

	sync := groups.SyncReq{
		Group: "g", MemberID: "m", Generation: 3,
		Assignments: []groups.Assignment{{Member: "a", Bytes: []byte{1}}, {Member: "b", Bytes: []byte{2}}},
	}
	sameValue(t, "sync", syncGroupToCore(syncGroupFromCore(sync)), sync)

	beat := groups.HeartbeatReq{Group: "g", MemberID: "m", Generation: 4}
	sameValue(t, "heartbeat", heartbeatToCore(heartbeatFromCore(beat)), beat)

	leave := groups.LeaveReq{Group: "g", Members: []string{"a", "b"}}
	sameValue(t, "leave", leaveGroupToCore(leaveGroupFromCore(leave)), leave)

	commit := contract.OffsetCommit{
		Group: "g", Member: "m", Generation: 5,
		Offsets: []contract.PartitionOffset{
			{TopicPartition: contract.NewTopicPartition("t", 0), Offset: 9, LeaderEpoch: 2, Metadata: "x"},
			{TopicPartition: contract.NewTopicPartition("t", 1), Offset: 10},
		},
	}
	sameValue(t, "offset commit", offsetCommitToCore(offsetCommitFromCore(commit)), commit)

	fetch := contract.OffsetFetch{
		Group:      "g",
		Partitions: []contract.TopicPartition{contract.NewTopicPartition("t", 0), contract.NewTopicPartition("u", 7)},
	}
	sameValue(t, "offset fetch", offsetFetchToCore(offsetFetchFromCore(fetch)), fetch)

	names := []string{"a", "b"}
	sameValue(t, "describe groups", describeGroupsToCore(describeGroupsFromCore(names)), names)
}

func TestGroupResponsesRoundTrip(t *testing.T) {
	join := groups.JoinResp{
		MemberID: "m", Generation: 3, Protocol: "range", Leader: "l",
		Members: []groups.Member{{ID: "a", InstanceID: "i", Metadata: []byte{1}}, {ID: "b"}},
	}
	sameValue(t, "join resp", joinGroupRespToCore(joinGroupRespFromCore(join)), join)

	sync := groups.SyncResp{Assignment: []byte{7, 8}}
	sameValue(t, "sync resp", syncGroupRespToCore(syncGroupRespFromCore(sync)), sync)

	codes := []runtime.ErrCode{runtime.OK, runtime.ErrUnknownMember}
	sameValue(t, "leave resp", leaveGroupRespToCore(leaveGroupRespFromCore(codes)), codes)

	commit := contract.OffsetCommitResp{Errs: []runtime.ErrCode{runtime.OK, runtime.ErrIllegalGeneration}}
	sameValue(t, "offset commit resp", offsetCommitRespToCore(offsetCommitRespFromCore(commit)), commit)

	fetch := contract.OffsetFetchResp{Offsets: []contract.PartitionOffset{
		{TopicPartition: contract.NewTopicPartition("t", 2), Offset: 5, LeaderEpoch: 1, Metadata: "m"},
	}}
	sameValue(t, "offset fetch resp", offsetFetchRespToCore(offsetFetchRespFromCore(fetch)), fetch)

	described := []groups.GroupDescription{{
		Name: "g",
		DescribeGroupResp: contract.DescribeGroupResp{
			Exists: true, Generation: 2, ProtocolType: "consumer", Protocol: "range",
			Leader: "l", AssignedGeneration: 2, State: "Stable",
			Members: []contract.GroupMemberInfo{{
				GroupMember: contract.GroupMember{
					MemberID: "m", ClientID: "c", ClientHost: "h", GroupInstanceID: "i",
					SessionTimeoutMs: 1, RebalanceTimeoutMs: 2, Metadata: []byte{1},
				},
				JoinedTimeMs: 99, Assignment: []byte{2},
			}},
		},
	}, {Name: "empty"}}
	sameValue(t, "describe groups resp", describeGroupsRespToCore(describeGroupsRespFromCore(described)), described)

	listed := []contract.GroupSummary{{Name: "g", ProtocolType: "consumer", State: "Stable"}}
	sameValue(t, "list groups resp", listGroupsRespToCore(listGroupsRespFromCore(listed)), listed)
}

func TestBrokerHeartbeatRoundTrip(t *testing.T) {
	self := contract.BrokerAddr{NodeID: 3, Host: "h", Port: 9092}
	sameValue(t, "broker heartbeat", brokerHeartbeatToCore(brokerHeartbeatFromCore(self)), self)

	live := []contract.BrokerAddr{self, {NodeID: 4, Host: "k", Port: 9093}}
	sameValue(t, "broker heartbeat resp", brokerHeartbeatRespToCore(brokerHeartbeatRespFromCore(live)), live)
}

// Nil messages and empty lists must give zero values, never a panic.
func TestNilAndEmptyInputs(t *testing.T) {
	sameValue(t, "nil commit", commitSegmentToCore(nil), contract.CommitSegment{})
	sameValue(t, "nil lookup", lookupToCore(nil), contract.FindSegments{})
	sameValue(t, "nil describe", describeTopicsToCore(nil), contract.DescribeTopics{})
	sameValue(t, "nil join", joinGroupToCore(nil), groups.JoinReq{})
	sameValue(t, "nil join resp", joinGroupRespToCore(nil), groups.JoinResp{})
	sameValue(t, "nil broker heartbeat", brokerHeartbeatToCore(nil), contract.BrokerAddr{})
	sameValue(t, "empty live set", brokerHeartbeatRespToCore(brokerHeartbeatRespFromCore(nil)), []contract.BrokerAddr(nil))

	// A batch with missing parts keeps its place and gets zero values.
	got := commitSegmentToCore(&cpv1.CommitSegmentRequest{SegmentKey: "k", Batches: []*cpv1.PendingBatch{{Records: 2}}})
	sameValue(t, "partial batch", got, contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{{Records: 2}}})
}

// Every wire code must map to a runtime code and back, so a new code cannot be forgotten on one side.
func TestErrCodeTableIsComplete(t *testing.T) {
	for number, name := range cpv1.ErrCode_name {
		wire := cpv1.ErrCode(number)
		if back := fromCoreErr(toCoreErr(wire)); back != wire {
			t.Errorf("wire code %s comes back as %s", name, back)
		}
	}
	for code := runtime.OK; code <= runtime.ErrTopicExists; code++ {
		if strings.HasPrefix(code.String(), "ErrCode(") {
			continue // a retired number
		}
		if back := toCoreErr(fromCoreErr(code)); back != code {
			t.Errorf("core code %s comes back as %s", code, back)
		}
	}
	sameValue(t, "unknown wire code", toCoreErr(cpv1.ErrCode(9999)), runtime.ErrIO)
	sameValue(t, "unknown core code", fromCoreErr(runtime.ErrCode(9999)), cpv1.ErrCode_ERR_CODE_IO)
}
