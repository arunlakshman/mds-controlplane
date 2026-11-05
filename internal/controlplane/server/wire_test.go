package server

import (
	"github.com/manifesterio/mds-controlplane/internal/controlplane/wire"
	"reflect"
	"slices"
	"testing"

	sharedv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/controlplane/shared/v1"
	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/groups"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Conversions that only the tests need: the other direction of the group and
// broker calls (the server reads requests and builds answers; a client does the
// opposite). The round-trip tests below pair them with the server's own.

// brokerHeartbeatFromCore builds a BrokerHeartbeatRequest for a broker's own address.
func brokerHeartbeatFromCore(b contract.BrokerAddr) *cpv1.BrokerHeartbeatRequest {
	return &cpv1.BrokerHeartbeatRequest{Broker: wire.BrokerInfoFromCore(b)}
}

// brokerHeartbeatRespToCore rebuilds the live set from a response.
func brokerHeartbeatRespToCore(r *cpv1.BrokerHeartbeatResponse) []contract.BrokerAddr {
	return mapSlice(r.GetBrokers(), wire.BrokerInfoToCore)
}

// leaveGroupRespToCore rebuilds the codes of a leave answer.
func leaveGroupRespToCore(r *cpv1.LeaveGroupResponse) []runtime.ErrCode {
	return mapSlice(r.GetErrs(), wire.ErrCodeToCore)
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
		if wire.ErrCodeToCore(wire.ErrCodeFromCore(c)) != c {
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
	sameValue(t, "join", wire.JoinGroupRequestToCore(wire.JoinGroupRequestFromCore(join)), join)

	sync := groups.SyncReq{
		Group: "g", MemberID: "m", Generation: 3,
		Assignments: []groups.Assignment{{Member: "a", Bytes: []byte{1}}, {Member: "b", Bytes: []byte{2}}},
	}
	sameValue(t, "sync", wire.SyncGroupRequestToCore(wire.SyncGroupRequestFromCore(sync)), sync)

	beat := groups.HeartbeatReq{Group: "g", MemberID: "m", Generation: 4}
	sameValue(t, "heartbeat", wire.HeartbeatRequestToCore(wire.HeartbeatRequestFromCore(beat)), beat)

	leave := groups.LeaveReq{Group: "g", Members: []string{"a", "b"}}
	sameValue(t, "leave", wire.LeaveGroupRequestToCore(wire.LeaveGroupRequestFromCore(leave)), leave)

	commit := contract.OffsetCommit{
		Group: "g", Member: "m", Generation: 5,
		Offsets: []contract.PartitionOffset{
			{TopicPartition: contract.NewTopicPartition("t", 0), Offset: 9, LeaderEpoch: 2, Metadata: "x"},
			{TopicPartition: contract.NewTopicPartition("t", 1), Offset: 10},
		},
	}
	sameValue(t, "offset commit", wire.OffsetCommitRequestToCore(wire.OffsetCommitRequestFromCore(commit)), commit)

	fetch := contract.OffsetFetch{
		Group:      "g",
		Partitions: []contract.TopicPartition{contract.NewTopicPartition("t", 0), contract.NewTopicPartition("u", 7)},
	}
	sameValue(t, "offset fetch", wire.OffsetFetchRequestToCore(wire.OffsetFetchRequestFromCore(fetch)), fetch)

	names := []string{"a", "b"}
	sameValue(t, "describe groups", describeGroupsToCore(describeGroupsFromCore(names)), names)
}

func TestGroupResponsesRoundTrip(t *testing.T) {
	join := groups.JoinResp{
		MemberID: "m", Generation: 3, Protocol: "range", Leader: "l",
		Members: []groups.Member{{ID: "a", InstanceID: "i", Metadata: []byte{1}}, {ID: "b"}},
	}
	sameValue(t, "join resp", wire.JoinGroupResponseToCore(wire.JoinGroupResponseFromCore(join)), join)

	sync := groups.SyncResp{Assignment: []byte{7, 8}}
	sameValue(t, "sync resp", wire.SyncGroupResponseToCore(wire.SyncGroupResponseFromCore(sync)), sync)

	codes := []runtime.ErrCode{runtime.OK, runtime.ErrUnknownMember}
	sameValue(t, "leave resp", leaveGroupRespToCore(leaveGroupRespFromCore(codes)), codes)

	commit := contract.OffsetCommitResp{Errs: []runtime.ErrCode{runtime.OK, runtime.ErrIllegalGeneration}}
	sameValue(t, "offset commit resp", wire.OffsetCommitResponseToCore(wire.OffsetCommitResponseFromCore(commit)), commit)

	fetch := contract.OffsetFetchResp{Offsets: []contract.PartitionOffset{
		{TopicPartition: contract.NewTopicPartition("t", 2), Offset: 5, LeaderEpoch: 1, Metadata: "m"},
	}}
	sameValue(t, "offset fetch resp", wire.OffsetFetchResponseToCore(wire.OffsetFetchResponseFromCore(fetch)), fetch)

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
	sameValue(t, "list groups resp", wire.ListGroupsResponseToCore(wire.ListGroupsResponseFromCore(contract.ListGroupsResp{Groups: listed})).Groups, listed)
}

func TestBrokerHeartbeatRoundTrip(t *testing.T) {
	self := contract.BrokerAddr{NodeID: 3, Host: "h", Port: 9092}
	sameValue(t, "broker heartbeat", brokerHeartbeatToCore(brokerHeartbeatFromCore(self)), self)

	live := []contract.BrokerAddr{self, {NodeID: 4, Host: "k", Port: 9093}}
	sameValue(t, "broker heartbeat resp", brokerHeartbeatRespToCore(brokerHeartbeatRespFromCore(live)), live)
}

// Nil messages and empty lists must give zero values, never a panic.
func TestNilAndEmptyInputs(t *testing.T) {
	sameValue(t, "nil commit", wire.CommitSegmentRequestToCore(nil), contract.CommitSegment{})
	sameValue(t, "nil lookup", wire.FindSegmentsRequestToCore(nil), contract.FindSegments{})
	sameValue(t, "nil describe", wire.DescribeTopicsRequestToCore(nil), contract.DescribeTopics{})
	sameValue(t, "nil join", wire.JoinGroupRequestToCore(nil), groups.JoinReq{})
	sameValue(t, "nil join resp", wire.JoinGroupResponseToCore(nil), groups.JoinResp{})
	sameValue(t, "nil broker heartbeat", brokerHeartbeatToCore(nil), contract.BrokerAddr{})
	sameValue(t, "empty live set", brokerHeartbeatRespToCore(brokerHeartbeatRespFromCore(nil)), []contract.BrokerAddr(nil))

	// A batch with missing parts keeps its place and gets zero values.
	got := wire.CommitSegmentRequestToCore(&cpv1.CommitSegmentRequest{SegmentKey: "k", Batches: []*sharedv1.PendingBatch{{Records: 2}}})
	sameValue(t, "partial batch", got, contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{{Records: 2}}})
}
