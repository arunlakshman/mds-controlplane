package wire

import (
	"reflect"
	"testing"

	sharedv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/controlplane/shared/v1"
	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/groups"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

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
		if ErrCodeToCore(ErrCodeFromCore(c)) != c {
			t.Fatalf("err %v", c)
		}
	}
	for _, o := range []runtime.Outcome{runtime.NotApplied, runtime.MaybeApplied, runtime.Applied} {
		if toCoreOutcome(OutcomeFromCore(o)) != o {
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
	sameValue(t, "join", JoinGroupRequestToCore(JoinGroupRequestFromCore(join)), join)

	sync := groups.SyncReq{
		Group: "g", MemberID: "m", Generation: 3,
		Assignments: []groups.Assignment{{Member: "a", Bytes: []byte{1}}, {Member: "b", Bytes: []byte{2}}},
	}
	sameValue(t, "sync", SyncGroupRequestToCore(SyncGroupRequestFromCore(sync)), sync)

	beat := groups.HeartbeatReq{Group: "g", MemberID: "m", Generation: 4}
	sameValue(t, "heartbeat", HeartbeatRequestToCore(HeartbeatRequestFromCore(beat)), beat)

	leave := groups.LeaveReq{Group: "g", Members: []string{"a", "b"}}
	sameValue(t, "leave", LeaveGroupRequestToCore(LeaveGroupRequestFromCore(leave)), leave)

	commit := contract.OffsetCommit{
		Group: "g", Member: "m", Generation: 5,
		Offsets: []contract.PartitionOffset{
			{TopicPartition: contract.NewTopicPartition("t", 0), Offset: 9, LeaderEpoch: 2, Metadata: "x"},
			{TopicPartition: contract.NewTopicPartition("t", 1), Offset: 10},
		},
	}
	sameValue(t, "offset commit", OffsetCommitRequestToCore(OffsetCommitRequestFromCore(commit)), commit)

	fetch := contract.OffsetFetch{
		Group:      "g",
		Partitions: []contract.TopicPartition{contract.NewTopicPartition("t", 0), contract.NewTopicPartition("u", 7)},
	}
	sameValue(t, "offset fetch", OffsetFetchRequestToCore(OffsetFetchRequestFromCore(fetch)), fetch)

	names := groups.DescribeGroupsReq{Groups: []string{"a", "b"}}
	sameValue(t, "describe groups", DescribeGroupsRequestToCore(DescribeGroupsRequestFromCore(names)), names)
}

func TestGroupResponsesRoundTrip(t *testing.T) {
	join := groups.JoinResp{
		MemberID: "m", Generation: 3, Protocol: "range", Leader: "l",
		Members: []groups.Member{{ID: "a", InstanceID: "i", Metadata: []byte{1}}, {ID: "b"}},
	}
	sameValue(t, "join resp", JoinGroupResponseToCore(JoinGroupResponseFromCore(join)), join)

	sync := groups.SyncResp{Assignment: []byte{7, 8}}
	sameValue(t, "sync resp", SyncGroupResponseToCore(SyncGroupResponseFromCore(sync)), sync)

	leave := groups.LeaveResp{Errs: []runtime.ErrCode{runtime.OK, runtime.ErrUnknownMember}}
	sameValue(t, "leave resp", LeaveGroupResponseToCore(LeaveGroupResponseFromCore(leave)), leave)

	commit := contract.OffsetCommitResp{Errs: []runtime.ErrCode{runtime.OK, runtime.ErrIllegalGeneration}}
	sameValue(t, "offset commit resp", OffsetCommitResponseToCore(OffsetCommitResponseFromCore(commit)), commit)

	fetch := contract.OffsetFetchResp{Offsets: []contract.PartitionOffset{
		{TopicPartition: contract.NewTopicPartition("t", 2), Offset: 5, LeaderEpoch: 1, Metadata: "m"},
	}}
	sameValue(t, "offset fetch resp", OffsetFetchResponseToCore(OffsetFetchResponseFromCore(fetch)), fetch)

	described := groups.DescribeGroupsResp{Groups: []groups.GroupDescription{{
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
	}, {Name: "empty"}}}
	sameValue(t, "describe groups resp", DescribeGroupsResponseToCore(DescribeGroupsResponseFromCore(described)), described)

	listed := []contract.GroupSummary{{Name: "g", ProtocolType: "consumer", State: "Stable"}}
	sameValue(t, "list groups resp", ListGroupsResponseToCore(ListGroupsResponseFromCore(contract.ListGroupsResp{Groups: listed})).Groups, listed)
}

func TestBrokerHeartbeatRoundTrip(t *testing.T) {
	self := contract.BrokerHeartbeat{Broker: contract.BrokerAddr{NodeID: 3, Host: "h", Port: 9092}}
	sameValue(t, "broker heartbeat", BrokerHeartbeatRequestToCore(BrokerHeartbeatRequestFromCore(self)), self)

	live := contract.BrokerHeartbeatResp{Brokers: []contract.BrokerAddr{self.Broker, {NodeID: 4, Host: "k", Port: 9093}}}
	sameValue(t, "broker heartbeat resp", BrokerHeartbeatResponseToCore(BrokerHeartbeatResponseFromCore(live)), live)
}

// Nil messages and empty lists must give zero values, never a panic.
func TestNilAndEmptyInputs(t *testing.T) {
	sameValue(t, "nil commit", CommitSegmentRequestToCore(nil), contract.CommitSegment{})
	sameValue(t, "nil lookup", FindSegmentsRequestToCore(nil), contract.FindSegments{})
	sameValue(t, "nil describe", DescribeTopicsRequestToCore(nil), contract.DescribeTopics{})
	sameValue(t, "nil join", JoinGroupRequestToCore(nil), groups.JoinReq{})
	sameValue(t, "nil join resp", JoinGroupResponseToCore(nil), groups.JoinResp{})
	sameValue(t, "nil broker heartbeat", BrokerHeartbeatRequestToCore(nil), contract.BrokerHeartbeat{})
	sameValue(t, "empty live set", BrokerHeartbeatResponseToCore(BrokerHeartbeatResponseFromCore(contract.BrokerHeartbeatResp{})), contract.BrokerHeartbeatResp{})
	sameValue(t, "nil describe groups", DescribeGroupsRequestToCore(nil), groups.DescribeGroupsReq{})

	// A batch with missing parts keeps its place and gets zero values.
	got := CommitSegmentRequestToCore(&cpv1.CommitSegmentRequest{SegmentKey: "k", Batches: []*sharedv1.PendingBatch{{Records: 2}}})
	sameValue(t, "partial batch", got, contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{{Records: 2}}})
}
