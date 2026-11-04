package wireboundary

import (
	"reflect"
	"strings"
	"testing"

	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/groups"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

func TestCommitSegmentRoundTrip(t *testing.T) {
	operation := contract.CommitSegment{
		SegmentKey: "seg/abc",
		Batches: []contract.PendingBatch{{
			TopicPartition: contract.NewTopicPartition("t", 1), Records: 3,
			Loc: runtime.SegmentRef{Key: "seg/abc", Off: 0, Len: 10},
		}},
	}
	got := CommitSegmentToCore(CommitSegmentFromCore(operation))
	if got.SegmentKey != operation.SegmentKey || len(got.Batches) != 1 {
		t.Fatalf("got %+v", got)
	}
	b := got.Batches[0]
	if b.TopicPartition != operation.Batches[0].TopicPartition || b.Records != 3 || b.Loc != operation.Batches[0].Loc {
		t.Fatalf("batch %+v", b)
	}
}

func TestLookupDescribeRoundTrip(t *testing.T) {
	lk := contract.FindSegments{TopicPartition: contract.NewTopicPartition("t", 2), From: 5, MaxBytes: 100}
	if got := LookupToCore(LookupFromCore(lk)); got != lk {
		t.Fatalf("lookup %+v", got)
	}
	dt := contract.DescribeTopics{Topics: []string{"a", "b"}}
	got := DescribeTopicsToCore(DescribeTopicsFromCore(dt))
	if len(got.Topics) != 2 || got.Topics[0] != "a" || got.Topics[1] != "b" {
		t.Fatalf("describe %+v", got)
	}
}

func TestResponseRoundTrip(t *testing.T) {
	cr := contract.CommitSegmentResp{Assigned: []contract.BatchRange{{
		TopicPartition: contract.NewTopicPartition("t", 0), Base: 10, Count: 2,
		Loc: runtime.SegmentRef{Key: "k", Off: 1, Len: 2},
	}}}
	got := CommitSegmentRespToCore(CommitSegmentRespFromCore(cr))
	if len(got.Assigned) != 1 || got.Assigned[0] != cr.Assigned[0] {
		t.Fatalf("commit resp %+v", got)
	}
	lr := contract.FindSegmentsResp{Ranges: cr.Assigned, End: 12}
	lgot := LookupRespToCore(LookupRespFromCore(lr))
	if lgot.End != 12 || len(lgot.Ranges) != 1 {
		t.Fatalf("lookup resp %+v", lgot)
	}
	dr := contract.DescribeTopicsResp{Topics: []contract.TopicInfo{{Name: "t", Partitions: 3}}}
	dgot := DescribeTopicsRespToCore(DescribeTopicsRespFromCore(dr))
	if len(dgot.Topics) != 1 || dgot.Topics[0] != dr.Topics[0] {
		t.Fatalf("describe resp %+v", dgot)
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
		if ToCoreErr(FromCoreErr(c)) != c {
			t.Fatalf("err %v", c)
		}
	}
	for _, o := range []runtime.Outcome{runtime.NotApplied, runtime.MaybeApplied, runtime.Applied} {
		if ToCoreOutcome(FromCoreOutcome(o)) != o {
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
	sameValue(t, "join", JoinGroupToCore(JoinGroupFromCore(join)), join)

	sync := groups.SyncReq{
		Group: "g", MemberID: "m", Generation: 3,
		Assignments: []groups.Assignment{{Member: "a", Bytes: []byte{1}}, {Member: "b", Bytes: []byte{2}}},
	}
	sameValue(t, "sync", SyncGroupToCore(SyncGroupFromCore(sync)), sync)

	beat := groups.HeartbeatReq{Group: "g", MemberID: "m", Generation: 4}
	sameValue(t, "heartbeat", HeartbeatToCore(HeartbeatFromCore(beat)), beat)

	leave := groups.LeaveReq{Group: "g", Members: []string{"a", "b"}}
	sameValue(t, "leave", LeaveGroupToCore(LeaveGroupFromCore(leave)), leave)

	commit := contract.OffsetCommit{
		Group: "g", Member: "m", Generation: 5,
		Offsets: []contract.PartitionOffset{
			{TopicPartition: contract.NewTopicPartition("t", 0), Offset: 9, LeaderEpoch: 2, Metadata: "x"},
			{TopicPartition: contract.NewTopicPartition("t", 1), Offset: 10},
		},
	}
	sameValue(t, "offset commit", OffsetCommitToCore(OffsetCommitFromCore(commit)), commit)

	fetch := contract.OffsetFetch{
		Group:      "g",
		Partitions: []contract.TopicPartition{contract.NewTopicPartition("t", 0), contract.NewTopicPartition("u", 7)},
	}
	sameValue(t, "offset fetch", OffsetFetchToCore(OffsetFetchFromCore(fetch)), fetch)

	names := []string{"a", "b"}
	sameValue(t, "describe groups", DescribeGroupsToCore(DescribeGroupsFromCore(names)), names)
}

func TestGroupResponsesRoundTrip(t *testing.T) {
	join := groups.JoinResp{
		MemberID: "m", Generation: 3, Protocol: "range", Leader: "l",
		Members: []groups.Member{{ID: "a", InstanceID: "i", Metadata: []byte{1}}, {ID: "b"}},
	}
	sameValue(t, "join resp", JoinGroupRespToCore(JoinGroupRespFromCore(join)), join)

	sync := groups.SyncResp{Assignment: []byte{7, 8}}
	sameValue(t, "sync resp", SyncGroupRespToCore(SyncGroupRespFromCore(sync)), sync)

	codes := []runtime.ErrCode{runtime.OK, runtime.ErrUnknownMember}
	sameValue(t, "leave resp", LeaveGroupRespToCore(LeaveGroupRespFromCore(codes)), codes)

	commit := contract.OffsetCommitResp{Errs: []runtime.ErrCode{runtime.OK, runtime.ErrIllegalGeneration}}
	sameValue(t, "offset commit resp", OffsetCommitRespToCore(OffsetCommitRespFromCore(commit)), commit)

	fetch := contract.OffsetFetchResp{Offsets: []contract.PartitionOffset{
		{TopicPartition: contract.NewTopicPartition("t", 2), Offset: 5, LeaderEpoch: 1, Metadata: "m"},
	}}
	sameValue(t, "offset fetch resp", OffsetFetchRespToCore(OffsetFetchRespFromCore(fetch)), fetch)

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
	sameValue(t, "describe groups resp", DescribeGroupsRespToCore(DescribeGroupsRespFromCore(described)), described)

	listed := []contract.GroupSummary{{Name: "g", ProtocolType: "consumer", State: "Stable"}}
	sameValue(t, "list groups resp", ListGroupsRespToCore(ListGroupsRespFromCore(listed)), listed)
}

func TestBrokerHeartbeatRoundTrip(t *testing.T) {
	self := contract.BrokerAddr{NodeID: 3, Host: "h", Port: 9092}
	sameValue(t, "broker heartbeat", BrokerHeartbeatToCore(BrokerHeartbeatFromCore(self)), self)

	live := []contract.BrokerAddr{self, {NodeID: 4, Host: "k", Port: 9093}}
	sameValue(t, "broker heartbeat resp", BrokerHeartbeatRespToCore(BrokerHeartbeatRespFromCore(live)), live)
}

// Nil messages and empty lists must give zero values, never a panic.
func TestNilAndEmptyInputs(t *testing.T) {
	sameValue(t, "nil commit", CommitSegmentToCore(nil), contract.CommitSegment{})
	sameValue(t, "nil lookup", LookupToCore(nil), contract.FindSegments{})
	sameValue(t, "nil describe", DescribeTopicsToCore(nil), contract.DescribeTopics{})
	sameValue(t, "nil commit resp", CommitSegmentRespToCore(nil), contract.CommitSegmentResp{})
	sameValue(t, "nil lookup resp", LookupRespToCore(nil), contract.FindSegmentsResp{})
	sameValue(t, "nil describe resp", DescribeTopicsRespToCore(nil), contract.DescribeTopicsResp{})
	sameValue(t, "nil join", JoinGroupToCore(nil), groups.JoinReq{})
	sameValue(t, "nil join resp", JoinGroupRespToCore(nil), groups.JoinResp{})
	sameValue(t, "nil broker heartbeat", BrokerHeartbeatToCore(nil), contract.BrokerAddr{})
	sameValue(t, "empty live set", BrokerHeartbeatRespToCore(BrokerHeartbeatRespFromCore(nil)), []contract.BrokerAddr(nil))

	// A batch with missing parts keeps its place and gets zero values.
	got := CommitSegmentToCore(&cpv1.CommitSegmentRequest{SegmentKey: "k", Batches: []*cpv1.PendingBatch{{Records: 2}}})
	sameValue(t, "partial batch", got, contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{{Records: 2}}})
}

// Every wire code must map to a runtime code and back, so a new code cannot be forgotten on one side.
func TestErrCodeTableIsComplete(t *testing.T) {
	for number, name := range cpv1.ErrCode_name {
		wire := cpv1.ErrCode(number)
		if back := FromCoreErr(ToCoreErr(wire)); back != wire {
			t.Errorf("wire code %s comes back as %s", name, back)
		}
	}
	for code := runtime.OK; code <= runtime.ErrTopicExists; code++ {
		if strings.HasPrefix(code.String(), "ErrCode(") {
			continue // a retired number
		}
		if back := ToCoreErr(FromCoreErr(code)); back != code {
			t.Errorf("core code %s comes back as %s", code, back)
		}
	}
	sameValue(t, "unknown wire code", ToCoreErr(cpv1.ErrCode(9999)), runtime.ErrIO)
	sameValue(t, "unknown core code", FromCoreErr(runtime.ErrCode(9999)), cpv1.ErrCode_ERR_CODE_IO)
}
