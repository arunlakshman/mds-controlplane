package metadatastore

import (
	"context"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/controlplanetest/storecall"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadata"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

func newStore() *Store { return &Store{A: metadata.NewMem()} }

func TestSegmentAndTopicOperations(t *testing.T) {
	ctx := t.Context()
	s := newStore()
	if code, err := storecall.Code(ctx, s, contract.CreateTopic{Name: "t", Partitions: 3}); code != runtime.OK || err != nil {
		t.Fatal(code, err)
	}
	if code, _ := storecall.Code(ctx, s, contract.CreateTopic{Name: "t", Partitions: 3, FailIfExists: true}); code != runtime.ErrTopicExists {
		t.Fatalf("second create with FailIfExists: %v", code)
	}
	batch := contract.PendingBatch{TopicPartition: contract.NewTopicPartition("t", 2), Records: 4, Loc: runtime.SegmentRef{Key: "k", Len: 40}}
	response, code, err := storecall.Call[contract.CommitSegmentResp](ctx, s, contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{batch}})
	if err != nil || code != runtime.OK || response.Assigned[0].Base != 0 {
		t.Fatalf("%+v %v %v", response, code, err)
	}
	// The slot counter moves on: a second key gets the next offsets.
	response, code, _ = storecall.Call[contract.CommitSegmentResp](ctx, s, contract.CommitSegment{SegmentKey: "k2", Batches: []contract.PendingBatch{batch}})
	if code != runtime.OK || response.Assigned[0].Base != 4 {
		t.Fatalf("%+v %v", response, code)
	}
	found, code, _ := storecall.Call[contract.FindSegmentsResp](ctx, s, contract.FindSegments{TopicPartition: contract.NewTopicPartition("t", 2)})
	if code != runtime.OK || found.End != 8 || len(found.Ranges) != 2 {
		t.Fatalf("%+v %v", found, code)
	}
	if _, code, _ = storecall.Call[contract.FindSegmentsResp](ctx, s, contract.FindSegments{From: -1}); code != runtime.ErrInvalid {
		t.Fatal(code)
	}
	described, err := storecall.Read[contract.DescribeTopicsResp](ctx, s, contract.DescribeTopics{})
	if err != nil || len(described.Topics) != 1 || described.Topics[0].Partitions != 3 {
		t.Fatalf("%+v %v", described, err)
	}
}

func TestGroupOperations(t *testing.T) {
	ctx := t.Context()
	s := newStore()
	member := contract.GroupMember{MemberID: "m1", ClientID: "c", SessionTimeoutMs: 1000, RebalanceTimeoutMs: 1000}
	install := contract.GroupInstall{Group: "g", ProtocolType: "consumer", Protocol: "range", Generation: 1, Leader: "m1", TimeMs: 1, Members: []contract.GroupMember{member}}
	if code, err := storecall.Code(ctx, s, install); code != runtime.OK || err != nil {
		t.Fatalf("install: %v %v", code, err)
	}
	assign := contract.GroupAssign{Group: "g", Generation: 1, Leader: "m1", TimeMs: 2, Assignments: []contract.GroupAssignment{{Member: "m1", Bytes: []byte("a")}}}
	if code, err := storecall.Code(ctx, s, assign); code != runtime.OK || err != nil {
		t.Fatalf("assign: %v %v", code, err)
	}
	if code, err := storecall.Code(ctx, s, contract.GroupRemove{Group: "g", Members: []string{"m1"}, TimeMs: 3, Reason: "leave"}); code != runtime.OK || err != nil {
		t.Fatalf("remove: %v %v", code, err)
	}
	if code, _ := storecall.Code(ctx, s, assign); code != runtime.ErrUnknownMember {
		t.Fatalf("assign after remove: %v", code)
	}
	offsets := contract.OffsetCommit{Group: "g", Generation: -1, TimeMs: 4, Offsets: []contract.PartitionOffset{{TopicPartition: contract.NewTopicPartition("orders", 0), Offset: 3}}}
	response, code, err := storecall.Call[contract.OffsetCommitResp](ctx, s, offsets)
	if code != runtime.OK || err != nil || len(response.Errs) != 1 || response.Errs[0] != runtime.ErrUnknownPartition {
		t.Fatalf("commit to a topic that does not exist: %+v %v %v", response, code, err)
	}
	if _, err := storecall.Read[contract.OffsetFetchResp](ctx, s, contract.OffsetFetch{Group: "g"}); err != nil {
		t.Fatal(err)
	}
	if d, err := storecall.Read[contract.DescribeGroupResp](ctx, s, contract.DescribeGroup{Group: "g"}); err != nil || d.State != "Empty" {
		t.Fatalf("describe: %+v %v", d, err)
	}
	if l, err := storecall.Read[contract.ListGroupsResp](ctx, s, contract.ListGroups{}); err != nil || len(l.Groups) != 1 {
		t.Fatalf("list: %+v %v", l, err)
	}
}

// unknownOp is an operation that no module knows.
type unknownOp struct{}

func (unknownOp) IsControlPlaneOperation() {}

func TestUnknownOperationAndCancelledContext(t *testing.T) {
	s := newStore()
	if d, err := s.Exec(t.Context(), unknownOp{}); err != nil || d.Err != runtime.ErrInvalid {
		t.Fatalf("unknown operation: %+v %v", d, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Exec(ctx, contract.ListGroups{}); err == nil {
		t.Fatal("want the context error")
	}
}
