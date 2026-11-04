package controlplanetest

import (
	"errors"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"

	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

func TestRoundTrip(t *testing.T) {
	ctx := t.Context()
	n := StartMem(t)
	c := n.Client
	if err := c.CreateTopic(ctx, "t", 3); err != nil {
		t.Fatal(err)
	}
	commit := contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{
		{TopicPartition: contract.NewTopicPartition("t", 1), Records: 4, Loc: runtime.SegmentRef{Key: "k", Off: 8, Len: 40}},
	}}
	cr, err := c.Call(ctx, commit)
	if err != nil {
		t.Fatal(err)
	}
	if got := cr.(contract.CommitSegmentResp).Assigned; len(got) != 1 || got[0].Base != 0 || got[0].Count != 4 || got[0].Loc != commit.Batches[0].Loc {
		t.Fatalf("commit: %+v", got)
	}
	l, err := c.Call(ctx, contract.FindSegments{TopicPartition: commit.Batches[0].TopicPartition})
	if err != nil || l.(contract.FindSegmentsResp).End != 4 || len(l.(contract.FindSegmentsResp).Ranges) != 1 {
		t.Fatalf("lookup: %+v %v", l, err)
	}
	d, err := c.Call(ctx, contract.DescribeTopics{})
	if err != nil || d.(contract.DescribeTopicsResp).Topics[0] != (contract.TopicInfo{Name: "t", Partitions: 3}) {
		t.Fatalf("describe: %+v %v", d, err)
	}
	commit.Batches[0].Records = 9
	_, err = c.Call(ctx, commit)
	be, ok := errors.AsType[*Error](err)
	if !ok || be.Code != runtime.ErrConflict {
		t.Fatalf("want ErrConflict, got %v", err)
	}
}
