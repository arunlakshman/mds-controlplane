package speccheck

import (
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// otherOp is an operation that Exec does not know. The embedded interface
// gives it the marker method, and Exec never calls any method on it.
type otherOp struct{ runtime.ControlPlaneOperation }

// TestExecDispatch checks that Exec routes every operation kind to its rule.
func TestExecDispatch(t *testing.T) {
	c := newGroupControl(t)
	cases := []struct {
		name      string
		operation runtime.ControlPlaneOperation
		want      runtime.ErrCode
		outcome   runtime.Outcome
	}{
		{"commit", contract.CommitSegment{SegmentKey: "s", Batches: []contract.PendingBatch{pb("orders", 0, 2)}}, runtime.OK, runtime.Applied},
		{"lookup", contract.FindSegments{TopicPartition: tp("orders", 0), From: -1}, runtime.ErrInvalid, runtime.NotApplied},
		{"describe topics", contract.DescribeTopics{}, runtime.OK, runtime.NotApplied},
		{"create topic", contract.CreateTopic{Name: "n", Partitions: 1}, runtime.OK, runtime.Applied},
		{"group install", gInstall("g", 1, "m1", "m1"), runtime.OK, runtime.Applied},
		{"group assign with no group", gAssign("nope", 0, "m1", "m1", "a"), runtime.ErrUnknownMember, runtime.NotApplied},
		{"group remove", gRemove("g", "leave", "m1"), runtime.OK, runtime.Applied},
		{"offset commit with no group", gCommit("nope", "m1", 1, gPair("orders", 0, 1)), runtime.ErrUnknownMember, runtime.NotApplied},
		{"offset fetch", contract.OffsetFetch{Group: "g"}, runtime.OK, runtime.NotApplied},
		{"describe group", contract.DescribeGroup{Group: "g"}, runtime.OK, runtime.NotApplied},
		{"list groups", contract.ListGroups{}, runtime.OK, runtime.NotApplied},
		{"unknown operation", otherOp{}, runtime.ErrInvalid, runtime.NotApplied},
	}
	for _, tc := range cases {
		d := call(c, tc.operation)
		if d.Err != tc.want || d.Outcome != tc.outcome {
			t.Fatalf("%s: err %v outcome %v, want %v %v", tc.name, d.Err, d.Outcome, tc.want, tc.outcome)
		}
	}
	if d := call(c, contract.DescribeTopics{}); d.Resp.(contract.DescribeTopicsResp).Topics == nil {
		t.Fatal("describe topics lost its answer")
	}
}

// TestExecCreateTopicFailIfExists: with FailIfExists an existing topic is
// ErrTopicExists and nothing changes. Without the flag a create of an existing
// topic is OK and only grows the topic.
func TestExecCreateTopicFailIfExists(t *testing.T) {
	c := NewControlPlane()
	if d := call(c, contract.CreateTopic{Name: "t", Partitions: 2, FailIfExists: true}); d.Err != runtime.OK || d.Outcome != runtime.Applied {
		t.Fatalf("new topic: %+v", d)
	}
	h := c.StateHash()
	if d := call(c, contract.CreateTopic{Name: "t", Partitions: 4, FailIfExists: true}); d.Err != runtime.ErrTopicExists || d.Outcome != runtime.NotApplied {
		t.Fatalf("exists: %+v", d)
	}
	if c.StateHash() != h {
		t.Fatal("a refused exclusive create changed the state")
	}
	if d := call(c, contract.CreateTopic{Name: "t", Partitions: 4}); d.Err != runtime.OK || d.Outcome != runtime.Applied {
		t.Fatalf("plain create of an existing topic: %+v", d)
	}
	if d := call(c, contract.CreateTopic{Name: "t", Partitions: 3}); d.Err != runtime.OK {
		t.Fatalf("a smaller plain create: %+v", d)
	}
	if d := call(c, contract.CreateTopic{Name: "", Partitions: 1, FailIfExists: true}); d.Err != runtime.ErrInvalid || d.Outcome != runtime.NotApplied {
		t.Fatalf("empty name with the flag: %+v", d)
	}
	if d := call(c, contract.CreateTopic{Name: "t", Partitions: 0}); d.Err != runtime.ErrInvalid {
		t.Fatalf("zero partitions: %+v", d)
	}
	got := call(c, contract.DescribeTopics{Topics: []string{"t"}}).Resp.(contract.DescribeTopicsResp).Topics
	if len(got) != 1 || got[0] != (contract.TopicInfo{Name: "t", Partitions: 4}) {
		t.Fatalf("topics %+v, want t with 4 partitions", got)
	}
}

// notAControlPlaneCall is an IOCall that is not a ControlPlaneCall.
type notAControlPlaneCall struct{ runtime.IOCall }

func TestExecPanicsOnOtherCalls(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Exec did not panic on a non-control-plane call")
		}
	}()
	NewControlPlane().Exec(notAControlPlaneCall{})
}
