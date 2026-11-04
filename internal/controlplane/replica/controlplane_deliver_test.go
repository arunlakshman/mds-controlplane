package replica_test

import (
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

var brokerFrom = runtime.NodeInstance{Node: "b1"}

// writeOp sends one write op from a broker, lets its log slot apply, and
// returns the reply the broker gets.
func writeOp(t *testing.T, m *replica.Replica, request runtime.RequestID, operation runtime.ControlPlaneOperation) runtime.ControlPlaneDone {
	t.Helper()
	ap := onlyAppend(t, step(m, runtime.ControlPlaneDeliver{From: brokerFrom, Req: request, Op: operation}))
	acts := step(m, runtime.LogAppendDone{Req: ap.Req, Slot: ap.Slot, Err: runtime.OK, Outcome: runtime.Applied})
	for _, a := range acts {
		if r, ok := a.(runtime.ControlPlaneReply); ok && r.To == brokerFrom {
			if r.Done.Req != request {
				t.Fatalf("reply req = %d, want %d", r.Done.Req, request)
			}
			return r.Done
		}
	}
	t.Fatalf("no reply to broker after apply: %v", acts)
	return runtime.ControlPlaneDone{}
}

// readOp sends one read op from a broker on a ready machine. A read waits for
// a tail check that starts after it, so this runs catch-up rounds until the
// reply comes out. It returns the reply and the catch-up timer to use next.
func readOp(t *testing.T, m *replica.Replica, timer timerView, request runtime.RequestID, operation runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, timerView) {
	t.Helper()
	if acts := step(m, runtime.ControlPlaneDeliver{From: brokerFrom, Req: request, Op: operation}); len(acts) != 0 {
		t.Fatalf("read answered at once: %v", acts)
	}
	for range 3 {
		tail := step(m, runtime.TimerFired{ID: timer.ID, Gen: timer.Gen})[0].(runtime.LogGetEnd)
		acts := step(m, runtime.LogGetEndDone{Req: tail.Req, Tail: 0, Err: runtime.OK, Outcome: runtime.Applied})
		next, hasNext := findTimer(acts, 1)
		if hasNext {
			timer = next
		}
		for _, a := range acts {
			if r, ok := a.(runtime.ControlPlaneReply); ok && r.To == brokerFrom && r.Done.Req == request {
				return r.Done, timer
			}
		}
		if !hasNext {
			t.Fatalf("no catch-up timer after tail check: %v", acts)
		}
	}
	t.Fatalf("read %d was never answered", request)
	return runtime.ControlPlaneDone{}, timer
}

// TestOnControlPlaneDeliverWritesAnswerBroker: each write op goes through the log and the
// broker gets its reply after the slot applies.
func TestOnControlPlaneDeliverWritesAnswerBroker(t *testing.T) {
	m := newMachine(t, replica.Config{MaxCmdsPerSlot: 1, Writer: "w"})

	// CommitSegment: the reply carries one assignment per batch.
	done := writeOp(t, m, 1, contract.CommitSegment{
		SegmentKey: "s1",
		Batches: []contract.PendingBatch{
			{TopicPartition: contract.NewTopicPartition("t", 0), Records: 2, Loc: runtime.SegmentRef{Key: "s1", Off: 0, Len: 2}},
		},
	})
	if done.Err != runtime.OK {
		t.Fatalf("commit err = %v", done.Err)
	}
	if response, ok := done.Resp.(contract.CommitSegmentResp); !ok || len(response.Assigned) != 1 || response.Assigned[0].Count != 2 {
		t.Fatalf("commit resp = %+v", done.Resp)
	}

	// CreateTopic without FailIfExists succeeds.
	if done := writeOp(t, m, 2, contract.CreateTopic{Name: "orders", Partitions: 2}); done.Err != runtime.OK {
		t.Fatalf("create err = %v", done.Err)
	}
	// CreateTopic with FailIfExists on the same name is refused.
	if done := writeOp(t, m, 3, contract.CreateTopic{Name: "orders", Partitions: 3, FailIfExists: true}); done.Err != runtime.ErrTopicExists {
		t.Fatalf("exclusive create err = %v, want ErrTopicExists", done.Err)
	}

	// Group ops: install a group with one member, assign at the same
	// generation as its leader, commit an offset, and remove the member.
	member := contract.GroupMember{MemberID: "m1", ClientID: "c1", SessionTimeoutMs: 10000, RebalanceTimeoutMs: 30000}
	if done := writeOp(t, m, 4, contract.GroupInstall{
		Group: "g", ProtocolType: "consumer", Protocol: "range", Generation: 1, Leader: "m1",
		Members: []contract.GroupMember{member},
	}); done.Err != runtime.OK {
		t.Fatalf("install err = %v", done.Err)
	}
	if done := writeOp(t, m, 5, contract.GroupAssign{
		Group: "g", Generation: 1, Leader: "m1",
		Assignments: []contract.GroupAssignment{{Member: "m1", Bytes: []byte("a")}},
	}); done.Err != runtime.OK {
		t.Fatalf("assign err = %v", done.Err)
	}
	if done := writeOp(t, m, 6, contract.OffsetCommit{
		Group: "g", Member: "m1", Generation: 1,
		Offsets: []contract.PartitionOffset{{TopicPartition: contract.NewTopicPartition("orders", 0), Offset: 7}},
	}); done.Err != runtime.OK {
		t.Fatalf("offset commit err = %v", done.Err)
	}
	if done := writeOp(t, m, 7, contract.GroupRemove{Group: "g", Members: []string{"m1"}, Reason: "leave"}); done.Err != runtime.OK {
		t.Fatalf("remove err = %v", done.Err)
	}
}

// TestOnControlPlaneDeliverReadsAnswerBroker: each read op gets its answer from the fold
// once the node is ready and a fresh tail has applied.
func TestOnControlPlaneDeliverReadsAnswerBroker(t *testing.T) {
	m := newMachine(t, replica.Config{
		Writer: "s1", Rank: 1,
		CatchUpEvery:  runtime.Millisecond,
		TakeoverAfter: 10 * runtime.Second,
	})
	acts := bootEmpty(t, m, 0)
	timer, ok := findTimer(acts, 1)
	if !ok {
		t.Fatalf("catch-up timer missing in %v", acts)
	}

	done, timer := readOp(t, m, timer, 10, contract.FindSegments{TopicPartition: contract.NewTopicPartition("t", 0), From: 0, MaxBytes: 100})
	if _, ok := done.Resp.(contract.FindSegmentsResp); !ok {
		t.Fatalf("find resp = %#v", done.Resp)
	}
	done, timer = readOp(t, m, timer, 11, contract.DescribeTopics{})
	if _, ok := done.Resp.(contract.DescribeTopicsResp); !ok || done.Err != runtime.OK {
		t.Fatalf("describe topics = %+v", done)
	}
	done, timer = readOp(t, m, timer, 12, contract.OffsetFetch{Group: "g"})
	if _, ok := done.Resp.(contract.OffsetFetchResp); !ok {
		t.Fatalf("offset fetch resp = %#v", done.Resp)
	}
	done, timer = readOp(t, m, timer, 13, contract.DescribeGroup{Group: "g"})
	if response, ok := done.Resp.(contract.DescribeGroupResp); !ok || response.Exists {
		t.Fatalf("describe group = %#v, want a group that does not exist", done.Resp)
	}
	done, _ = readOp(t, m, timer, 14, contract.ListGroups{})
	if _, ok := done.Resp.(contract.ListGroupsResp); !ok || done.Err != runtime.OK {
		t.Fatalf("list groups = %+v", done)
	}
}

// TestOnControlPlaneDeliverUnknownOpInvalid: an op the machine does not know is refused
// with ErrInvalid and nothing reaches the log.
func TestOnControlPlaneDeliverUnknownOpInvalid(t *testing.T) {
	m := newMachine(t, replica.Config{Writer: "w"})
	acts := step(m, runtime.ControlPlaneDeliver{From: brokerFrom, Req: 9, Op: nil})
	if len(acts) != 1 {
		t.Fatalf("actions = %v, want one reply", acts)
	}
	r, ok := acts[0].(runtime.ControlPlaneReply)
	if !ok || r.To != brokerFrom || r.Done.Err != runtime.ErrInvalid || r.Done.Req != 9 {
		t.Fatalf("reply = %v, want ErrInvalid to broker", acts[0])
	}
}
