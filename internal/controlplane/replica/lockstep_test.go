package replica_test

import (
	"fmt"
	"hash/fnv"
	"reflect"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadata"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
	"github.com/manifesterio/mds-controlplane/internal/spec"
)

var lockstepCmds = []metadata.Entry{
	{ID: "0", Kind: metadata.KindCreateTopic, Op: contract.CreateTopic{Name: "orders", Partitions: 2}},
	{ID: "1", Kind: metadata.KindNoop},
	{ID: "2", Kind: metadata.KindCommit, Op: contract.CommitSegment{
		SegmentKey: "seg-a",
		Batches: []contract.PendingBatch{
			{TopicPartition: contract.NewTopicPartition("orders", 0), Records: 3, Loc: runtime.SegmentRef{Key: "o1", Off: 0, Len: 30}},
			{TopicPartition: contract.NewTopicPartition("orders", 1), Records: 2, Loc: runtime.SegmentRef{Key: "o1", Off: 30, Len: 20}},
		},
	}},
	{ID: "3", Kind: metadata.KindCommit, Op: contract.CommitSegment{
		SegmentKey: "seg-b",
		Batches: []contract.PendingBatch{
			{TopicPartition: contract.NewTopicPartition("orders", 0), Records: 1, Loc: runtime.SegmentRef{Key: "o2", Off: 0, Len: 10}},
		},
	}},
	{ID: "4", Kind: metadata.KindCommit, Op: contract.CommitSegment{ // replay
		SegmentKey: "seg-a",
		Batches: []contract.PendingBatch{
			{TopicPartition: contract.NewTopicPartition("orders", 0), Records: 3, Loc: runtime.SegmentRef{Key: "o1", Off: 0, Len: 30}},
			{TopicPartition: contract.NewTopicPartition("orders", 1), Records: 2, Loc: runtime.SegmentRef{Key: "o1", Off: 30, Len: 20}},
		},
	}},
	{ID: "5", Kind: metadata.KindNoop},
	{ID: "6", Kind: metadata.KindCommit, Op: contract.CommitSegment{
		SegmentKey: "seg-c",
		Batches: []contract.PendingBatch{
			{TopicPartition: contract.NewTopicPartition("orders", 1), Records: 4, Loc: runtime.SegmentRef{Key: "o3", Off: 0, Len: 40}},
		},
	}},
	// Exclusive creates go last, so the indexes above stay the same.
	{ID: "7", Kind: metadata.KindCreateTopicExclusive, Op: contract.CreateTopic{Name: "fresh", Partitions: 3, FailIfExists: true}},  // new: OK
	{ID: "8", Kind: metadata.KindCreateTopicExclusive, Op: contract.CreateTopic{Name: "fresh", Partitions: 1, FailIfExists: true}},  // exists: ErrTopicExists
	{ID: "9", Kind: metadata.KindCreateTopicExclusive, Op: contract.CreateTopic{Name: "orders", Partitions: 5, FailIfExists: true}}, // exists: ErrTopicExists
	{ID: "10", Kind: metadata.KindCreateTopic, Op: contract.CreateTopic{Name: "fresh", Partitions: 6}},                              // plain create: OK, grows it
	{ID: "11", Kind: metadata.KindCreateTopicExclusive, Op: contract.CreateTopic{Name: "", Partitions: 1, FailIfExists: true}},      // invalid name: ErrInvalid
}

func queryDigest(exec func(runtime.ControlPlaneOperation) runtime.ControlPlaneDone) uint64 {
	h := fnv.New64a()
	d := exec(contract.DescribeTopics{}).Resp.(contract.DescribeTopicsResp)
	for _, t := range d.Topics {
		_, _ = fmt.Fprintf(h, "T%s/%d;", t.Name, t.Partitions)
		for p := int32(0); p < t.Partitions; p++ {
			l := exec(contract.FindSegments{TopicPartition: contract.NewTopicPartition(t.Name, p)}).Resp.(contract.FindSegmentsResp)
			_, _ = fmt.Fprintf(h, "P%d/%d:%+v;", p, l.End, l.Ranges)
		}
	}
	return h.Sum64()
}

// feedContiguous drives catch-up so the machine applies entries in slot order
// through one ReadDone page (no gaps).
func feedContiguous(t *testing.T, m *replica.Replica, entries []metadata.Entry) []runtime.ControlPlaneDone {
	t.Helper()
	acts := step(m, runtime.NodeStarted{})
	timer := mustTimer(t, acts[0])
	acts = step(m, runtime.TimerFired{ID: timer.ID, Gen: timer.Gen})
	tail := acts[0].(runtime.LogGetEnd)
	n := runtime.LogPosition(len(entries))
	acts = step(m, runtime.LogGetEndDone{Req: tail.Req, Tail: n, Err: runtime.OK, Outcome: runtime.Applied})
	read := acts[0].(runtime.LogRead)
	logs := make([]runtime.LogEntry, len(entries))
	for i, e := range entries {
		logs[i] = runtime.LogEntry{Slot: runtime.LogPosition(i), Data: metadata.Encode(e)}
	}
	step(m, runtime.LogReadDone{Req: read.Req, Err: runtime.OK, Outcome: runtime.Applied, Entries: logs})
	if m.Applied() != n {
		t.Fatalf("Applied = %d, want %d", m.Applied(), n)
	}
	out := make([]runtime.ControlPlaneDone, len(entries))
	twin := metadata.NewMem()
	t.Cleanup(func() { _ = twin.Close() })
	for i, e := range entries {
		out[i] = twin.Apply(int64(i), e)
	}
	if twin.StateHash() != m.Metadata().StateHash() {
		t.Fatalf("twin fold hash %x != machine fold %x", twin.StateHash(), m.Metadata().StateHash())
	}
	return out
}

func TestLockstepMachineVsModel(t *testing.T) {
	m := newMachine(t, replica.Config{CatchUpEvery: runtime.Millisecond})
	orc := spec.New()
	machineAnswers := feedContiguous(t, m, lockstepCmds)

	var orcAnswers []runtime.ControlPlaneDone
	for i, e := range lockstepCmds {
		switch e.Kind {
		case metadata.KindNoop:
			orcAnswers = append(orcAnswers, runtime.ControlPlaneDone{Req: runtime.RequestID(i), Outcome: runtime.Applied})
		case metadata.KindCreateTopic, metadata.KindCreateTopicExclusive:
			create := e.Op.(contract.CreateTopic)
			orcAnswers = append(orcAnswers, runtime.ControlPlaneDone{Req: runtime.RequestID(i), Err: orc.Apply(create).Err})
		case metadata.KindCommit:
			orcAnswers = append(orcAnswers, orc.Apply(e.Op))
		}
	}

	for i := range lockstepCmds {
		if lockstepCmds[i].Kind == metadata.KindNoop {
			continue
		}
		got, want := machineAnswers[i], orcAnswers[i]
		if got.Err != want.Err || !reflect.DeepEqual(got.Resp, want.Resp) {
			t.Fatalf("slot %d: machine %+v, the model %+v", i, got, want)
		}
	}

	appDig := queryDigest(func(operation runtime.ControlPlaneOperation) runtime.ControlPlaneDone {
		return m.Metadata().Exec(runtime.ControlPlaneCall{Op: operation}).(runtime.ControlPlaneDone)
	})
	orcDig := queryDigest(func(operation runtime.ControlPlaneOperation) runtime.ControlPlaneDone {
		return orc.Apply(operation)
	})
	if appDig != orcDig {
		t.Fatalf("query digest: machine fold %x, the model %x", appDig, orcDig)
	}
	t.Logf("lockstep cmds=%d applied=%d foldHash=%x queryDigest=%x",
		len(lockstepCmds), m.Applied(), m.Metadata().StateHash(), appDig)
}
