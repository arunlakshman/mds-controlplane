package topics_test

import (
	"reflect"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/module"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/topics"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

func roundTrip(t *testing.T, operation runtime.ControlPlaneOperation) runtime.ControlPlaneOperation {
	t.Helper()
	m := topics.Module{}
	kind, ok := m.KindOf(operation)
	if !ok {
		t.Fatalf("%T has no kind", operation)
	}
	w := engine.NewWriter()
	m.Encode(w, kind, operation)
	r := engine.NewReader(w.Bytes())
	got := m.Decode(r, kind)
	if !r.Done() {
		t.Fatalf("%T: the decoder did not read exactly the bytes written", operation)
	}
	return got
}

func TestCodecRoundTrip(t *testing.T) {
	tp := contract.NewTopicPartition
	for _, operation := range []runtime.ControlPlaneOperation{
		contract.CreateTopic{Name: "orders", Partitions: 3},
		contract.CreateTopic{Name: "orders", Partitions: 3, FailIfExists: true},
		contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{
			{TopicPartition: tp("t", 0), Records: 3, Loc: runtime.SegmentRef{Key: "o", Off: 10, Len: 30}},
			{TopicPartition: tp("t", 1), Records: 2, Loc: runtime.SegmentRef{Key: "o", Off: 40, Len: 20}},
		}},
		contract.CommitSegment{SegmentKey: "empty"},
	} {
		got := roundTrip(t, operation)
		// an empty batch list reads back as an empty, non-nil slice; compare by content
		if c, ok := operation.(contract.CommitSegment); ok && len(c.Batches) == 0 {
			if g, ok := got.(contract.CommitSegment); !ok || g.SegmentKey != c.SegmentKey || len(g.Batches) != 0 {
				t.Fatalf("empty commit: %+v", got)
			}
			continue
		}
		if !reflect.DeepEqual(got, operation) {
			t.Fatalf("got %+v, want %+v", got, operation)
		}
	}
}

func TestKindsAndMode(t *testing.T) {
	m := topics.Module{}
	if len(m.Kinds()) != 3 {
		t.Fatalf("kinds %v", m.Kinds())
	}
	if _, ok := m.KindOf(contract.DescribeTopics{}); ok {
		t.Fatal("a read has no kind")
	}
	for _, c := range []struct {
		operation runtime.ControlPlaneOperation
		want      module.Mode
	}{
		{contract.CommitSegment{}, module.Logged},
		{contract.CreateTopic{}, module.Logged},
		{contract.FindSegments{}, module.ReadOnly},
		{contract.DescribeTopics{}, module.ReadOnly},
		{contract.ListGroups{}, module.NotMine},
	} {
		if got := m.Mode(c.operation); got != c.want {
			t.Errorf("%T: mode %v, want %v", c.operation, got, c.want)
		}
	}
}

func TestDecodeRejectsDamage(t *testing.T) {
	m := topics.Module{}
	valid := engine.NewWriter()
	m.Encode(valid, topics.KindCommit, contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{
		{TopicPartition: contract.NewTopicPartition("t", 0), Records: 1, Loc: runtime.SegmentRef{Key: "o", Len: 1}},
	}})
	whole := valid.Bytes()
	// every cut of a valid commit must fail the reader, and never panic
	for n := range len(whole) {
		r := engine.NewReader(whole[:n])
		m.Decode(r, topics.KindCommit)
		if r.Done() {
			t.Fatalf("a commit cut to %d of %d bytes was accepted", n, len(whole))
		}
	}
	// a batch that points past the string table
	bad := engine.NewWriter()
	bad.String("k")
	bad.Uvarint(1) // one string in the table
	bad.String("t")
	bad.Uvarint(1) // one batch
	bad.Uvarint(5) // topic index 5: out of range
	if r := engine.NewReader(bad.Bytes()); func() bool { m.Decode(r, topics.KindCommit); return r.OK() }() {
		t.Fatal("an index outside the string table was accepted")
	}
	// a kind this module does not own
	r := engine.NewReader(nil)
	if got := m.Decode(r, 99); got != nil || r.OK() {
		t.Fatal("an unknown kind must fail the reader")
	}
	// Encode of an operation this module does not own writes nothing
	w := engine.NewWriter()
	m.Encode(w, topics.KindCommit, contract.ListGroups{})
	if len(w.Bytes()) != 0 {
		t.Fatal("encoded an operation that is not a topics operation")
	}
}
