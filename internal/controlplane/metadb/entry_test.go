package metadb_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadb"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

func TestEntryCodecRoundTrip(t *testing.T) {
	es := []metadb.Entry{
		{ID: "w/1", Kind: metadb.KindNoop},
		{ID: "w/2", Kind: metadb.KindCreateTopic, Op: contract.CreateTopic{Name: "t", Partitions: 5}},
		{ID: "w/2x", Kind: metadb.KindCreateTopicExclusive, Op: contract.CreateTopic{Name: "t", Partitions: 5, FailIfExists: true}},
		{ID: "w/3", Kind: metadb.KindCommit, Op: contract.CommitSegment{
			SegmentKey: "k",
			Batches: []contract.PendingBatch{
				{TopicPartition: contract.NewTopicPartition("a", 0), Records: 3, Loc: runtime.SegmentRef{Key: "o", Off: 7, Len: 30}},
				{TopicPartition: contract.NewTopicPartition("a", 1), Records: 1, Loc: runtime.SegmentRef{Key: "o", Off: 1, Len: 10}},
			},
		}},
	}
	for _, e := range es {
		got, err := metadb.Decode(metadb.Encode(e))
		if err != nil || got.ID != e.ID || got.Kind != e.Kind || !reflect.DeepEqual(got.Op, e.Op) {
			t.Fatalf("round trip %+v -> %+v (%v)", e, got, err)
		}
	}
}

func TestApplyBytesNoopAndCommit(t *testing.T) {
	a := metadb.NewMem()
	t.Cleanup(func() { _ = a.Close() })
	noop, err := a.ApplyBytes(0, metadb.Encode(metadb.Entry{ID: "n", Kind: metadb.KindNoop}))
	if err != nil || noop.Err != runtime.OK {
		t.Fatalf("noop: %+v %v", noop, err)
	}
	before := a.StateHash()
	d, err := a.ApplyBytes(1, metadb.Encode(metadb.Entry{
		ID: "c", Kind: metadb.KindCommit,
		Op: contract.CommitSegment{
			SegmentKey: "s1",
			Batches:    []contract.PendingBatch{{TopicPartition: contract.NewTopicPartition("t", 0), Records: 2, Loc: runtime.SegmentRef{Key: "o", Off: 0, Len: 8}}},
		},
	}))
	if err != nil || d.Err != runtime.OK || d.Outcome != runtime.Applied {
		t.Fatalf("commit: %+v %v", d, err)
	}
	if a.StateHash() == before {
		t.Fatal("commit left StateHash unchanged")
	}
	if end := endOf(t, a, contract.NewTopicPartition("t", 0)); end != 2 {
		t.Fatalf("end = %d, want 2", end)
	}
}

func TestApplyBytesCorrupt(t *testing.T) {
	a := metadb.NewMem()
	t.Cleanup(func() { _ = a.Close() })
	if _, err := a.ApplyBytes(0, []byte{9}); err == nil {
		t.Fatal("corrupt entry accepted")
	}
}

// Every kind belongs to one module, kinds do not repeat, and the no-op (zero) is
// the kernel's own. A repeated number would make a log entry mean two things.
func TestKindsAreUniqueAndNonZero(t *testing.T) {
	seen := map[metadb.Kind]bool{}
	for _, kind := range []metadb.Kind{
		metadb.KindCommit, metadb.KindCreateTopic, metadb.KindCreateTopicExclusive,
		metadb.KindGroupInstall, metadb.KindGroupAssign, metadb.KindGroupRemove, metadb.KindOffsetCommit,
	} {
		if kind == metadb.KindNoop {
			t.Fatal("a module kind must not be the no-op")
		}
		if seen[kind] {
			t.Fatalf("kind %d is used twice", kind)
		}
		seen[kind] = true
	}
}

func TestDecodeRejectsUnknownKindAndLeftoverBytes(t *testing.T) {
	if _, err := metadb.Decode([]byte{0xFE, 0}); !errors.Is(err, metadb.ErrCorrupt) {
		t.Fatalf("unknown kind: %v", err)
	}
	if _, err := metadb.Decode(append(metadb.Encode(metadb.Entry{ID: "n", Kind: metadb.KindNoop}), 0)); !errors.Is(err, metadb.ErrCorrupt) {
		t.Fatalf("leftover byte: %v", err)
	}
}
