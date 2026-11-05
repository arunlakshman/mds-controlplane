package topics

import (
	"errors"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

func TestRunRoundTripAndCorruption(t *testing.T) {
	topicPartition := contract.NewTopicPartition("t", 3)
	rs := []contract.BatchRange{
		{TopicPartition: topicPartition, Base: 10, Count: 2, Loc: runtime.SegmentRef{Key: "s1", Off: 0, Len: 50}},
		{TopicPartition: topicPartition, Base: 12, Count: 5, Loc: runtime.SegmentRef{Key: "s1", Off: 50, Len: -1}},
		{TopicPartition: topicPartition, Base: 17, Count: 1, Loc: runtime.SegmentRef{Key: "s2", Off: 7, Len: 9}},
	}
	b := encodeBatchRun(rs)
	got, err := decodeBatchRun(topicPartition, 10, b)
	if err != nil || len(got) != 3 || got[2] != rs[2] || got[1] != rs[1] {
		t.Fatalf("round trip: %+v %v", got, err)
	}
	bad := [][]byte{nil, {9}, b[:len(b)-1], append(append([]byte{}, b...), 0), {runFormat, 0xFF}, {runFormat, 1, 5, 'a'},
		{runFormat, 0, 200}, {runFormat, 1, 1, 'k', 1, 5, 1, 0, 0}, {runFormat, 100}}
	for i, x := range bad {
		if _, err := decodeBatchRun(topicPartition, 0, x); !errors.Is(err, errCorrupt) {
			t.Errorf("case %d: err %v", i, err)
		}
	}
}

func TestIdemRecordCorruption(t *testing.T) {
	var dig [checksumLength]byte
	rec := duplicateCheckRecord(7, dig, []contract.BatchRange{{Base: 3}, {Base: 300}})
	if _, bases, err := parseDuplicateCheck(rec); err != nil || len(bases) != 2 || bases[1] != 300 {
		t.Fatalf("%v %v", bases, err)
	}
	for _, x := range [][]byte{nil, {0x80}, rec[:5], append(append([]byte{}, rec[:1+checksumLength]...), 0x80)} {
		if _, _, err := parseDuplicateCheck(x); err == nil {
			t.Errorf("accepted %v", x)
		}
	}
}

func TestCorruptStoredRecordsPanic(t *testing.T) {
	store := engine.NewMemory()
	topicPartition := contract.NewTopicPartition("t", 0)
	_ = store.Update(func(transaction engine.Txn) {
		transaction.Set(batchRunKey(topicPartition, 0), []byte{9, 9})
		transaction.Set(endKey(topicPartition), []byte{5})
		transaction.Set(duplicateCheckKey("k"), []byte{0x80})
	})
	for name, f := range map[string]func(){
		"lookup": func() {
			_ = store.View(func(transaction engine.Txn) {
				Lookup(transaction, contract.FindSegments{TopicPartition: topicPartition})
			})
		},
		"replay": func() {
			_ = store.Update(func(transaction engine.Txn) { Commit(transaction, 1, contract.CommitSegment{SegmentKey: "k"}) })
		},
	} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s: no panic on a corrupt record", name)
				}
			}()
			f()
		}()
	}
}
