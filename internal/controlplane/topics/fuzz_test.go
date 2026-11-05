package topics

import (
	"bytes"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/kv"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/module"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// FuzzModuleDecode: the payload decoder of the module reads any bytes without a
// panic. It never panics on purpose: damage shows as r.OK() false. A decoded
// operation is checked to re-encode to bytes that decode and re-encode to the
// same bytes (the encoding is not canonical, so the input bytes are not compared).
func FuzzModuleDecode(f *testing.F) {
	tp := contract.NewTopicPartition
	for _, op := range []runtime.ControlPlaneOperation{
		contract.CreateTopic{Name: "orders", Partitions: 3},
		contract.CreateTopic{Name: "orders", Partitions: 3, FailIfExists: true},
		contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{
			{TopicPartition: tp("t", 0), Records: 3, Loc: runtime.SegmentRef{Key: "o", Off: 10, Len: 30}},
			{TopicPartition: tp("t", 1), Records: 2, Loc: runtime.SegmentRef{Key: "o", Off: 40, Len: 20}},
		}},
	} {
		kind, _ := Module{}.KindOf(op)
		w := kv.NewWriter()
		Module{}.Encode(w, kind, op)
		f.Add(uint8(kind), w.Bytes())
	}
	f.Add(uint8(0), []byte{})
	f.Add(uint8(200), []byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, k uint8, b []byte) {
		kind := module.Kind(k)
		r := kv.NewReader(b)
		op := Module{}.Decode(r, kind)
		if !r.Done() || op == nil {
			return
		}
		w := kv.NewWriter()
		Module{}.Encode(w, kind, op)
		first := w.Bytes()
		r2 := kv.NewReader(first)
		op2 := Module{}.Decode(r2, kind)
		if !r2.Done() {
			t.Fatalf("encoded operation does not decode: %+v", op)
		}
		w2 := kv.NewWriter()
		Module{}.Encode(w2, kind, op2)
		if !bytes.Equal(w2.Bytes(), first) {
			t.Fatalf("re-encoding changed the bytes")
		}
	})
}

// FuzzDecodeBatchRun: a stored R record decodes to ranges or to errCorrupt, and
// never panics. (The caller panics on errCorrupt on purpose, fail-stop on
// corrupt state; that is outside this decoder.) A record that decoded encodes
// to a record that decodes to the same ranges.
func FuzzDecodeBatchRun(f *testing.F) {
	tp := contract.NewTopicPartition("t", 3)
	f.Add(encodeBatchRun([]contract.BatchRange{
		{TopicPartition: tp, Base: 7, Count: 3, Loc: runtime.SegmentRef{Key: "a", Off: 1, Len: 2}},
		{TopicPartition: tp, Base: 10, Count: 4, Loc: runtime.SegmentRef{Key: "b", Off: 5, Len: 6}},
		{TopicPartition: tp, Base: 14, Count: 1, Loc: runtime.SegmentRef{Key: "a", Off: 9, Len: 1}},
	}))
	f.Add([]byte{})
	f.Add([]byte{runFormat})
	f.Fuzz(func(t *testing.T, b []byte) {
		rs, err := decodeBatchRun(tp, 7, b)
		if err != nil {
			return
		}
		rs2, err := decodeBatchRun(tp, 7, encodeBatchRun(rs))
		if err != nil {
			t.Fatalf("a record that decoded does not decode after encode: %v", err)
		}
		if len(rs) != len(rs2) {
			t.Fatalf("round trip changed the count: %d, %d", len(rs), len(rs2))
		}
		for i := range rs {
			if rs[i] != rs2[i] {
				t.Fatalf("range %d: %+v, %+v", i, rs[i], rs2[i])
			}
		}
	})
}

// FuzzParseDuplicateCheck: a stored duplicate-check record parses or returns
// errCorrupt, and never panics.
func FuzzParseDuplicateCheck(f *testing.F) {
	f.Add(duplicateCheckRecord(5, [checksumLength]byte{1, 2, 3}, []contract.BatchRange{{Base: 7}, {Base: 12}}))
	f.Add([]byte{})
	f.Add([]byte{0x80})
	f.Fuzz(func(t *testing.T, b []byte) {
		_, _, _ = parseDuplicateCheck(b)
	})
}
