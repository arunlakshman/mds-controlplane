package storage

import (
	"bytes"
	"errors"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/module"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// FuzzModuleDecode: the payload decoder of the module reads any bytes without a
// panic. It never panics on purpose: damage shows as r.OK() false. A decoded
// operation is checked to re-encode to bytes that decode and re-encode to the
// same bytes (the encoding is not canonical, so the input bytes are not compared).
func FuzzModuleDecode(f *testing.F) {
	m := NewModule(nil)
	tp := contract.NewTopicPartition
	for _, op := range []runtime.ControlPlaneOperation{
		contract.GroupInstall{Group: "g", ProtocolType: "consumer", Protocol: "range", Generation: 1, Leader: "m", TimeMs: 9,
			Members: []contract.GroupMember{{MemberID: "m", ClientID: "c", ClientHost: "h", SessionTimeoutMs: 10, RebalanceTimeoutMs: 20, Metadata: []byte("x")}}},
		contract.GroupAssign{Group: "g", Generation: 1, Leader: "m", Assignments: []contract.GroupAssignment{{Member: "m", Bytes: []byte("a")}}},
		contract.GroupRemove{Group: "g", Reason: "r", Members: []string{"m", "n"}},
		contract.OffsetCommit{Group: "g", Member: "m", Generation: 1, Offsets: []contract.PartitionOffset{
			{TopicPartition: tp("t", 0), Offset: 5, LeaderEpoch: 1, Metadata: "md"}}},
	} {
		kind, ok := m.KindOf(op)
		if !ok {
			f.Fatalf("%T has no kind", op)
		}
		w := engine.NewWriter()
		m.Encode(w, kind, op)
		f.Add(uint8(kind), w.Bytes())
	}
	f.Add(uint8(0), []byte{})
	f.Add(uint8(200), []byte{1, 2, 3})
	f.Fuzz(func(t *testing.T, k uint8, b []byte) {
		kind := module.Kind(k)
		r := engine.NewReader(b)
		op := m.Decode(r, kind)
		if !r.Done() || op == nil {
			return
		}
		w := engine.NewWriter()
		m.Encode(w, kind, op)
		first := w.Bytes()
		r2 := engine.NewReader(first)
		op2 := m.Decode(r2, kind)
		if !r2.Done() {
			t.Fatalf("encoded operation does not decode: %+v", op)
		}
		w2 := engine.NewWriter()
		m.Encode(w2, kind, op2)
		if !bytes.Equal(w2.Bytes(), first) {
			t.Fatalf("re-encoding changed the bytes")
		}
	})
}

// corruptOrValue runs a state-record decoder. These decoders panic with
// errCorrupt on purpose when a stored record is damaged (fail-stop on corrupt
// state: the node must not go on with a state it cannot read). That panic is the
// intended answer to bad bytes, so it is allowed here. Any other panic is a bug,
// and it is raised again. It reports whether the record decoded.
func corruptOrValue(t *testing.T, decode func()) bool {
	t.Helper()
	decoded := false
	func() {
		defer func() {
			if p := recover(); p != nil {
				if err, ok := p.(error); ok && errors.Is(err, errCorrupt) {
					return
				}
				panic(p)
			}
		}()
		decode()
		decoded = true
	}()
	return decoded
}

// FuzzDecodeHeader: a stored group header decodes, or panics with errCorrupt
// (the intended fail-stop). A header that decoded encodes to the same header.
func FuzzDecodeHeader(f *testing.F) {
	f.Add(groupHeader{generation: 3, protoType: "consumer", protocol: "range", leader: "m", assignedGen: 2}.encode())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		var h groupHeader
		if !corruptOrValue(t, func() { h = decodeHeader("g", b) }) {
			return
		}
		if h2 := decodeHeader("g", h.encode()); h2 != h {
			t.Fatalf("round trip: %+v, %+v", h, h2)
		}
	})
}

// FuzzDecodeMember: a stored member record decodes, or panics with errCorrupt
// (the intended fail-stop). A member that decoded re-encodes to bytes that
// decode and re-encode to the same bytes.
func FuzzDecodeMember(f *testing.F) {
	f.Add(encodeMember(contract.GroupMemberInfo{
		GroupMember:  contract.GroupMember{ClientID: "c", ClientHost: "h", GroupInstanceID: "i", SessionTimeoutMs: 1, RebalanceTimeoutMs: 2, Metadata: []byte("md")},
		JoinedTimeMs: 7, Assignment: []byte("as"),
	}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		var m contract.GroupMemberInfo
		if !corruptOrValue(t, func() { m = decodeMember("g", "id", b) }) {
			return
		}
		again := encodeMember(m)
		if !bytes.Equal(encodeMember(decodeMember("g", "id", again)), again) {
			t.Fatalf("re-encoding changed the bytes")
		}
	})
}

// FuzzDecodeOffset: a stored offset record decodes, or panics with errCorrupt
// (the intended fail-stop). An offset that decoded encodes to the same offset.
func FuzzDecodeOffset(f *testing.F) {
	f.Add(offsetRecord{offset: 5, epoch: 1, meta: "md", time: 99}.encode())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		var o offsetRecord
		if !corruptOrValue(t, func() { o = decodeOffset("g", b) }) {
			return
		}
		if o2 := decodeOffset("g", o.encode()); o2 != o {
			t.Fatalf("round trip: %+v, %+v", o, o2)
		}
	})
}
