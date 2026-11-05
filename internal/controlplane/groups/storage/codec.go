package storage

import (
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/module"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// The log entry kinds of this module. The numbers are part of the log format.
const (
	// KindGroupInstall is a GroupInstall.
	KindGroupInstall module.Kind = 3
	// KindGroupAssign is a GroupAssign.
	KindGroupAssign module.Kind = 4
	// KindGroupRemove is a GroupRemove.
	KindGroupRemove module.Kind = 5
	// KindOffsetCommit is an OffsetCommit.
	KindOffsetCommit module.Kind = 6
)

// Kinds lists the log entry kinds of the module.
func (Module) Kinds() []module.Kind {
	return []module.Kind{KindGroupInstall, KindGroupAssign, KindGroupRemove, KindOffsetCommit}
}

// KindOf is the kind of a group write.
func (Module) KindOf(operation runtime.ControlPlaneOperation) (module.Kind, bool) {
	switch operation.(type) {
	case contract.GroupInstall:
		return KindGroupInstall, true
	case contract.GroupAssign:
		return KindGroupAssign, true
	case contract.GroupRemove:
		return KindGroupRemove, true
	case contract.OffsetCommit:
		return KindOffsetCommit, true
	}
	return 0, false
}

// Encode writes the payload of a group write.
func (Module) Encode(w *engine.Writer, _ module.Kind, operation runtime.ControlPlaneOperation) {
	switch x := operation.(type) {
	case contract.GroupInstall:
		encodeInstall(w, x)
	case contract.GroupAssign:
		encodeAssign(w, x)
	case contract.GroupRemove:
		encodeRemove(w, x)
	case contract.OffsetCommit:
		encodeOffsetCommit(w, x)
	}
}

// Decode reads the payload of an entry of kind.
func (Module) Decode(r *engine.Reader, kind module.Kind) runtime.ControlPlaneOperation {
	switch kind {
	case KindGroupInstall:
		return decodeInstall(r)
	case KindGroupAssign:
		return decodeAssign(r)
	case KindGroupRemove:
		return decodeRemove(r)
	case KindOffsetCommit:
		return decodeOffsetCommit(r)
	}
	r.Fail()
	return nil
}

func encodeInstall(w *engine.Writer, operation contract.GroupInstall) {
	w.String(operation.Group)
	w.String(operation.ProtocolType)
	w.String(operation.Protocol)
	w.Varint(int64(operation.Generation))
	w.String(operation.Leader)
	w.Varint(operation.TimeMs)
	w.Uvarint(uint64(len(operation.Members)))
	for _, m := range operation.Members {
		w.String(m.MemberID)
		w.String(m.ClientID)
		w.String(m.ClientHost)
		w.String(m.GroupInstanceID)
		w.Varint(int64(m.SessionTimeoutMs))
		w.Varint(int64(m.RebalanceTimeoutMs))
		w.String(string(m.Metadata))
	}
}

func decodeInstall(r *engine.Reader) contract.GroupInstall {
	var operation contract.GroupInstall
	operation.Group = r.String()
	operation.ProtocolType = r.String()
	operation.Protocol = r.String()
	operation.Generation = int32(r.Varint())
	operation.Leader = r.String()
	operation.TimeMs = r.Varint()
	for n := r.Count(); n > 0 && r.OK(); n-- {
		var m contract.GroupMember
		m.MemberID = r.String()
		m.ClientID = r.String()
		m.ClientHost = r.String()
		m.GroupInstanceID = r.String()
		m.SessionTimeoutMs = int32(r.Varint())
		m.RebalanceTimeoutMs = int32(r.Varint())
		m.Metadata = r.Bytes()
		operation.Members = append(operation.Members, m)
	}
	return operation
}

func encodeAssign(w *engine.Writer, operation contract.GroupAssign) {
	w.String(operation.Group)
	w.Varint(int64(operation.Generation))
	w.String(operation.Leader)
	w.Varint(operation.TimeMs)
	w.Uvarint(uint64(len(operation.Assignments)))
	for _, a := range operation.Assignments {
		w.String(a.Member)
		w.String(string(a.Bytes))
	}
}

func decodeAssign(r *engine.Reader) contract.GroupAssign {
	var operation contract.GroupAssign
	operation.Group = r.String()
	operation.Generation = int32(r.Varint())
	operation.Leader = r.String()
	operation.TimeMs = r.Varint()
	for n := r.Count(); n > 0 && r.OK(); n-- {
		var a contract.GroupAssignment
		a.Member = r.String()
		a.Bytes = r.Bytes()
		operation.Assignments = append(operation.Assignments, a)
	}
	return operation
}

func encodeRemove(w *engine.Writer, operation contract.GroupRemove) {
	w.String(operation.Group)
	w.Varint(operation.TimeMs)
	w.String(operation.Reason)
	w.Uvarint(uint64(len(operation.Members)))
	for _, m := range operation.Members {
		w.String(m)
	}
}

func decodeRemove(r *engine.Reader) contract.GroupRemove {
	var operation contract.GroupRemove
	operation.Group = r.String()
	operation.TimeMs = r.Varint()
	operation.Reason = r.String()
	for n := r.Count(); n > 0 && r.OK(); n-- {
		operation.Members = append(operation.Members, r.String())
	}
	return operation
}

func encodeOffsetCommit(w *engine.Writer, operation contract.OffsetCommit) {
	w.String(operation.Group)
	w.String(operation.Member)
	w.Varint(int64(operation.Generation))
	w.Varint(operation.TimeMs)
	w.Uvarint(uint64(len(operation.Offsets)))
	for _, o := range operation.Offsets {
		w.String(o.TopicPartition.Topic)
		w.Varint(int64(o.TopicPartition.Partition))
		w.Varint(o.Offset)
		w.Varint(int64(o.LeaderEpoch))
		w.String(o.Metadata)
	}
}

func decodeOffsetCommit(r *engine.Reader) contract.OffsetCommit {
	var operation contract.OffsetCommit
	operation.Group = r.String()
	operation.Member = r.String()
	operation.Generation = int32(r.Varint())
	operation.TimeMs = r.Varint()
	for n := r.Count(); n > 0 && r.OK(); n-- {
		var o contract.PartitionOffset
		o.TopicPartition.Topic = r.String()
		o.TopicPartition.Partition = int32(r.Varint())
		o.Offset = r.Varint()
		o.LeaderEpoch = int32(r.Varint())
		o.Metadata = r.String()
		operation.Offsets = append(operation.Offsets, o)
	}
	return operation
}
