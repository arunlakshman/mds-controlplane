package metadata

import (
	"errors"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/groups/storage"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/kv"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/module"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/topics"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// The log format of an entry is: the kind (uvarint), the id (string), and then
// the payload. The kernel writes the kind and the id. The module that owns the
// kind writes and reads the payload, so a new context brings its own encoding
// and the kernel does not change. The bytes of every kind are fixed by the
// golden test (TestGoldenEntryBytes): a log written by an older node must be
// read by a newer one.

// Kind is the kind of a log entry. See module.Kind.
type Kind = module.Kind

// The entry kinds. The kernel owns the no-op (zero). Each other kind belongs to
// the module that defines it.
const (
	KindNoop                 Kind = 0
	KindCommit                    = topics.KindCommit
	KindCreateTopic               = topics.KindCreateTopic
	KindCreateTopicExclusive      = topics.KindCreateTopicExclusive
	KindGroupInstall              = storage.KindGroupInstall
	KindGroupAssign               = storage.KindGroupAssign
	KindGroupRemove               = storage.KindGroupRemove
	KindOffsetCommit              = storage.KindOffsetCommit
)

// Entry is one slot's command payload. KindNoop changes nothing on apply.
type Entry struct {
	ID   string
	Kind Kind
	// Op is the logged operation that the entry carries. It is nil for KindNoop.
	Op runtime.ControlPlaneOperation
}

// CommitSegment is the CommitSegment that the entry carries. It is false for any
// other entry.
func (e Entry) CommitSegment() (contract.CommitSegment, bool) {
	c, ok := e.Op.(contract.CommitSegment)
	return c, ok
}

// ErrCorrupt means the bytes are not a valid Entry.
var ErrCorrupt = errors.New("metadata: corrupt entry")

// registry is the list of modules, and which module owns each entry kind. Every
// Store uses the same list, and so does the codec.
var registry = newRegistry(topics.Module{}, storage.NewModule(topics.PartitionCount))

type modules struct {
	list  []module.Module
	owner map[Kind]module.Module
}

func newRegistry(list ...module.Module) modules {
	r := modules{list: list, owner: map[Kind]module.Module{}}
	for _, m := range list {
		for _, k := range m.Kinds() {
			r.owner[k] = m
		}
	}
	return r
}

// EntryOf is the log entry of a logged operation. It returns false for an
// operation that is not logged (a read, or an unknown one).
func EntryOf(operation runtime.ControlPlaneOperation) (Entry, bool) {
	for _, m := range registry.list {
		if kind, ok := m.KindOf(operation); ok {
			return Entry{Kind: kind, Op: operation}, true
		}
	}
	return Entry{}, false
}

// CreateEntry is the log entry of a CreateTopic.
func CreateEntry(operation contract.CreateTopic) Entry {
	e, _ := EntryOf(operation)
	return e
}

// Encode serialises an entry.
func Encode(en Entry) []byte {
	w := kv.NewWriter()
	w.Uvarint(uint64(en.Kind))
	w.String(en.ID)
	if m := registry.owner[en.Kind]; m != nil {
		m.Encode(w, en.Kind, en.Op)
	}
	return w.Bytes()
}

// Decode parses an entry made by Encode.
func Decode(b []byte) (Entry, error) {
	r := kv.NewReader(b)
	en := Entry{Kind: Kind(r.Uvarint())}
	en.ID = r.String()
	if en.Kind != KindNoop {
		m := registry.owner[en.Kind]
		if m == nil {
			return Entry{}, ErrCorrupt
		}
		en.Op = m.Decode(r, en.Kind)
	}
	if !r.Done() {
		return Entry{}, ErrCorrupt
	}
	return en, nil
}

// NewMem returns a metadata store over an in-memory KV. No Pebble. Suitable for the
// DSM Step fold and for tests that need the pure command fold.
func NewMem() *Store { return New(kv.NewMemory()) }

// Apply folds one log entry at slot into the store. No-ops leave state unchanged.
// A store failure panics (fail-stop), matching Commit.
func (a *Store) Apply(slot int64, e Entry) runtime.ControlPlaneDone {
	if e.Kind == KindNoop {
		return runtime.ControlPlaneDone{Req: runtime.RequestID(slot), Outcome: runtime.Applied}
	}
	if e.Op == nil {
		return runtime.ControlPlaneDone{Req: runtime.RequestID(slot), Err: runtime.ErrInvalid}
	}
	d, err := a.Write(slot, e.Op)
	mustStore(err)
	d.Req = runtime.RequestID(slot)
	return d
}

// ApplyBytes decodes data then Apply. Returns ErrCorrupt without mutating state.
func (a *Store) ApplyBytes(slot int64, data []byte) (runtime.ControlPlaneDone, error) {
	e, err := Decode(data)
	if err != nil {
		return runtime.ControlPlaneDone{}, err
	}
	return a.Apply(slot, e), nil
}
