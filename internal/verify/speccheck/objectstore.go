package speccheck

import (
	"slices"
	"strings"

	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// ObjectStore is the spec of the object store: a map from key to bytes.
// Sequential and fault-free, so every completion is Err OK (or a truthful
// ErrNotFound/ErrInvalid) and Outcome is Applied or NotApplied, never
// MaybeApplied. Rules:
//
//	Put     empty Key: ErrInvalid, no change. Else stores a copy, replacing
//	        any object: OK, Applied.
//	Get     absent Key: ErrNotFound. Reads [Off, Off+Len), Len <= 0 meaning
//	        to the end. Off < 0, Off > size or Off+Len > size: ErrInvalid
//	        (a range outside the object is a malformed request, not a
//	        missing one). Off == size with Len <= 0 gives empty data.
//	Delete  absent Key: ErrNotFound, NotApplied. Else removes it: OK, Applied.
//	List    keys with Prefix, sorted; nil if none. Never fails.
//
// Exec panics on an action that is not one of these four.
type ObjectStore struct {
	objs map[string][]byte
	keys []string // every key, sorted
	// pre[i] is the StateHash state after folding keys[:i]; len(pre)-1 is how
	// many are valid. A change at index i truncates it to i+1 entries.
	pre []stateHasher
}

// NewObjectStore returns an empty store.
func NewObjectStore() *ObjectStore { return &ObjectStore{objs: map[string][]byte{}} }

// touch records that the entry at index i of keys changed (or was inserted or
// removed there), so folded prefix states beyond it are stale.
func (s *ObjectStore) touch(i int) {
	if len(s.pre) > i+1 {
		s.pre = s.pre[:i+1]
	}
}

// Exec runs one action and returns its completion.
func (s *ObjectStore) Exec(a runtime.IOCall) runtime.IOResult {
	switch a := a.(type) {
	case runtime.ObjectPut:
		return s.execPut(a)
	case runtime.ObjectGet:
		return s.execGet(a)
	case runtime.ObjectDelete:
		return s.execDelete(a)
	case runtime.ObjectList:
		return s.execList(a)
	}
	panic("speccheck.ObjectStore: not an object store action")
}

func (s *ObjectStore) execPut(a runtime.ObjectPut) runtime.IOResult {
	if a.Key == "" {
		return runtime.ObjectPutDone{Req: a.Req, Key: a.Key, Err: runtime.ErrInvalid}
	}
	i, found := slices.BinarySearch(s.keys, a.Key)
	if !found {
		s.keys = slices.Insert(s.keys, i, a.Key)
	}
	s.touch(i)
	s.objs[a.Key] = clone(a.Data)
	return runtime.ObjectPutDone{Req: a.Req, Key: a.Key, Outcome: runtime.Applied}
}

func (s *ObjectStore) execGet(a runtime.ObjectGet) runtime.IOResult {
	data, ok := s.objs[a.Ref.Key]
	if !ok {
		return runtime.ObjectGetDone{Req: a.Req, Err: runtime.ErrNotFound}
	}
	got, ok := cutRange(data, a.Ref)
	if !ok {
		return runtime.ObjectGetDone{Req: a.Req, Err: runtime.ErrInvalid}
	}
	return runtime.ObjectGetDone{Req: a.Req, Data: clone(got)}
}

func cutRange(data []byte, ref runtime.SegmentRef) ([]byte, bool) {
	end := int64(len(data))
	if ref.Len > 0 {
		end = ref.Off + ref.Len
	}
	if ref.Off < 0 || ref.Off > int64(len(data)) || end > int64(len(data)) {
		return nil, false
	}
	return data[ref.Off:end], true
}

func (s *ObjectStore) execDelete(a runtime.ObjectDelete) runtime.IOResult {
	if _, ok := s.objs[a.Key]; !ok {
		return runtime.ObjectDeleteDone{Req: a.Req, Err: runtime.ErrNotFound}
	}
	i, _ := slices.BinarySearch(s.keys, a.Key)
	s.keys = slices.Delete(s.keys, i, i+1)
	s.touch(i)
	delete(s.objs, a.Key)
	return runtime.ObjectDeleteDone{Req: a.Req, Outcome: runtime.Applied}
}

func (s *ObjectStore) execList(a runtime.ObjectList) runtime.IOResult {
	var keys []string
	for _, k := range s.keys {
		if strings.HasPrefix(k, a.Prefix) {
			keys = append(keys, k)
		}
	}
	return runtime.ObjectListDone{Req: a.Req, Keys: keys}
}

// Keys returns every key, sorted.
func (s *ObjectStore) Keys() []string { return slices.Clone(s.keys) }

// StateHash folds the sorted (key, data) pairs. It resumes from the last
// folded prefix a change left intact, so a put near the end of the key order
// costs little.
func (s *ObjectStore) StateHash() uint64 {
	s.pre = nextPrefix(s.pre, len(s.keys), func(i int, h stateHasher) stateHasher {
		h.StringValue(s.keys[i])
		h.Bytes(s.objs[s.keys[i]])
		return h
	})
	return uint64(s.pre[len(s.keys)])
}

// fullStateHash is StateHash without the prefix cache (for tests).
func (s *ObjectStore) fullStateHash() uint64 {
	h := newStateHasher()
	for _, k := range sortedKeys(s.objs) {
		h.StringValue(k)
		h.Bytes(s.objs[k])
	}
	return uint64(h)
}
