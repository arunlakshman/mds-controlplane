package s3store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/runtime"
	"github.com/manifesterio/mds-controlplane/internal/system"
)

// The rules of the object store, as a table of steps. The same table runs on the
// S3 adapter over the in-process fake S3 server, and, when MDS_S3_ENDPOINT is
// set, over a real S3 endpoint (SeaweedFS, minio or AWS). The steps run in order
// on one store.
//
//	Put     An empty key is ErrInvalid. Else the object is stored, and it
//	        replaces any object that was there.
//	Get     An absent key is ErrNotFound. It reads [Off, Off+Len), and Len <= 0
//	        means to the end. A range outside the object (Off < 0, Off > size,
//	        Off+Len > size) is ErrInvalid. Off == size with Len <= 0 is empty.
//	Delete  An absent key is ErrNotFound. Else the object is removed.
//	List    The keys with the prefix, sorted. None is no keys.
type step struct {
	name string
	do   func(system.ObjectStore) (data string, keys []string, err error)
	data string
	keys []string
	code runtime.ErrCode // runtime.OK when the step must succeed
}

// ctx is the context of the steps. A step has no use for a deadline.
func ctx() context.Context { return context.Background() }

func put(key, data string) func(system.ObjectStore) (string, []string, error) {
	return func(s system.ObjectStore) (string, []string, error) {
		return "", nil, s.Put(ctx(), key, []byte(data))
	}
}

func get(key string, off, n int64) func(system.ObjectStore) (string, []string, error) {
	return func(s system.ObjectStore) (string, []string, error) {
		b, err := s.Get(ctx(), runtime.SegmentRef{Key: key, Off: off, Len: n})
		return string(b), nil, err
	}
}

func del(key string) func(system.ObjectStore) (string, []string, error) {
	return func(s system.ObjectStore) (string, []string, error) { return "", nil, s.Delete(ctx(), key) }
}

func list(prefix string) func(system.ObjectStore) (string, []string, error) {
	return func(s system.ObjectStore) (string, []string, error) {
		keys, err := s.List(ctx(), prefix)
		return "", keys, err
	}
}

var objectRules = []step{
	{name: "put", do: put("a", "0123456789")},
	{name: "put in a folder", do: put("dir/b", "xyz")},
	{name: "put empty data", do: put("dir/e", "")},
	{name: "put with an empty key", do: put("", "x"), code: runtime.ErrInvalid},
	{name: "get all", do: get("a", 0, 0), data: "0123456789"},
	{name: "get from an offset to the end", do: get("a", 3, 0), data: "3456789"},
	{name: "get a range", do: get("a", 2, 4), data: "2345"},
	{name: "get the last byte", do: get("a", 9, 1), data: "9"},
	{name: "get at the end", do: get("a", 10, 0), data: ""},
	{name: "get empty object", do: get("dir/e", 0, 0), data: ""},
	{name: "get an absent key", do: get("nope", 0, 0), code: runtime.ErrNotFound},
	{name: "get after the end", do: get("a", 11, 0), code: runtime.ErrInvalid},
	{name: "get a range that is too long", do: get("a", 5, 6), code: runtime.ErrInvalid},
	{name: "get a negative offset", do: get("a", -1, 2), code: runtime.ErrInvalid},
	{name: "put replaces", do: put("a", "AB")},
	{name: "get what replaced it", do: get("a", 0, 0), data: "AB"},
	{name: "list all", do: list(""), keys: []string{"a", "dir/b", "dir/e"}},
	{name: "list a prefix", do: list("dir/"), keys: []string{"dir/b", "dir/e"}},
	{name: "list no match", do: list("zz"), keys: nil},
	{name: "delete", do: del("dir/b")},
	{name: "delete again", do: del("dir/b"), code: runtime.ErrNotFound},
	{name: "get what was deleted", do: get("dir/b", 0, 0), code: runtime.ErrNotFound},
	{name: "list after delete", do: list("dir/"), keys: []string{"dir/e"}},
}

func runRules(t *testing.T, s system.ObjectStore) {
	for _, c := range objectRules {
		data, keys, err := c.do(s)
		code := runtime.OK
		if err != nil {
			code = codeOf(err)
		}
		if code != c.code {
			t.Fatalf("%s: code %v (%v), want %v", c.name, code, err, c.code)
		}
		if data != c.data || !slices.Equal(keys, c.keys) {
			t.Fatalf("%s: got data %q keys %q, want data %q keys %q", c.name, data, keys, c.data, c.keys)
		}
	}
}

// codeOf is the code of an error of the adapter. Any other error counts as ErrIO.
func codeOf(err error) runtime.ErrCode {
	if se, ok := errors.AsType[*Error](err); ok {
		return se.Code
	}
	return runtime.ErrIO
}

func TestObjectRulesFake(t *testing.T) {
	s, _ := newTestStore(t, "rules/")
	runRules(t, s)
}

// TestObjectRulesS3 runs the rules on a real S3 endpoint, only when
// MDS_S3_ENDPOINT is set. It uses a bucket of its own.
func TestObjectRulesS3(t *testing.T) {
	ep := os.Getenv("MDS_S3_ENDPOINT")
	if ep == "" {
		t.Skip("The environment variable MDS_S3_ENDPOINT is not set, so the test of the real S3 endpoint is skipped.")
	}
	s, err := New(t.Context(), Config{Endpoint: ep, Bucket: UniqueBucket(), Prefix: fmt.Sprintf("rules-%d/", os.Getpid()), PathStyle: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.EnsureBucket(t.Context()); err != nil {
		t.Fatal(err)
	}
	runRules(t, s)
}
