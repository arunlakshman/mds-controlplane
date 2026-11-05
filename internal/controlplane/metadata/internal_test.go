package metadata

import (
	"errors"
	"strings"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/metadb/engine"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// brokenKV fails every transaction.
type brokenKV struct{}

func (*brokenKV) View(func(engine.Txn)) error { return errors.New("disk gone") }

func (*brokenKV) Update(func(engine.Txn)) error { return errors.New("disk gone") }

func (*brokenKV) Close() error { return nil }

func TestStoreFailureIsFailStop(t *testing.T) {
	a := New(&brokenKV{})
	if _, err := a.Commit(0, contract.CommitSegment{SegmentKey: "k"}); err == nil {
		t.Error("Commit must return the store error")
	}
	for name, f := range map[string]func(){
		"Exec":        func() { a.Exec(runtime.ControlPlaneCall{Op: contract.DescribeTopics{}}) },
		"CreateTopic": func() { a.CreateTopic("t", 1) },
		"End":         func() { a.End(contract.NewTopicPartition("t", 0)) },
	} {
		func() {
			defer func() {
				if r := recover(); r == nil || !strings.Contains(r.(error).Error(), "store failed") {
					t.Errorf("%s: recover = %v", name, r)
				}
			}()
			f()
		}()
	}
}
