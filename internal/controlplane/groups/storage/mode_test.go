package storage_test

import (
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/groups/storage"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/module"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

func TestModeAndForeignOperations(t *testing.T) {
	m := storage.NewModule(nil)
	for _, c := range []struct {
		operation runtime.ControlPlaneOperation
		want      module.Mode
	}{
		{contract.GroupInstall{}, module.Logged},
		{contract.GroupAssign{}, module.Logged},
		{contract.GroupRemove{}, module.Logged},
		{contract.OffsetCommit{}, module.Logged},
		{contract.OffsetFetch{}, module.ReadOnly},
		{contract.DescribeGroup{}, module.ReadOnly},
		{contract.ListGroups{}, module.ReadOnly},
		{contract.CreateTopic{}, module.NotMine},
		{contract.DescribeTopics{}, module.NotMine},
	} {
		if got := m.Mode(c.operation); got != c.want {
			t.Errorf("%T: mode %v, want %v", c.operation, got, c.want)
		}
	}
	if d := m.Read(nil, contract.CreateTopic{}); d.Err != runtime.ErrInvalid {
		t.Fatalf("Read of a foreign operation: %+v", d)
	}
}
