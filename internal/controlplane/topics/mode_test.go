package topics_test

import (
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/module"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/topics"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

func TestMode(t *testing.T) {
	m := topics.Module{}
	for _, c := range []struct {
		operation runtime.ControlPlaneOperation
		want      module.Mode
	}{
		{contract.CommitSegment{}, module.Logged},
		{contract.CreateTopic{}, module.Logged},
		{contract.FindSegments{}, module.ReadOnly},
		{contract.DescribeTopics{}, module.ReadOnly},
		{contract.ListGroups{}, module.NotMine},
	} {
		if got := m.Mode(c.operation); got != c.want {
			t.Errorf("%T: mode %v, want %v", c.operation, got, c.want)
		}
	}
}
