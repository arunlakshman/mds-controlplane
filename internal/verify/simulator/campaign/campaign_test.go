package campaign

import (
	"fmt"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/verify/simulator"
)

// TestCampaignReachesReplicaPaths: over a campaign of the chaos scenario the
// replica reaches the paths that the campaign summary lists.
func TestCampaignReachesReplicaPaths(t *testing.T) {
	var sum Summary
	for _, sc := range []string{"crash-restart", "leader-fencing", "chaos", "log-faults"} {
		c, err := RunCampaign(simulator.Config{Scenario: sc}, 1, 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(c.Failed) > 0 {
			t.Fatal(simulator.Report(c.Failed[0]))
		}
		sum.Add(c)
	}
	if text := sum.Coverage(); text != fmt.Sprintf("coverage: all %d expected paths were reached", len(expectedReachable)) {
		t.Fatal(text)
	}
}
