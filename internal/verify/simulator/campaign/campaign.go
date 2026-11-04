// Package campaign runs many seeds of the simulator on several goroutines and
// sums up the results. Parallelism and fold live in dst/campaign; each run is
// still one goroutine and no real time.
package campaign

import (
	"fmt"
	"sort"
	"strings"
	"time"

	dstcampaign "github.com/arunlakshman/dst/campaign"
	"github.com/manifesterio/mds-controlplane/internal/verify/simulator"
)

// Campaign is the outcome of many seeds of one scenario.
type Campaign struct {
	Scenario string
	Seeds    int
	Failed   []simulator.Result // runs that broke a property, in seed order
	Runs     int
	Steps    int
	Slots    int
	Acked    int
	Hash     uint64 // all the trace hashes of the seeds folded in seed order
	Elapsed  time.Duration
	Coverage map[string]dstcampaign.Coverage
}

// RunCampaign runs seeds first, first+1, ... of one scenario. The runs are
// independent, so they run on several goroutines. Each run is single threaded
// inside, and the results are put back in seed order.
func RunCampaign(cfg simulator.Config, first uint64, seeds int) (Campaign, error) {
	if _, ok := simulator.ScenarioByName(cfg.Scenario); !ok {
		return Campaign{}, fmt.Errorf("unknown scenario %q (have %v)", cfg.Scenario, simulator.ScenarioNames())
	}
	results := make([]simulator.Result, max(seeds, 0))
	sum := dstcampaign.Run(first, seeds, 0, func(seed uint64) dstcampaign.Result {
		c := cfg
		c.Seed = seed
		r, _ := simulator.Run(c)
		results[int(seed-first)] = r
		cov := make(map[string]dstcampaign.Coverage, len(r.Coverage))
		for k, v := range r.Coverage {
			cov[k] = dstcampaign.Coverage{Kind: string(v.Kind), Checks: v.Checks, Passes: v.Passes}
		}
		return dstcampaign.Result{
			Hash:      r.Hash,
			OK:        r.OK(),
			Reproduce: r.Reproduce(),
			Coverage:  cov,
		}
	})
	out := Campaign{
		Scenario: cfg.Scenario,
		Seeds:    seeds,
		Runs:     seeds,
		Hash:     sum.Hash,
		Elapsed:  sum.Elapsed,
		Coverage: sum.Coverage,
	}
	for _, r := range results {
		out.Steps += r.Steps
		out.Slots += r.Stats.LogSlots
		out.Acked += r.Stats.Acked
		if !r.OK() {
			out.Failed = append(out.Failed, r)
		}
	}
	return out, nil
}

// Line is the one-line summary of the campaign.
func (c Campaign) Line() string {
	status := "ok"
	if len(c.Failed) > 0 {
		status = fmt.Sprintf("FAILED %d of %d seeds", len(c.Failed), c.Seeds)
	}
	return fmt.Sprintf("%-15s %4d seeds  %9d steps  %7d slots  %6d acked writes  %5.1fs  hash %08x  %s",
		c.Scenario, c.Seeds, c.Steps, c.Slots, c.Acked, c.Elapsed.Seconds(), uint32(c.Hash), status)
}

// Summary adds up the coverage of the scenarios of one campaign run.
type Summary struct{ cov map[string]dstcampaign.Coverage }

// Add adds the coverage of c.
func (s *Summary) Add(c Campaign) {
	if s.cov == nil {
		s.cov = map[string]dstcampaign.Coverage{}
	}
	for k, c := range c.Coverage {
		t := s.cov[k]
		t.Kind = c.Kind
		t.Checks += c.Checks
		t.Passes += c.Passes
		s.cov[k] = t
	}
}

// Coverage lists the replica paths that no run reached. The replica says which
// paths matter with Reachable and Sometimes; a path that no seed reaches is not
// tested, so it is worth a look.
func (s Summary) Coverage() string {
	var missing []string
	for _, p := range expectedReachable {
		if s.cov[p].Passes == 0 {
			missing = append(missing, p)
		}
	}
	sort.Strings(missing)
	if len(missing) == 0 {
		return fmt.Sprintf("coverage: all %d expected paths were reached", len(expectedReachable))
	}
	return "coverage: not reached by any seed:\n  " + strings.Join(missing, "\n  ")
}

// expectedReachable are the Reachable paths of the replica that a long campaign
// should reach. A short campaign may miss some.
var expectedReachable = []string{
	"logcp-sim: epoch claimed",
	"logcp-sim: standby took over",
	"logcp-sim: writer fenced, now read-only",
	"logcp-sim: replica restored from a checkpoint",
	"logcp-sim: log trimmed below a checkpoint",
	"logcp-sim: lost slot retried at a later slot",
	"logcp-sim: Put not answered, same slot repeated",
	"logcp-sim: replica hit a trimmed slot",
	"logcp-sim: checkpoint read had a bad CRC, read again",
	"logcp-sim: replica crashed",
	"logcp-sim: replica stalled",
}
