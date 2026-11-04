// Package campaign runs many seeds of the simulator on several goroutines and
// sums up the results. The goroutines and the wall clock live here, outside the
// deterministic engine: every run is still one goroutine and no real time.
package campaign

import (
	"fmt"
	goruntime "runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/manifesterio/mds-controlplane/internal/runtime"
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
	Coverage map[string]simulator.Coverage
}

// RunCampaign runs seeds first, first+1, ... of one scenario. The runs are
// independent, so they run on several goroutines. Each run is single threaded
// inside, and the results are put back in seed order.
func RunCampaign(cfg simulator.Config, first uint64, seeds int) (Campaign, error) {
	if _, ok := simulator.ScenarioByName(cfg.Scenario); !ok {
		return Campaign{}, fmt.Errorf("unknown scenario %q (have %v)", cfg.Scenario, simulator.ScenarioNames())
	}
	start := time.Now() // wall time is only used to report how long the campaign took
	results := make([]simulator.Result, seeds)
	var wg sync.WaitGroup
	jobs := make(chan int)
	for range min(goruntime.GOMAXPROCS(0), max(seeds, 1)) {
		wg.Go(func() {
			for i := range jobs {
				c := cfg
				c.Seed = first + uint64(i)
				results[i], _ = simulator.Run(c)
			}
		})
	}
	for i := range seeds {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	out := Campaign{Scenario: cfg.Scenario, Seeds: seeds, Runs: seeds, Coverage: map[string]simulator.Coverage{}}
	h := runtime.NewHasher()
	for _, r := range results {
		h.Uint64Value(r.Hash)
		out.Steps += r.Steps
		out.Slots += r.Stats.LogSlots
		out.Acked += r.Stats.Acked
		if !r.OK() {
			out.Failed = append(out.Failed, r)
		}
		addCoverage(out.Coverage, r.Coverage)
	}
	out.Hash = h.Sum()
	out.Elapsed = time.Since(start)
	return out, nil
}

func addCoverage(into, from map[string]simulator.Coverage) {
	for k, c := range from { //determinismcheck:ok sums into a map; order does not matter
		t := into[k]
		t.Kind = c.Kind
		t.Checks += c.Checks
		t.Passes += c.Passes
		into[k] = t
	}
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
type Summary struct{ cov map[string]simulator.Coverage }

// Add adds the coverage of c.
func (s *Summary) Add(c Campaign) {
	if s.cov == nil {
		s.cov = map[string]simulator.Coverage{}
	}
	addCoverage(s.cov, c.Coverage)
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
