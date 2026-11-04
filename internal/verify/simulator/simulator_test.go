package simulator

import (
	"reflect"
	"sync"
	"testing"

	"github.com/arunlakshman/dst/mutant"
)

func mustRun(t *testing.T, cfg Config) Result {
	t.Helper()
	res, err := Run(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func seedCount(short, long int) int {
	if testing.Short() {
		return short
	}
	return long
}

// TestSameSeedSameHash is the proof of determinism: a seed run twice gives the
// same trace hash, the same counters and the same coverage. Go randomizes map
// iteration on every run, so a run that depends on it would differ here.
func TestSameSeedSameHash(t *testing.T) {
	for _, sc := range ScenarioNames() {
		t.Run(sc, func(t *testing.T) {
			for seed := uint64(1); seed <= uint64(seedCount(4, 25)); seed++ {
				a := mustRun(t, Config{Scenario: sc, Seed: seed})
				b := mustRun(t, Config{Scenario: sc, Seed: seed})
				if a.Hash != b.Hash || !reflect.DeepEqual(a.Stats, b.Stats) || !reflect.DeepEqual(a.Coverage, b.Coverage) {
					t.Fatalf("seed %d: two runs differ: hash %x vs %x, stats %+v vs %+v", seed, a.Hash, b.Hash, a.Stats, b.Stats)
				}
			}
		})
	}
}

// TestSameSeedSameHashInParallel runs one seed on many goroutines. Runs share
// no state, so every one must give the same hash.
func TestSameSeedSameHashInParallel(t *testing.T) {
	const runs = 8
	hashes := make([]uint64, runs)
	var wg sync.WaitGroup
	for i := range runs {
		wg.Go(func() { hashes[i] = mustRun(t, Config{Scenario: "chaos", Seed: 7}).Hash })
	}
	wg.Wait()
	for i, h := range hashes {
		if h != hashes[0] {
			t.Fatalf("run %d has hash %x, run 0 has %x", i, h, hashes[0])
		}
	}
}

func TestDifferentSeedsDiffer(t *testing.T) {
	a := mustRun(t, Config{Scenario: "chaos", Seed: 1})
	b := mustRun(t, Config{Scenario: "chaos", Seed: 2})
	if a.Hash == b.Hash {
		t.Fatalf("seeds 1 and 2 have the same hash %x", a.Hash)
	}
}

// TestScenariosPass runs a few seeds of every scenario. A failure prints the
// command that reproduces it.
func TestScenariosPass(t *testing.T) {
	for _, sc := range ScenarioNames() {
		t.Run(sc, func(t *testing.T) {
			for seed := uint64(1); seed <= uint64(seedCount(5, 40)); seed++ {
				if res := mustRun(t, Config{Scenario: sc, Seed: seed}); !res.OK() {
					t.Fatal(Report(res))
				}
			}
		})
	}
}

// TestFaultsAreInjected checks that each scenario really does what its name
// says, so a scenario cannot go quiet by accident.
func TestFaultsAreInjected(t *testing.T) {
	want := map[string]func(Stats) bool{
		"log-faults":     func(s Stats) bool { return s.FailedCalls > 0 && s.LostReplies > 0 },
		"crash-restart":  func(s Stats) bool { return s.Crashes > 0 && s.Boots > 3 && s.Pauses > 0 },
		"leader-fencing": func(s Stats) bool { return s.Partitions > 0 && s.CutCalls > 0 },
	}
	for sc, ok := range want {
		var total Stats
		for seed := uint64(1); seed <= 10; seed++ {
			s := mustRun(t, Config{Scenario: sc, Seed: seed}).Stats
			total.FailedCalls += s.FailedCalls
			total.LostReplies += s.LostReplies
			total.Crashes += s.Crashes
			total.Boots += s.Boots
			total.Pauses += s.Pauses
			total.Partitions += s.Partitions
			total.CutCalls += s.CutCalls
		}
		if !ok(total) {
			t.Errorf("%s: faults not seen: %+v", sc, total)
		}
	}
}

// TestPlantedBugsAreFound is why we can trust the simulator: when one known bug
// is planted, some seed in a few hundred must fail. The bug is in the replica
// (writer-offsets) or in the fake log (the other two).
func TestPlantedBugsAreFound(t *testing.T) {
	scenario := map[mutant.Bug]string{
		mutant.Bug(BugWriterOffsets):   "basic",
		mutant.Bug(BugLogOverwrite):    "chaos",
		mutant.Bug(BugClaimAlwaysWins): "leader-fencing",
	}
	bugs := []mutant.Bug{
		mutant.Bug(BugWriterOffsets),
		mutant.Bug(BugLogOverwrite),
		mutant.Bug(BugClaimAlwaysWins),
	}
	mutant.Require(t, bugs, 300, func(bug mutant.Bug, seed uint64) bool {
		res := mustRun(t, Config{Scenario: scenario[bug], Seed: seed, Bug: Bug(bug)})
		if res.OK() {
			return false
		}
		t.Logf("found at seed %d: %s: %s", seed, res.Failures[0].Property, res.Failures[0].Detail)
		return true
	})
}

// TestCleanRunHasNoFalseAlarm is the other side of the planted bug test: with no
// bug, the same seeds that find a bug must pass.
func TestCleanRunHasNoFalseAlarm(t *testing.T) {
	mutant.RequireClean(t, seedCount(10, 60), func(seed uint64) bool {
		for _, sc := range []string{"chaos", "leader-fencing"} {
			if res := mustRun(t, Config{Scenario: sc, Seed: seed}); !res.OK() {
				t.Log(Report(res))
				return true
			}
		}
		return false
	})
}

func TestUnknownScenario(t *testing.T) {
	if _, err := Run(Config{Scenario: "nope"}); err == nil {
		t.Fatal("want an error for an unknown scenario")
	}
}

func TestNodeCountsOtherThanThree(t *testing.T) {
	for _, n := range []int{1, 2, 5} {
		if res := mustRun(t, Config{Scenario: "crash-restart", Seed: 3, Nodes: n}); !res.OK() {
			t.Fatalf("%d nodes: %s", n, Report(res))
		}
	}
}
