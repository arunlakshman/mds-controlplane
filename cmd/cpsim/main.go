// Command cpsim runs the deterministic simulation of the replicated control plane.
//
//	go run ./cmd/cpsim -seeds 300                         # all scenarios, 300 seeds each
//	go run ./cmd/cpsim -scenario chaos -seed 17           # one seed, with a step trace
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/manifesterio/mds-controlplane/internal/verify/simulator"
	"github.com/manifesterio/mds-controlplane/internal/verify/simulator/campaign"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("cpsim", flag.ContinueOnError)
	fs.SetOutput(stderr)
	seeds := fs.Int("seeds", 100, "seeds to run for each scenario")
	first := fs.Uint64("first", 1, "first seed of a campaign")
	seed := fs.Uint64("seed", 0, "run this one seed with a full step trace (0: run a campaign)")
	scenario := fs.String("scenario", "all", "scenario name, or all: "+strings.Join(simulator.ScenarioNames(), ", "))
	nodes := fs.Int("nodes", 3, "number of replicas")
	steps := fs.Int("steps", simulator.DefaultSteps, "scheduler events in the fault phase of a run")
	bug := fs.String("bug", "", "plant a bug to see the simulator find it: writer-offsets, log-overwrite, claim-always-wins")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	base := simulator.Config{Nodes: *nodes, Steps: *steps, Bug: simulator.Bug(*bug)}
	if *seed != 0 {
		return runOne(base, *scenario, *seed, stdout, stderr)
	}
	names := simulator.ScenarioNames()
	if *scenario != "all" {
		names = []string{*scenario}
	}
	return runCampaign(base, names, *first, *seeds, stdout, stderr)
}

func runOne(base simulator.Config, scenario string, seed uint64, stdout, stderr io.Writer) int {
	if scenario == "all" {
		_, _ = fmt.Fprintln(stderr, "cpsim: -seed needs one -scenario")
		return 2
	}
	base.Scenario, base.Seed = scenario, seed
	base.Trace = func(line string) { _, _ = fmt.Fprintln(stdout, line) }
	res, err := simulator.Run(base)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "cpsim:", err)
		return 2
	}
	_, _ = fmt.Fprintln(stdout, simulator.Report(res))
	if !res.OK() {
		return 1
	}
	return 0
}

func runCampaign(base simulator.Config, names []string, first uint64, seeds int, stdout, stderr io.Writer) int {
	code := 0
	var sum campaign.Summary
	for _, name := range names {
		c := base
		c.Scenario = name
		out, err := campaign.RunCampaign(c, first, seeds)
		if err != nil {
			_, _ = fmt.Fprintln(stderr, "cpsim:", err)
			return 2
		}
		_, _ = fmt.Fprintln(stdout, out.Line())
		for _, f := range out.Failed {
			_, _ = fmt.Fprintln(stdout, simulator.Report(f))
			code = 1
		}
		sum.Add(out)
	}
	_, _ = fmt.Fprintln(stdout, sum.Coverage())
	if code != 0 {
		_, _ = fmt.Fprintln(stdout, "cpsim: FAILED")
	}
	return code
}
