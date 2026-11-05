# Deterministic simulation of the control plane

`cpsim` runs the replicated control plane (3 replicas by default) inside one
goroutine, on a virtual clock, with a fake shared log and a fake object store.
It sends random client work, injects faults, and checks the answers against the
control-plane spec. A run is made from one number, the seed. The same seed gives
the same run, bit for bit. So a failure found with seed 4711 can be run again
and again until it is understood.

## Run it

    make sim                                   # the tests of the simulator
    make sim-campaign                          # 300 seeds for each scenario, about 10 s
    make sim-campaign SIM_SEEDS=5000           # a long campaign
    go run ./cmd/cpsim -scenario chaos -seed 17          # one seed, with a trace of every step
    go run ./cmd/cpsim -scenario chaos -seeds 1000 -first 5000

Flags: `-seeds N` (seeds per scenario), `-first S` (first seed of a campaign),
`-seed S` (one seed, verbose), `-scenario NAME|all`, `-nodes N`, `-steps N`
(events in the fault phase, default 4000), `-bug NAME` (plant a bug, see below), and the report flags `-report`, `-json`,
`-until-stable`, `-stable-after`, `-max-seeds` (next section).

## The report

The lines that `make build` prints do not change. Three more flags add a report
after them:

    go run ./cmd/cpsim -seeds 2000 -report                  # report after the campaign
    go run ./cmd/cpsim -until-stable                        # run until no new assertion shows up
    go run ./cmd/cpsim -until-stable -stable-after 100 -max-seeds 2000
    go run ./cmd/cpsim -seeds 300 -json                     # only the report, as JSON

- `-report` prints the report after the normal lines.
- `-until-stable` runs seeds in steps of 50 numbers (all scenarios each time)
  until `-stable-after` seed numbers in a row (default 200) brought no new
  assertion. `-max-seeds` (default 5000) is the limit for each scenario. If the
  limit comes first, the report ends with a line that starts with `WARNING`:
  the campaign is not known to be stable. It replaces `-seeds`. It implies
  `-report`.
- `-json` prints the report as JSON and nothing else (no scenario lines). It
  implies `-report`. The exit code is still 1 when a seed fails.

What the report holds:

1. Counts: seed numbers run, runs (seeds times scenarios), passed, failed.
2. Failed runs, first, each with the command that replays it.
3. Every assertion by name: kind (`always`, `sometimes` or `reachable`), how
   many times it was checked and how many times it held.
4. Never reached: `sometimes` assertions that never held, and the paths in
   `expectedReachable` (`cmd/cpsim/campaign.go`) that no seed reached.
5. The coverage curve: for each seed number that saw an assertion for the first
   time, how many different assertions had been seen by then. The last line
   says at which seed the curve stopped growing. A curve that is still growing
   at the end means the campaign is too short.

An assertion that is never evaluated leaves no trace in the counts. That is why
the hand-kept `expectedReachable` list stays: only a list can name a path that
no seed reached. When a path is added or removed in the replica, change the list.

## What a run does

1. Boot: the replicas are built with `replica.New(config)`, the constructor that
   `cmd/mdscontrolplane` uses. The config has the same shape as `machineConfig`
   there, with times scaled down (catch-up every 50 ms, heartbeat 250 ms,
   takeover after 1.5 s, checkpoint every 1 s, log trim 8 slots behind it) so a
   run is short in virtual time.
2. Fault phase: 3 clients send random operations (create topic, commit segment,
   find segments, describe topics, and the group operations: install, assign,
   remove, offset commit, offset fetch, describe group, list groups). Each
   operation goes to a random node. On `ErrUnavailable`, a timeout or an IO
   error the client retries the same operation with a new request id, up to 8
   times, then gives up. Faults of the scenario are on.
3. Quiet tail: all faults stop, partitions are healed, paused nodes resume, dead
   nodes boot. The run waits until every node is ready and has applied every
   slot that changes state (at least 3 s, at most 40 s of virtual time).
4. Final reads: one client per node reads all the state from that node.
5. Judgement: the oracle checks everything (next section) and the run returns
   its result with the trace hash.

Time, order and chance: the scheduler keeps a queue ordered by (virtual time,
random tie, sequence number). Events at the same virtual time run in an order
that the seed decides, so races between a timer, a reply and a client request
are explored. Every random choice (latency, fault, workload, node choice) draws
from one stream (`Rand`, SplitMix64). The replicas draw their ids from the same
stream through `runtime.Environment`.

## What is simulated

| Real thing | In the simulator |
|---|---|
| Run loop | `simNode`: stamps `At` and `Wall` on every event, runs the returned actions, handles `SetTimer`, `StopForRestart`. |
| Log (`executor.Log`) | The real `executor.MemoryLog` behind a fault layer. Calls take effect at one instant; the reply comes later. |
| Object store | The real `executor.MemoryObjects`, with `List` sorted like S3 (the memory store returns unsorted keys). |
| Executor error mapping | `executor.Classify` for errors, and the same completions as `Executor.logIO`. |
| Timers | `SetTimer` and `CancelTimer` on the virtual clock, matched by token, like the run loop. |
| gRPC server | Not used. The client sends `runtime.ControlPlaneDeliver` and gets `ControlPlaneReply`, the simulator bridge path of the replica. |

Faults:

- Log and object calls: error reply with nothing applied, timeout with nothing
  applied, applied but the reply is lost (a timeout, `MaybeApplied`), slow
  replies, corrupted checkpoint reads (one byte changed).
- Nodes: crash at any step and boot later with a new boot number (all memory
  lost, the node restores from the log and the checkpoint), pause for a while,
  cut off from the log and the object store (every call times out), so another
  node can claim the epoch and the old writer is fenced when it comes back. The
  replica's own cooperative faults (`StallProb`, `CrashProb`) are on in
  `crash-restart` and `chaos`.

The log has no epoch check on append. Fencing in this design is a conditional
put (a slot can be written once) plus the writer epoch stored in every entry,
and `LogClaimEpoch` is a compare-and-swap. The fake log keeps those semantics
because it is the real in-memory log.

## What is checked (the oracle)

Every check is reported with `env.Always` (a broken one is a failure) and has a
property name.

| Property | How |
|---|---|
| acked write is in the log, with the spec answer | After the run the log history is replayed slot by slot into the spec model. Every acked commit must be in it and must have the answer the spec gives. Acked topic creates and group installs must be in the final spec state. |
| reads agree with the spec | Every read records how many slots the node had applied when it answered. The spec, replayed to that prefix of the log, must give the same answer. |
| acked write is visible, offsets and segment lists never go backwards | A read must show what clients had been told before the read was sent: end offsets, committed ranges, topics, group generation. Needs no order of the writes. |
| one writer per epoch | Every epoch has one claim winner, one writer name in the log entries, and at most one node that holds it. |
| replicas agree on every slot | After every step, a node that has applied N slots must have the same state hash as every other node (and the same node before a crash) at N slots. Checkpoint restores are covered, too. |
| live replicas converge | At the end of the quiet tail every node is ready and misses no slot that changes state, and answers the final reads. |
| log has no hole, log entries decode | The history of the log is contiguous. |
| assertions of the replica | The `Always` calls inside the replica (for example "a Put never wins a slot below the trim floor") are recorded as failures too. |

Because the log is the truth about the order of writes, an operation whose
answer was lost ("maybe applied") needs no special case: it is in the log or it
is not. The model (`internal/spec`) lives behind a one-method interface (`spec.Model`).
`spec.go` in the simulator is the only file that imports it.

Coverage: a campaign prints which replica paths no seed reached (claims,
take-over, fencing, restore from checkpoint, trimmed log, lost slot, bad
checkpoint CRC, replica crash and stall). It is a hint, not a failure.

## Trace hash

After every step the run folds the virtual time, the node, the replica's
`StateHash()` and the number of actions into one hash. The hash is in the
report and the campaign line. `TestSameSeedSameHash` runs each seed twice (and
once on 8 goroutines) and requires the same hash. Running `make sim-campaign`
twice prints the same hash for each scenario; this also holds across processes,
which have different map iteration orders. The engine is in the determinism
gate (`ops/scripts/ci-determinism.sh`): no goroutines, no map ranges, no
imports outside the allow-list (plus `context`). The campaign runner
(`cmd/cpsim`, on `dst/campaign`) uses goroutines to run seeds in parallel
and the wall clock only to print the duration; it is not in the gate.

## Read a failure

    FAIL seed=7 scenario=chaos step=2123 t=19133ms: sim: replicas agree on every slot: detail=after 22 slots, cp-1 has state b2243c2c93cc1170 but cp-0 had 515376c0e35c0fae (and 3 more)
    last steps:
      step 2119 t=19130ms cp-1 <- runtime.LogReadDone{...} (2 actions)
      ...
    reproduce: go run ./cmd/cpsim -scenario chaos -seed 7

- `seed` and `scenario` are all you need to run it again. The command on the last
  line does it, with a line for every step on the output.
- `step` is the scheduler event count and `t` the virtual time when the
  property first broke. `(and N more)` counts later hits of the same property.
- `last steps` are the last 48 steps before the end of the run: event, node,
  and how many actions the node returned.
- The first failure is usually the cause. Later ones (for example a read that
  disagrees with the spec) often follow from it.

## Planted bugs

A simulator that cannot find a planted bug cannot be trusted. `-bug` plants one:

- `writer-offsets`: the replica's own mutant, the writer chooses offsets (found
  at once, replicas disagree).
- `log-overwrite`: the log says "stored" for a put into a taken slot.
- `claim-always-wins`: a claim of the writer epoch wins when it should lose.

`TestPlantedBugsAreFound` requires that each is found within 300 seeds, and
`TestCleanRunHasNoFalseAlarm` that the same seeds pass without the bug.

## Add a scenario

1. Add a `Scenario{Name, Detail, Faults}` in `allScenarios` (`scenario.go`).
   `Faults` has per-call probabilities and the mean gap of each node fault.
2. If you need a new kind of fault, add a field to `Faults`, act on it in
   `services.go` (calls) or `scenario.go` (nodes), and add a counter to `Stats`.
3. Add its name to `TestFaultsAreInjected` so it cannot go silent.
4. Run `go run ./cmd/cpsim -scenario NAME -seeds 500`.

To add a client operation, add it to `workload.next`. To add a check, record
what is needed in `oracle.record` and judge it in `oracle.verify` (after the
run) or `oracle.checkRead` (during the run).

## Not covered yet

- The group coordinator (join, sync and heartbeat with timers and channels) and
  the gRPC server are not run. Only the replicated group storage operations are.
- Reads go through the simulator bridge path (`ControlPlaneDeliver`: wait for a
  fresh tail), not through the server path that reads the fold directly.
- The idempotency window is off (`WindowSlots` 0), so forgetting old commit
  keys is not tested. Clients retry quickly, so it would not be hit anyway.
- Committed group offsets may legitimately go down (a late retry of an old
  commit), so only the spec comparison checks them, not a monotone rule.
- No corruption of log entries, no clock skew between nodes, no loss of
  acknowledged log writes, no network faults between clients and nodes.
- The log is the in-memory log. The DynamoDB and PostgreSQL logs are checked by
  their own conformance tests (`executor/logtest`), not here.
- Liveness is checked only at the end (the cluster converges when faults stop).
  There is no check of how long a take-over may take.
