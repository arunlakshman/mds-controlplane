# spec: the reference model of the control plane

`spec.ControlPlane` is a small program that gives the correct answer to every
control plane operation (topics, segments, consumer groups). It keeps plain maps
and slices, runs one operation at a time, and has no faults. It is about 1% of
the code it checks. The idea is from the paper "Using Lightweight Formal Methods
to Validate a Key-Value Storage Node in Amazon S3" (Bornholt et al., SOSP 2021).

The model is deterministic: no clock, network, goroutines or files, and no map
order reaches an answer. It is in the determinism gate (`make determinism`). Only
tests and the simulator import it. `go list -deps ./cmd/mdscontrolplane` does
not list it.

```go
m := spec.New()
done := m.Apply(contract.CreateTopic{Name: "t", Partitions: 2}) // spec.Model
```

The rules are in the comments of `spec.go` and `groups.go`. Small examples of
each rule are in `spec_test.go`.

## The tests that use the model

All of them are in `internal/controlplane/controlplanetest` and use
[rapid](https://pkg.go.dev/pgregory.net/rapid) (a test-only dependency).

| Test | What it checks |
|---|---|
| `TestConformMemorystore`, `TestConformMetadataMemory`, `TestConformMetadataPebble` | The in-memory store and the metadata store (on memory and on Pebble) answer like the model. |
| `TestConformModelTwice` | Two models given the same operations answer the same (determinism). |
| `TestConformReplica` | The real replica in one process (in-memory log and object store), with clean restarts and checkpoints in the middle. |
| `TestConformGRPC` | The real gRPC stack: client, server, replica. The wire API has only the topics operations. |
| `TestCrashReplica` | Crash test. `dirtyReboot` kills the replica and recovers it from the log and the checkpoints the crash left. Persistence: every write that was answered is there after a crash. Forward progress: the same after a clean restart. Run with and without log trim. |
| `TestFailureInjectionReplica` | The crash test plus injected storage failures, with the "has failed" flag. |
| `TestPlantedBug*` | Five known bugs. Each test checks that the conformance test FAILS. |

How a test works: rapid draws a list of actions. Each action sends one
operation to the model and to the system, and the answers must be equal. After
each answer some invariants are checked (runs of a partition have no gap,
topics are sorted). Background actions (`checkpoint`, `reboot`, `dirtyReboot`) do
nothing in the model, so a restart must not change any answer. After a restart
the whole state is compared with the model, and every earlier commit is sent
again: it must be a replay with the same ranges.

The actions are in a list from simple to complex (reads, commit, create topic,
group operations, background operations). The shrinker prefers the first ones, so
a failure is shrunk to simple actions. We use rapid's shrinker and nothing of our
own. Arguments come from small sets, and only a few choices lean on purpose. They
are listed at the top of `alphabet_test.go`, and the tests count the answers to
show that every rule is reached.

### Failure injection

`Inject` makes the next call to the storage fail: an append that stores nothing,
an append that stores and then fails (the answer is lost), a failed log read, a
failed checkpoint upload. After an injected failure the flag `hasFailed` is set.
Then an operation may answer with an error, and the test asks again until it
gets a real answer. The real answer must equal the model. An error is allowed, a
wrong answer never is. The paper also allows "no data". Our log does not lose
data, so we do not.

Because the test sends an operation again, this test uses only operations that
are safe to send again: no group install, assign or remove, and no create topic
with `FailIfExists`.

### What the paper has that we leave out

Concurrency and interleavings are not tested here. The simulator
(`internal/verify/simulator`, `make sim`) does that. The object store has no
model: its rules are a plain table in `internal/system/s3store/conform_test.go`.

## More checks, and replaying a failure

Rapid runs 100 checks by default. Some slow tests run fewer. A number that you
give with a flag is used for all of them.

```sh
make spec-long                                     # 2000 checks of each test
go test ./internal/controlplane/controlplanetest -run TestCrashReplica \
    -rapid.checks=500 -rapid.steps=40              # more checks, longer lists
```

When a check fails, rapid shrinks it and prints the shrunk list of actions with
the drawn values, and the seed and a fail file:

```
To reproduce, specify -run="TestCrashReplica/trim=true" -rapid.failfile="testdata/rapid/.../x.fail" (or -rapid.seed=123)
```

Run the same test with `-rapid.failfile=...` or `-rapid.seed=...` to see it again.
Rapid also replays the files it finds in `testdata/rapid/<test>/` at the start of
that test, so keep a fail file as a regression test if you want. Use
`-rapid.nofailfile` to stop rapid from writing the file. With `-rapid.v` you
see more detail. A test with a real clock and goroutines (the replica tests) can be
slightly different on each run, so a replay may need more than one try.
