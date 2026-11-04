package controlplanetest

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/server"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// The real replica in one process, on the in-memory log and object store.
// A "disk" is what survives a restart. A "process" is what one run of the node
// sees of the disk, and a crash kills it: from then on its storage calls fail
// and change nothing.

type disk struct {
	log  *executor.MemoryLog
	objs *executor.MemoryObjects
}

// Kinds of injected failure (the argument of Inject).
const (
	failAppend     = iota // an append fails and nothing is stored
	failAppendLate        // an append is stored, then it fails: the answer is lost
	failRead              // a log read or tail check fails
	failPut               // a checkpoint upload fails
)

var errInjected = errors.New("injected failure")

type process struct {
	disk  *disk
	dead  atomic.Bool
	mu    sync.Mutex
	armed map[int]int // failures still to inject, by kind
}

func (p *process) take(kind int) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.armed[kind] == 0 {
		return false
	}
	p.armed[kind]--
	return true
}

func (p *process) down() error {
	if p.dead.Load() {
		return errInjected
	}
	return nil
}

var (
	_ executor.Log         = (*process)(nil)
	_ executor.ObjectStore = (*process)(nil)
)

func (p *process) Append(ctx context.Context, slot runtime.LogPosition, data []byte) ([]byte, bool, error) {
	if err := p.down(); err != nil || p.take(failAppend) {
		return nil, false, errInjected
	}
	existing, taken, err := p.disk.log.Append(ctx, slot, data)
	if err == nil && p.take(failAppendLate) {
		return nil, false, errInjected
	}
	return existing, taken, err
}

func (p *process) Read(ctx context.Context, from runtime.LogPosition, limit int) ([]runtime.LogEntry, error) {
	if err := p.down(); err != nil || p.take(failRead) {
		return nil, errInjected
	}
	return p.disk.log.Read(ctx, from, limit)
}

func (p *process) CheckTail(ctx context.Context) (runtime.LogPosition, error) {
	if err := p.down(); err != nil || p.take(failRead) {
		return 0, errInjected
	}
	return p.disk.log.CheckTail(ctx)
}

func (p *process) TrimThrough(ctx context.Context, through runtime.LogPosition) error {
	if err := p.down(); err != nil {
		return err
	}
	return p.disk.log.TrimThrough(ctx, through)
}

func (p *process) ClaimEpoch(ctx context.Context, expect uint64, owner string) (uint64, string, bool, error) {
	if err := p.down(); err != nil {
		return 0, "", false, err
	}
	return p.disk.log.ClaimEpoch(ctx, expect, owner)
}

func (p *process) Put(ctx context.Context, key string, data []byte) error {
	if err := p.down(); err != nil || p.take(failPut) {
		return errInjected
	}
	return p.disk.objs.Put(ctx, key, data)
}

func (p *process) Get(ctx context.Context, ref runtime.SegmentRef) ([]byte, error) {
	if err := p.down(); err != nil {
		return nil, err
	}
	return p.disk.objs.Get(ctx, ref)
}

func (p *process) Delete(ctx context.Context, key string) error {
	if err := p.down(); err != nil {
		return err
	}
	return p.disk.objs.Delete(ctx, key)
}

func (p *process) List(ctx context.Context, prefix string) ([]string, error) {
	if err := p.down(); err != nil {
		return nil, err
	}
	return p.disk.objs.List(ctx, prefix)
}

// nodeSystem is a server.Node that can be restarted on the same disk. It is
// restartable and faulty (see conform_test.go).
type nodeSystem struct {
	t    *rapid.T
	cfg  replica.Config
	disk *disk
	proc *process
	node *server.Node

	injected bool // Inject was called at least once
}

const checkpointPrefix = "checkpoint/"

// newNodeSystem starts a node. With trim, the log is cut behind each checkpoint.
func newNodeSystem(rt *rapid.T, trim bool) *nodeSystem {
	cfg := replica.Config{
		CatchUpEvery: runtime.Duration(time.Millisecond), Rank: 0, Writer: "cp-0",
		Heartbeat:       runtime.Duration(time.Second),
		TakeoverAfter:   runtime.Duration(20 * time.Millisecond), // a restarted node claims the write role after this
		CheckpointEvery: runtime.Duration(3 * time.Millisecond), CheckpointMinSlots: 1, CheckpointPrefix: checkpointPrefix,
	}
	if trim {
		cfg.KeepSlots = 2
	}
	s := &nodeSystem{t: rt, cfg: cfg, disk: &disk{log: executor.NewMemoryLog(), objs: executor.NewMemoryObjects()}}
	s.start()
	rt.Cleanup(func() {
		if s.node != nil {
			_ = s.node.Close()
		}
	})
	return s
}

func (s *nodeSystem) start() {
	s.proc = &process{disk: s.disk, armed: map[int]int{}}
	s.node = server.New(server.Config{
		Machine: s.cfg, Log: s.proc, Objects: s.proc,
		Self: runtime.NodeInstance{Node: "cp-0"}, StopGrace: runtime.Duration(50 * time.Millisecond),
	})
	ctx, cancel := context.WithTimeout(s.t.Context(), 10*time.Second)
	defer cancel()
	if err := s.node.Start(ctx); err != nil {
		s.node = nil // Start already stopped it, and Close would wait for ever
		s.t.Fatalf("the node did not start: %v", err)
	}
}

// Exec asks again while the node says ErrUnavailable (it is not the writer
// yet, or not ready). That answer means "not accepted, try later".
func (s *nodeSystem) Exec(ctx context.Context, op runtime.ControlPlaneOperation) (runtime.ControlPlaneDone, error) {
	for {
		d, err := s.node.Exec(ctx, op)
		if err != nil || d.Err != runtime.ErrUnavailable || ctx.Err() != nil {
			return d, err
		}
		time.Sleep(time.Millisecond)
	}
}

// Reboot is a clean restart: the node stops, a new one starts on the same disk.
func (s *nodeSystem) Reboot() {
	_ = s.node.Close()
	s.start()
}

// DirtyReboot is a crash. The process dies first (its storage calls stop at
// once, so nothing it had in flight lands later), and a new one starts on the
// same disk. All it knew in memory is lost. keep chooses what the crash
// leaves of the checkpoints: 0 all, 1 all but the newest (its upload had not
// finished), 2 none. The log is never touched, because a write that was
// answered is in the log. The test only asks for keep > 0 when the log is not
// trimmed, because otherwise the log alone cannot rebuild the state.
func (s *nodeSystem) DirtyReboot(keep int) {
	s.proc.dead.Store(true)
	_ = s.node.Close()
	keys, _ := s.disk.objs.List(s.t.Context(), checkpointPrefix)
	slices.Sort(keys)
	if keep == 1 && len(keys) > 0 {
		keys = keys[len(keys)-1:]
	} else if keep == 0 {
		keys = nil
	}
	for _, k := range keys {
		_ = s.disk.objs.Delete(s.t.Context(), k)
	}
	s.start()
}

// Checkpoint waits (a little) until a checkpoint covers the whole log. It is a
// background operation: the answers must not depend on it.
func (s *nodeSystem) Checkpoint() {
	tail, _ := s.disk.log.CheckTail(s.t.Context())
	for end := time.Now().Add(500 * time.Millisecond); time.Now().Before(end); time.Sleep(2 * time.Millisecond) {
		keys, _ := s.disk.objs.List(s.t.Context(), checkpointPrefix)
		slices.Sort(keys)
		if len(keys) == 0 {
			continue
		}
		if slot, err := strconv.ParseInt(strings.TrimPrefix(keys[len(keys)-1], checkpointPrefix), 10, 64); err == nil && slot >= int64(tail) {
			return
		}
	}
}

// Inject arms one failure of a kind for the next matching call. -1 disarms all.
func (s *nodeSystem) Inject(kind int) {
	p := s.proc
	s.injected = s.injected || kind >= 0
	p.mu.Lock()
	defer p.mu.Unlock()
	if kind < 0 {
		clear(p.armed)
		return
	}
	p.armed[kind]++
}

// ---- the tests ----

// TestConformReplica runs the real replica in process, with clean restarts and
// checkpoints in the middle (they are background operations: no-ops in the model).
func TestConformReplica(t *testing.T) {
	setRapid(t, 30, 20)
	seen := check(t, options{groups: true, restart: true}, func(rt *rapid.T) runtime.OperationRunner { return newNodeSystem(rt, false) })
	requireReached(t, seen, append(topicRules, "action reboot", "action checkpoint", "sweep.find/OK", "sweep.replay/OK")...)
}

// TestCrashReplica is the crash test (paper, section 5). DirtyReboot kills the
// replica and recovers it from the log and the checkpoints that the crash left.
//
//	persistence       after a crash, every write that was answered OK is there,
//	                  and each commit is still a replay with the same ranges.
//	forward progress  after a clean restart, the same.
//
// The first variant does not trim the log, so a crash may also drop checkpoints
// and the state is rebuilt from the log. The second trims the log behind each
// checkpoint, so the state must come back from the checkpoint plus the rest.
func TestCrashReplica(t *testing.T) {
	setRapid(t, 30, 20)
	for _, trim := range []bool{false, true} {
		t.Run("trim="+strconv.FormatBool(trim), func(t *testing.T) {
			seen := check(t, options{groups: true, restart: true, dirty: true, trim: trim}, func(rt *rapid.T) runtime.OperationRunner { return newNodeSystem(rt, trim) })
			requireReached(t, seen, "action dirtyReboot", "action reboot", "sweep.replay/OK", "sweep.describeGroup/OK")
		})
	}
}

// TestFailureInjectionReplica is the crash test with injected storage failures
// (a failed append, an append whose answer is lost, a failed read, a failed
// checkpoint upload). The model gets the "has failed" flag: after an injected
// failure an operation may answer with an error, never with wrong data. It uses
// only operations that are safe to send again, because the test sends the
// operation again after an error.
func TestFailureInjectionReplica(t *testing.T) {
	setRapid(t, 30, 20)
	for _, trim := range []bool{false, true} {
		t.Run("trim="+strconv.FormatBool(trim), func(t *testing.T) {
			seen := check(t, options{retry: true, restart: true, dirty: true, faults: true, trim: trim}, func(rt *rapid.T) runtime.OperationRunner { return newNodeSystem(rt, trim) })
			requireReached(t, seen, "action injectFailure", "action dirtyReboot", "commit/OK")
		})
	}
}
