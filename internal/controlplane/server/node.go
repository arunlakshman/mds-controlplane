package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/manifesterio/dst/sched"
	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/controlplane/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/groups"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/membership"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
	"github.com/manifesterio/mds-controlplane/internal/system/live"
	"github.com/manifesterio/mds-controlplane/internal/system/realclock"
)

// Config wires a Node.
type Config struct {
	Machine replica.Config
	Self    runtime.NodeInstance
	Env     runtime.Environment
	Clock   runtime.Clock
	Log     executor.Log
	Objects executor.ObjectStore
	// StopGrace is how long the machine gets after Shutdown to exit.
	StopGrace runtime.Duration
	// BrokerLeaseTTL is how long a broker stays live after its last heartbeat
	// (default 5s). Brokers send one every interval, so keep it 3x the interval.
	BrokerLeaseTTL time.Duration
	// GroupInitialRebalanceDelay is how long a new consumer group waits for
	// more members before its first generation (default 3s).
	GroupInitialRebalanceDelay time.Duration
}

// Node owns the machine run loop and serves ControlPlane RPCs.
type Node struct {
	cpv1.UnimplementedControlPlaneServer

	config Config
	inbox  *live.Inbox
	inject chan live.InjectFunc
	m      atomic.Pointer[replica.Replica]
	health *health.Server
	leases *membership.Leases  // run loop only; empty until brokers heartbeat
	groups *groups.Coordinator // serves group calls while this node is active

	mu        sync.Mutex
	waiters   map[runtime.RequestID]chan runtime.ControlPlaneDone
	nextReq   runtime.RequestID
	shutdown  chan struct{}
	started   bool          // Start launched the loop (guarded by mu)
	loopDone  chan struct{} // closed when the loop has ended
	loopErr   error         // the result of the loop; set before loopDone is closed
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeErr  error

	// epoch and stepDowns are copies of the machine's write role. The run loop
	// stores them after every step, so a metrics scrape never waits on the loop.
	epoch     atomic.Uint64
	stepDowns atomic.Uint64
}

// New builds a Node. Call Start then serve gRPC with Register.
func New(config Config) *Node {
	if config.Self.Node == "" {
		config.Self = runtime.NodeInstance{Node: "cp"}
	}
	if config.Clock == nil {
		config.Clock = realclock.New()
	}
	if config.Env == nil {
		config.Env = &productionEnvironment{}
	}
	if config.Log == nil {
		config.Log = executor.NewMemoryLog()
	}
	if config.StopGrace <= 0 {
		config.StopGrace = runtime.Duration(15e9) // 15s
	}
	if config.BrokerLeaseTTL <= 0 {
		config.BrokerLeaseTTL = membership.DefaultTTL
	}
	n := &Node{
		config:   config,
		inbox:    live.NewInbox(),
		inject:   make(chan live.InjectFunc, 64),
		health:   health.NewServer(),
		leases:   membership.New(config.BrokerLeaseTTL),
		waiters:  map[runtime.RequestID]chan runtime.ControlPlaneDone{},
		shutdown: make(chan struct{}),
		loopDone: make(chan struct{}),
	}
	n.groups = groups.New(groups.Config{
		InitialRebalanceDelay: config.GroupInitialRebalanceDelay,
		Clock:                 config.Clock,
	}, nodeGroups{n: n})
	return n
}

// liveClock lets a runtime.Clock drive live.
type liveClock struct{ runtime.Clock }

func (c liveClock) Mono() sched.TimeNs { return sched.TimeNs(c.Clock.Mono()) }
func (c liveClock) Wall() sched.TimeNs { return sched.TimeNs(c.Clock.Wall()) }
func (c liveClock) AfterFunc(d sched.TimeNs, f func()) live.Timer {
	return c.Clock.AfterFunc(runtime.Duration(d), f)
}

// Health returns the health server to register on the gRPC server.
func (n *Node) Health() *health.Server { return n.health }

// Start launches the run loop. Ready flips health to Serving.
// ctx bounds only the Ready wait; the loop runs until Close (or the replica returns Crash or Exit).
func (n *Node) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	n.cancel = cancel
	ex := executor.New(runCtx, executor.Config{}, n.config.Log, n.config.Objects, n.inbox)
	m := replica.New(n.config.Machine)(n.config.Env, n.config.Self)
	n.m.Store(m)
	cfg := live.Config{
		Process:   m,
		Boot:      runtime.NodeStarted{Self: n.config.Self},
		Stop:      func(deadline sched.TimeNs) any { return runtime.StopRequest{Deadline: runtime.NodeTime(deadline)} },
		StopGrace: time.Duration(n.config.StopGrace),
		Clock:     liveClock{n.config.Clock},
		Inbox:     n.inbox,
		Shutdown:  n.shutdown,
		Inject:    n.inject,
		Exec: func(out any) error {
			switch v := out.(type) {
			case runtime.IOCall:
				ex.Execute(v)
			case runtime.ControlPlaneReply:
				n.reply(v)
			default:
				return fmt.Errorf("controlplane/server: the replica returned %T, which nothing takes", out)
			}
			return nil
		},
		AfterStep: func() {
			n.epoch.Store(m.Epoch())
			n.stepDowns.Store(m.StepDowns())
		},
	}
	n.mu.Lock()
	n.started = true
	n.mu.Unlock()
	go n.watchGroups(runCtx)
	go func() {
		err := live.Run(runCtx, cfg)
		n.health.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		n.loopErr = err
		close(n.loopDone)
	}()
	return n.waitReady(ctx, cancel)
}

// LogRole returns the writer epoch this node holds (0 when it is a standby) and
// how many times it stepped down. It reads copies, so it never waits on the run loop.
func (n *Node) LogRole() (uint64, uint64) {
	return n.epoch.Load(), n.stepDowns.Load()
}

// waitReady waits until the machine is ready, then sets health to SERVING.
// It gives up when ctx ends, when the loop exits, or after two minutes.
// On a give-up it calls cancel and waits for the loop to stop.
func (n *Node) waitReady(ctx context.Context, cancel context.CancelFunc) error {
	fail := func(err error) error {
		cancel()
		<-n.loopDone
		if err != nil {
			return err
		}
		return n.loopErr
	}
	timedOut := make(chan struct{})
	deadline := n.config.Clock.AfterFunc(runtime.Duration(2*time.Minute), func() { close(timedOut) })
	defer deadline.Stop()
	for {
		done := make(chan struct{})
		var ready bool
		n.inject <- func() []any {
			defer close(done)
			m := n.m.Load()
			ready = m != nil && m.Ready()
			return nil
		}
		select {
		case <-done:
			if ready {
				n.health.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
				return nil
			}
		case <-ctx.Done():
			return fail(ctx.Err())
		case <-n.loopDone:
			if n.loopErr != nil {
				return n.loopErr
			}
			return errors.New("controlplane/server: run exited before Ready")
		case <-timedOut:
			return fail(errors.New("controlplane/server: timed out waiting for Ready"))
		}
	}
}

// Close asks the loop to stop and waits for it. Safe to call more than once,
// after a failed Start (the loop has ended then), and when Start was never called.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		select {
		case <-n.shutdown:
		default:
			close(n.shutdown)
		}
		n.mu.Lock()
		started := n.started
		n.mu.Unlock()
		if started {
			if n.cancel != nil {
				n.cancel()
			}
			<-n.loopDone
			n.closeErr = n.loopErr
		}
		n.groups.Close()
	})
	return n.closeErr
}

func (n *Node) allocReq() runtime.RequestID {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nextReq++
	return n.nextReq
}

func (n *Node) onLoop(ctx context.Context, fn func(m *replica.Replica) []any) error {
	done := make(chan struct{})
	var runErr error
	select {
	case n.inject <- func() []any {
		defer close(done)
		m := n.m.Load()
		if m == nil {
			runErr = errors.New("controlplane/server: machine not started")
			return nil
		}
		return fn(m)
	}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-done:
		return runErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// serverFrom is who the node is when it talks to its own replica: the reply to
// a request goes back to it, and the reply sink hands it to the waiting RPC.
var serverFrom = runtime.NodeInstance{Node: "server"}

// ask sends one request to the replica as an inbox value and waits for its
// reply. A request that ends with its context may have been applied if it is a
// command (maybeApplied), so its answer says MaybeApplied.
func (n *Node) ask(ctx context.Context, maybeApplied bool, body func(request runtime.RequestID) runtime.EventData) (runtime.ControlPlaneDone, error) {
	request := n.allocReq()
	ch := make(chan runtime.ControlPlaneDone, 1)
	n.mu.Lock()
	n.waiters[request] = ch
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.waiters, request)
		n.mu.Unlock()
	}()
	n.inbox.Deliver(body(request))
	select {
	case d := <-ch:
		return d, nil
	case <-ctx.Done():
		outcome := runtime.NotApplied
		if maybeApplied {
			outcome = runtime.MaybeApplied
		}
		return runtime.ControlPlaneDone{Req: request, Err: runtime.ErrTimeout, Outcome: outcome}, ctx.Err()
	case <-n.loopDone:
		return runtime.ControlPlaneDone{}, errors.New("controlplane/server: the run loop has ended")
	}
}

// reply is the reply sink of the run loop: it gives the answer of the replica
// to the RPC that waits for it.
func (n *Node) reply(r runtime.ControlPlaneReply) {
	n.mu.Lock()
	ch := n.waiters[r.Done.Req]
	delete(n.waiters, r.Done.Req)
	n.mu.Unlock()
	if ch != nil && r.To == serverFrom {
		ch <- r.Done
	}
}

// Register attaches the ControlPlane service and health to s.
func (n *Node) Register(s *grpc.Server) {
	cpv1.RegisterControlPlaneServer(s, n)
	healthpb.RegisterHealthServer(s, n.health)
}
