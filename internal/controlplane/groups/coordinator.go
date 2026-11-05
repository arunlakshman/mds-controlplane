// Package groups is the consumer group coordinator of the active
// control plane node. It speaks the classic group protocol only
// (decision Q9).
//
// The log holds decisions only. A join result, a leader's assignment, a
// member that left or expired, and an offset commit go through the Store
// as log entries. Everything else is memory only on the active node: the
// phase of each group, the parked JoinGroup and SyncGroup calls, heartbeats
// and session timers (decision Q6). A standby serves no group call.
//
// Phases of a group:
//
//   - Empty: no member.
//   - PreparingRebalance: members are joining. The round ends when every known
//     member has rejoined, or at the round deadline (the largest rebalance
//     timeout). A new group first waits InitialRebalanceDelay for more members.
//   - CompletingRebalance: the join result is out. The leader must send its
//     assignment. Followers park in SyncGroup until then.
//   - Stable: the leader's assignment is logged. Heartbeats are answered.
//
// Locks. The coordinator has one mutex. It guards the current term and the
// group map. Each group has its own mutex. The order is always the
// coordinator mutex first, then a group mutex, never the other way round.
// A log write runs while the group mutex is held, because the decision and
// the memory change must not be split. Every write has WriteTimeout. A write
// that fails in a way that may have changed the log breaks the term (see
// settle): the term stops serving, its parked calls get ErrUnavailable, and
// Rebuild reads the log again.
//
// Takeover. When this node becomes the active appender, Rebuild reads every
// group from the log. Each member gets a session timer from now. This is the
// grace window of decision Q5. A member that stays silent after it is expired
// by a logged GroupRemove.
//
// Time. Every logged command carries TimeMs from the coordinator clock. The
// fold never reads a clock (decision Q11).
package groups

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
	"github.com/manifesterio/mds-controlplane/internal/system/realclock"
)

// Config holds the coordinator settings. A zero field takes its default.
type Config struct {
	// InitialRebalanceDelay is how long a new group waits for more members
	// (default 3s). Each new member restarts the wait, up to the round deadline.
	InitialRebalanceDelay time.Duration
	// WriteTimeout bounds one log write (default 10s).
	WriteTimeout time.Duration
	// Clock is the time source (default: the real clock).
	Clock runtime.Clock
	// NewID returns a random id for a new member (default: 128 random bits in
	// uuid form).
	NewID func() string
}

const (
	defaultInitialRebalanceDelay = 3 * time.Second
	defaultWriteTimeout          = 10 * time.Second
	// retryAfter is how long a session expiry waits before it tries the log again.
	retryAfter = time.Second
)

// ErrClosed is returned by Rebuild after Close.
var ErrClosed = errors.New("groups: closed")

// JoinResp is the answer to a JoinGroup call. Err is the group error.
// The struct is written by hand because it carries Err, which the wire message
// does not. wire.JoinGroupResponseToCore fills the other fields (generated).
type JoinResp struct {
	Err        runtime.ErrCode
	Generation int32
	Protocol   string
	Leader     string
	MemberID   string
	Members    []Member
}

// SyncResp is the answer to a SyncGroup call. Err is the group error.
// The struct is written by hand because it carries Err, which the wire message
// does not. wire.SyncGroupResponseToCore fills the other fields (generated).
type SyncResp struct {
	Err        runtime.ErrCode
	Assignment []byte
}

// GroupDescription is one group of a DescribeGroups answer.
type GroupDescription struct {
	Name string
	contract.DescribeGroupResp
}

// Coordinator serves the group calls on the active node.
type Coordinator struct {
	config Config
	store  Store

	mu     sync.Mutex
	cur    *term // nil when standby or not rebuilt yet
	closed bool
}

// term is one stretch of serving as the active node. A new term starts at
// each Rebuild. Its memory is dropped at the end of the term.
type term struct {
	ctx    context.Context
	cancel context.CancelFunc
	broken atomic.Bool       // a log write failed; the memory may differ from the log
	groups map[string]*group // guarded by Coordinator.mu
}

// New returns a coordinator over store. It serves nothing until Rebuild.
func New(config Config, store Store) *Coordinator {
	if config.InitialRebalanceDelay <= 0 {
		config.InitialRebalanceDelay = defaultInitialRebalanceDelay
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = defaultWriteTimeout
	}
	if config.Clock == nil {
		config.Clock = realclock.New()
	}
	if config.NewID == nil {
		config.NewID = randomID
	}
	return &Coordinator{config: config, store: store}
}

// randomID returns a random id of 128 bits, as text.
func randomID() string { return rand.Text() }

// write runs one group write on the store and returns its business code.
func (c *Coordinator) write(ctx context.Context, operation runtime.ControlPlaneOperation) (runtime.ErrCode, error) {
	d, err := c.store.Exec(ctx, operation)
	return d.Err, err
}

// Serving reports whether group calls are answered now.
func (c *Coordinator) Serving() bool { return c.enter() != nil }

// Rebuild reads every group from the log and starts serving them. The node
// calls it when it becomes the active appender, and again after a broken term.
// Old memory is dropped first. A read error leaves the coordinator standby.
func (c *Coordinator) Rebuild(ctx context.Context) error {
	c.Stop()
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return ErrClosed
	}
	listed, err := c.store.Exec(ctx, contract.ListGroups{})
	if err != nil {
		return err
	}
	names := runtime.ResponseAs[contract.ListGroupsResp](listed)
	tctx, cancel := context.WithCancel(context.Background())
	t := &term{ctx: tctx, cancel: cancel, groups: map[string]*group{}}
	for _, s := range names.Groups {
		described, err := c.store.Exec(ctx, contract.DescribeGroup{Group: s.Name})
		if err != nil {
			cancel()
			return err
		}
		g := newGroup(s.Name)
		c.load(t, g, runtime.ResponseAs[contract.DescribeGroupResp](described))
		t.groups[s.Name] = g
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		cancel()
		return ErrClosed
	}
	c.cur = t
	return nil
}

// Stop drops the term. Timers stop and parked calls get ErrUnavailable. A
// standby calls it on every check, so it does nothing when no term is live.
func (c *Coordinator) Stop() {
	c.mu.Lock()
	t := c.cur
	c.cur = nil
	var groups []*group
	if t != nil {
		for _, g := range t.groups {
			groups = append(groups, g)
		}
	}
	c.mu.Unlock()
	if t == nil {
		return
	}
	t.cancel()
	for _, g := range groups {
		g.mu.Lock()
		g.stopTimers()
		g.mu.Unlock()
	}
}

// Close stops the coordinator for good. Rebuild fails after Close.
func (c *Coordinator) Close() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.Stop()
}

// enter returns the live term, or nil when this node does not serve groups.
func (c *Coordinator) enter() *term {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cur == nil || !c.cur.live() {
		return nil
	}
	return c.cur
}

// group returns the group called name. It is made when create is set.
func (c *Coordinator) group(t *term, name string, create bool) *group {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := t.groups[name]
	if g == nil && create {
		g = newGroup(name)
		t.groups[name] = g
	}
	return g
}

// lockLive takes the group mutex when the term still serves. It returns false
// with no lock held when the term ended.
func (t *term) lockLive(g *group) bool {
	g.mu.Lock()
	if !t.live() {
		g.mu.Unlock()
		return false
	}
	return true
}

func (t *term) live() bool { return t.ctx.Err() == nil && !t.broken.Load() }

// poison breaks the term after a log write failed in a way that may have
// changed the log. Its parked calls wake up now. The node rebuilds later.
func (t *term) poison() {
	t.broken.Store(true)
	t.cancel()
}

// settle turns the answer of a decision write into the code for the callers.
// A write that proves nothing was written keeps its code (ErrTooLarge and
// ErrInvalid). Any other failure breaks the term and gives ErrUnavailable.
func settle(t *term, code runtime.ErrCode, err error) runtime.ErrCode {
	switch {
	case err == nil && code == runtime.OK:
		return runtime.OK
	case err == nil && (code == runtime.ErrTooLarge || code == runtime.ErrInvalid):
		return code
	}
	t.poison()
	return runtime.ErrUnavailable
}

// writeCtx is the context of one log write. It ends with the term or after WriteTimeout.
func (c *Coordinator) writeCtx(t *term) (context.Context, context.CancelFunc) {
	return context.WithTimeout(t.ctx, c.config.WriteTimeout)
}

// nowMs is the coordinator clock in milliseconds, for TimeMs.
func (c *Coordinator) nowMs() int64 { return int64(c.config.Clock.Wall()) / int64(time.Millisecond) }

// newMemberID returns a member id: the client id, a dash, then a random id.
func (c *Coordinator) newMemberID(clientID string) string {
	if clientID == "" {
		return c.config.NewID()
	}
	return clientID + "-" + c.config.NewID()
}

// JoinGroup joins a group. It blocks until the join round ends, or until ctx
// ends (then the error is ctx.Err()).
func (c *Coordinator) JoinGroup(ctx context.Context, request JoinReq) (JoinResp, error) {
	if request.Group == "" || request.ProtocolType == "" || len(request.Protocols) == 0 || request.SessionTimeoutMs <= 0 {
		return JoinResp{Err: runtime.ErrInvalid}, nil
	}
	t := c.enter()
	if t == nil {
		return JoinResp{Err: runtime.ErrUnavailable}, nil
	}
	id := request.MemberID
	if id == "" {
		id = c.newMemberID(request.ClientID)
	}
	g := c.group(t, request.Group, true)
	if !t.lockLive(g) {
		return JoinResp{Err: runtime.ErrUnavailable}, nil
	}
	response, ch := c.joinLocked(t, g, request, id)
	g.mu.Unlock()
	if ch == nil {
		return response, nil
	}
	return waitReply(ctx, t, g, id, ch, JoinResp{Err: runtime.ErrUnavailable}, func(m *member) *chan JoinResp { return &m.joinWait })
}

// SyncGroup sends the leader's assignment, or waits for it as a follower. It
// blocks until the leader syncs, or until ctx ends (then the error is ctx.Err()).
func (c *Coordinator) SyncGroup(ctx context.Context, request SyncReq) (SyncResp, error) {
	t := c.enter()
	if t == nil {
		return SyncResp{Err: runtime.ErrUnavailable}, nil
	}
	g := c.group(t, request.Group, false)
	if g == nil {
		return SyncResp{Err: runtime.ErrUnknownMember}, nil
	}
	if !t.lockLive(g) {
		return SyncResp{Err: runtime.ErrUnavailable}, nil
	}
	m := g.members[request.MemberID]
	if m == nil {
		g.mu.Unlock()
		return SyncResp{Err: runtime.ErrUnknownMember}, nil
	}
	if request.Generation != g.generation {
		g.mu.Unlock()
		return SyncResp{Err: runtime.ErrIllegalGeneration}, nil
	}
	c.armSession(t, g, m)
	switch g.phase {
	case phaseStable:
		response := SyncResp{Assignment: copyBytes(m.assignment)}
		g.mu.Unlock()
		return response, nil
	case phaseCompleting:
		if m.id == g.leader {
			return c.leaderSync(t, g, m, request.Assignments), nil
		}
		ch := make(chan SyncResp, 1)
		if m.syncWait != nil {
			m.syncWait <- SyncResp{Err: runtime.ErrRebalanceInProgress}
		}
		m.syncWait = ch
		g.mu.Unlock()
		return waitReply(ctx, t, g, m.id, ch, SyncResp{Err: runtime.ErrUnavailable}, func(m *member) *chan SyncResp { return &m.syncWait })
	case phasePrep:
		g.mu.Unlock()
		return SyncResp{Err: runtime.ErrRebalanceInProgress}, nil
	}
	g.mu.Unlock()
	return SyncResp{Err: runtime.ErrUnknownMember}, nil
}

// Heartbeat renews a member's session. It is memory only.
func (c *Coordinator) Heartbeat(_ context.Context, request HeartbeatReq) runtime.ErrCode {
	t := c.enter()
	if t == nil {
		return runtime.ErrUnavailable
	}
	g := c.group(t, request.Group, false)
	if g == nil {
		return runtime.ErrUnknownMember
	}
	if !t.lockLive(g) {
		return runtime.ErrUnavailable
	}
	defer g.mu.Unlock()
	m := g.members[request.MemberID]
	if m == nil {
		return runtime.ErrUnknownMember
	}
	if request.Generation != g.generation {
		return runtime.ErrIllegalGeneration
	}
	c.armSession(t, g, m)
	if g.phase == phaseStable {
		return runtime.OK
	}
	return runtime.ErrRebalanceInProgress
}

// LeaveGroup removes members. It gives one code per member, in request order.
func (c *Coordinator) LeaveGroup(_ context.Context, request LeaveReq) []runtime.ErrCode {
	errs := make([]runtime.ErrCode, len(request.Members))
	t := c.enter()
	if t == nil {
		return fillCodes(errs, runtime.ErrUnavailable)
	}
	g := c.group(t, request.Group, false)
	if g == nil {
		return fillCodes(errs, runtime.ErrUnknownMember)
	}
	if !t.lockLive(g) {
		return fillCodes(errs, runtime.ErrUnavailable)
	}
	defer g.mu.Unlock()
	var gone []string
	seen := map[string]bool{}
	for i, id := range request.Members {
		if g.members[id] == nil {
			errs[i] = runtime.ErrUnknownMember
			continue
		}
		errs[i] = runtime.OK
		if !seen[id] {
			seen[id] = true
			gone = append(gone, id)
		}
	}
	if len(gone) == 0 {
		return errs
	}
	ctx, cancel := c.writeCtx(t)
	code, err := c.write(ctx, contract.GroupRemove{Group: g.name, Members: gone, TimeMs: c.nowMs(), Reason: "leave"})
	cancel()
	if code = settle(t, code, err); code != runtime.OK {
		for i := range errs {
			if errs[i] == runtime.OK {
				errs[i] = code
			}
		}
		return errs
	}
	for _, id := range gone {
		c.dropMember(g, id)
	}
	c.afterLeave(t, g)
	return errs
}

func fillCodes(errs []runtime.ErrCode, code runtime.ErrCode) []runtime.ErrCode {
	for i := range errs {
		errs[i] = code
	}
	return errs
}

// OffsetCommit stores committed offsets. A member's commit counts as its
// heartbeat. The log checks the generation and the member.
func (c *Coordinator) OffsetCommit(_ context.Context, operation contract.OffsetCommit) (contract.OffsetCommitResp, runtime.ErrCode) {
	t := c.enter()
	if t == nil {
		return contract.OffsetCommitResp{}, runtime.ErrUnavailable
	}
	if operation.Member != "" {
		if g := c.group(t, operation.Group, false); g != nil && t.lockLive(g) {
			if m := g.members[operation.Member]; m != nil {
				c.armSession(t, g, m)
			}
			g.mu.Unlock()
		}
	}
	operation.TimeMs = c.nowMs()
	ctx, cancel := c.writeCtx(t)
	defer cancel()
	d, err := c.store.Exec(ctx, operation)
	if err != nil {
		return contract.OffsetCommitResp{}, runtime.ErrUnavailable
	}
	if d.Err != runtime.OK {
		return contract.OffsetCommitResp{}, d.Err
	}
	return runtime.ResponseAs[contract.OffsetCommitResp](d), runtime.OK
}

// OffsetFetch reads committed offsets. It is not logged.
func (c *Coordinator) OffsetFetch(_ context.Context, operation contract.OffsetFetch) (contract.OffsetFetchResp, runtime.ErrCode) {
	t := c.enter()
	if t == nil {
		return contract.OffsetFetchResp{}, runtime.ErrUnavailable
	}
	d, err := c.store.Exec(t.ctx, operation)
	if err != nil {
		return contract.OffsetFetchResp{}, runtime.ErrUnavailable
	}
	return runtime.ResponseAs[contract.OffsetFetchResp](d), runtime.OK
}

// DescribeGroup reads one group. It is not logged.
func (c *Coordinator) DescribeGroup(_ context.Context, group string) (contract.DescribeGroupResp, runtime.ErrCode) {
	t := c.enter()
	if t == nil {
		return contract.DescribeGroupResp{}, runtime.ErrUnavailable
	}
	d, err := c.store.Exec(t.ctx, contract.DescribeGroup{Group: group})
	if err != nil {
		return contract.DescribeGroupResp{}, runtime.ErrUnavailable
	}
	return runtime.ResponseAs[contract.DescribeGroupResp](d), runtime.OK
}

// ListGroups lists every group in the log. It is not logged.
func (c *Coordinator) ListGroups(_ context.Context) ([]contract.GroupSummary, runtime.ErrCode) {
	t := c.enter()
	if t == nil {
		return nil, runtime.ErrUnavailable
	}
	d, err := c.store.Exec(t.ctx, contract.ListGroups{})
	if err != nil {
		return nil, runtime.ErrUnavailable
	}
	return runtime.ResponseAs[contract.ListGroupsResp](d).Groups, runtime.OK
}

// copyBytes returns a copy of b. An empty value is nil, as the log returns it.
func copyBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return bytes.Clone(b)
}
