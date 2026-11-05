package spec

import (
	"cmp"
	"slices"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Rebalance is the model of the classic consumer group protocol: one input at
// a time, no faults, no log. The rules are the table in docs/rebalance.md.
// Every input carries the time; deadlines up to that time fire first, earliest
// first (a tie: group name, then initial delay, round end, sessions by member
// id). The logged half of the state (installs, assignments, removes, offsets)
// is kept in Store, the metadata model, which also answers the reads.
type Rebalance struct {
	Store  *ControlPlane
	Delay  runtime.Duration // the initial rebalance delay of a new group
	now    runtime.NodeTime
	active bool
	groups map[string]*rgroup
}

// CallID names one call. Every call gets one Reply, unless it is abandoned.
type CallID uint64

// Protocol is one protocol a member can use, with its metadata.
type Protocol struct {
	Name     string
	Metadata []byte
}

// Join is a JoinGroup call. An empty MemberID asks for a new member, named
// ClientID + "-" + NewID (NewID alone when ClientID is empty). RebalanceMs 0
// means SessionMs.
type Join struct {
	Call                                                     CallID
	Group, MemberID, NewID, ClientID, ClientHost, InstanceID string
	ProtocolType                                             string
	Protocols                                                []Protocol
	SessionMs, RebalanceMs                                   int32
}

// Sync is a SyncGroup call. Only the leader's Assignments are used.
type Sync struct {
	Call            CallID
	Group, MemberID string
	Generation      int32
	Assignments     []contract.GroupAssignment
}

// Heartbeat is a Heartbeat call.
type Heartbeat struct {
	Call            CallID
	Group, MemberID string
	Generation      int32
}

// Leave is a LeaveGroup call. The reply body has one code per member.
type Leave struct {
	Call    CallID
	Group   string
	Members []string
}

// StoreCall is an OffsetCommit, OffsetFetch, DescribeGroup or ListGroups call.
// Store answers it. An OffsetCommit also renews its member's session.
type StoreCall struct {
	Call CallID
	Op   runtime.ControlPlaneOperation
}

// Abandon says the caller of Call stopped waiting. It gets no reply.
type Abandon struct{ Call CallID }

// RoleChanged says whether this node serves groups from now on.
type RoleChanged struct{ Active bool }

// Reply answers one call. Body is a JoinResult (Join), the assignment (Sync),
// one code per member (Leave), the Store answer (StoreCall), or nil.
type Reply struct {
	Call CallID
	Err  runtime.ErrCode
	Body any
}

// JoinResult is the body of a Join reply. Only the leader gets Members, in id
// order, with the metadata of the chosen protocol.
type JoinResult struct {
	Generation                 int32
	Protocol, Leader, MemberID string
	Members                    []contract.GroupMember
}

func (in Join) callID() CallID      { return in.Call }
func (in Sync) callID() CallID      { return in.Call }
func (in Heartbeat) callID() CallID { return in.Call }
func (in Leave) callID() CallID     { return in.Call }
func (in StoreCall) callID() CallID { return in.Call }

// The phases of a group, with the Kafka names.
const (
	PhaseEmpty   = "Empty"
	PhaseJoining = "PreparingRebalance"
	PhaseSyncing = "CompletingRebalance"
	PhaseStable  = "Stable"
)

const never = runtime.NodeTime(1<<63 - 1)

// rgroup is one group in memory. end is the round end, or the sync deadline.
// delayEnd is the end of the initial delay, 0 for none.
type rgroup struct {
	phase                       string
	generation                  int32
	protoType, protocol, leader string
	members                     map[string]*rmember
	joined                      int // members that ever joined, for the join order
	start, end, delayEnd        runtime.NodeTime
}

// rmember is one member. join and sync are its waiting calls, 0 for none. A
// member with a waiting call does not expire.
type rmember struct {
	contract.GroupMember
	order      int
	protocols  []Protocol
	expires    runtime.NodeTime
	join, sync CallID
	assignment []byte
}

func joinOf(m *rmember) *CallID { return &m.join }
func syncOf(m *rmember) *CallID { return &m.sync }

// NewRebalance returns a model with an empty Store that does not serve yet.
func NewRebalance(delay runtime.Duration) *Rebalance {
	return &Rebalance{Store: New(), Delay: delay, groups: map[string]*rgroup{}}
}

// Phase is the phase of a group in memory, "" when there is none.
func (r *Rebalance) Phase(group string) string {
	if g := r.groups[group]; g != nil {
		return g.phase
	}
	return ""
}

// Step fires the deadlines up to now, then handles in (nil for none), and
// returns the replies in the order they were made.
func (r *Rebalance) Step(now runtime.NodeTime, in any) []Reply {
	var out []Reply
	for at, fire := r.nextDeadline(now); fire != nil; at, fire = r.nextDeadline(now) {
		r.now = at
		out = append(out, fire()...)
	}
	r.now = max(r.now, now)
	return append(out, r.handle(in)...)
}

func (r *Rebalance) handle(in any) []Reply {
	switch in := in.(type) {
	case RoleChanged:
		return r.roleChanged(in.Active)
	case Abandon:
		r.abandon(in.Call)
		return nil
	case interface{ callID() CallID }:
		j, isJoin := in.(Join)
		if isJoin && (j.Group == "" || j.ProtocolType == "" || len(j.Protocols) == 0 || j.SessionMs <= 0) {
			return []Reply{{Call: j.Call, Err: runtime.ErrInvalid}}
		}
		if !r.active {
			return []Reply{{Call: in.callID(), Err: runtime.ErrUnavailable}}
		}
	}
	switch in := in.(type) {
	case Join:
		return r.join(in)
	case Sync:
		return r.syncGroup(in)
	case Heartbeat:
		return []Reply{{Call: in.Call, Err: r.heartbeat(in.Group, in.MemberID, in.Generation)}}
	case Leave:
		return r.leave(in)
	case StoreCall:
		op := in.Op
		if c, ok := op.(contract.OffsetCommit); ok {
			r.touch(r.member(c.Group, c.Member))
			c.TimeMs = r.ms()
			op = c
		}
		d := r.Store.Apply(op)
		return []Reply{{Call: in.Call, Err: d.Err, Body: d.Resp}}
	}
	return nil
}

func (r *Rebalance) ms() int64 { return int64(r.now) / int64(runtime.Millisecond) }

func (r *Rebalance) member(group, id string) *rmember {
	if g := r.groups[group]; g != nil {
		return g.members[id]
	}
	return nil
}

// touch starts the session of m again (m may be nil).
func (r *Rebalance) touch(m *rmember) {
	if m != nil {
		m.expires = r.now + runtime.NodeTime(m.SessionTimeoutMs)*runtime.NodeTime(runtime.Millisecond)
	}
}

// answer gives the waiting call *call its reply, and starts the session again.
func (r *Rebalance) answer(m *rmember, call *CallID, reply Reply) Reply {
	reply.Call, *call = *call, 0
	r.touch(m)
	return reply
}

func (r *Rebalance) answerAll(g *rgroup, field func(*rmember) *CallID, reply Reply) []Reply {
	var out []Reply
	for _, id := range sortedKeys(g.members) {
		if m := g.members[id]; *field(m) != 0 {
			out = append(out, r.answer(m, field(m), reply))
		}
	}
	return out
}

// nextDeadline returns the earliest deadline at or before now and what it does.
func (r *Rebalance) nextDeadline(now runtime.NodeTime) (runtime.NodeTime, func() []Reply) {
	best, found := now, (func() []Reply)(nil)
	consider := func(at runtime.NodeTime, fire func() []Reply) {
		if at <= now && (found == nil || at < best) {
			best, found = at, fire
		}
	}
	for _, name := range sortedKeys(r.groups) {
		g := r.groups[name]
		if g.phase == PhaseJoining && g.delayEnd > 0 {
			consider(g.delayEnd, func() []Reply { g.delayEnd = 0; return r.complete(name, g, false) })
		}
		if g.phase == PhaseJoining {
			consider(g.end, func() []Reply { return r.complete(name, g, true) })
		}
		if g.phase == PhaseSyncing {
			consider(g.end, func() []Reply { return r.openRound(g, false) })
		}
		for _, id := range sortedKeys(g.members) {
			if m := g.members[id]; m.join == 0 && m.sync == 0 {
				consider(m.expires, func() []Reply { out, _ := r.remove(name, g, []string{id}, "expire"); return out })
			}
		}
	}
	return best, found
}

func (r *Rebalance) join(in Join) []Reply {
	g := r.groups[in.Group]
	if g == nil {
		g = &rgroup{phase: PhaseEmpty, members: map[string]*rmember{}}
		r.groups[in.Group] = g
	}
	if len(g.members) == 0 {
		g.protoType = in.ProtocolType
	} else if in.ProtocolType != g.protoType {
		return []Reply{{Call: in.Call, Err: runtime.ErrInconsistentGroupProtocol}}
	}
	id, m := in.MemberID, g.members[in.MemberID]
	if id != "" && m == nil {
		return []Reply{{Call: in.Call, Err: runtime.ErrUnknownMember}}
	}
	isNew := m == nil
	if isNew {
		if id = in.NewID; in.ClientID != "" {
			id = in.ClientID + "-" + in.NewID
		}
		g.joined++
		m = &rmember{order: g.joined}
		g.members[id] = m
	}
	m.GroupMember = contract.GroupMember{MemberID: id, ClientID: in.ClientID, ClientHost: in.ClientHost,
		GroupInstanceID: in.InstanceID, SessionTimeoutMs: in.SessionMs, RebalanceTimeoutMs: in.RebalanceMs}
	if in.RebalanceMs <= 0 {
		m.RebalanceTimeoutMs = in.SessionMs
	}
	m.protocols = slices.Clone(in.Protocols)
	var out []Reply
	if m.join != 0 { // a newer join of the same member replaces the older call
		out = append(out, r.answer(m, &m.join, Reply{Err: runtime.ErrRebalanceInProgress}))
	}
	m.join = in.Call
	if g.phase != PhaseJoining {
		out = append(out, r.openRound(g, g.phase == PhaseEmpty)...)
	} else {
		if isNew && g.delayEnd > 0 {
			g.delayEnd = min(r.now+runtime.NodeTime(r.Delay), g.end)
		}
		g.end = max(g.end, g.start+longestRebalance(g))
	}
	return append(out, r.complete(in.Group, g, false)...)
}

func longestRebalance(g *rgroup) runtime.NodeTime {
	var ms int32
	for _, id := range sortedKeys(g.members) {
		ms = max(ms, g.members[id].RebalanceTimeoutMs)
	}
	return runtime.NodeTime(ms) * runtime.NodeTime(runtime.Millisecond)
}

// openRound starts a join round. Waiting syncs get ErrRebalanceInProgress.
func (r *Rebalance) openRound(g *rgroup, initial bool) []Reply {
	g.phase, g.start, g.end, g.delayEnd = PhaseJoining, r.now, r.now+longestRebalance(g), 0
	if initial && r.Delay > 0 {
		g.delayEnd = min(r.now+runtime.NodeTime(r.Delay), g.end)
	}
	return r.answerAll(g, syncOf, Reply{Err: runtime.ErrRebalanceInProgress})
}

// complete ends a join round when every member waits in a join and no initial
// delay is left, or at the round end (timeout) with the members that wait.
func (r *Rebalance) complete(name string, g *rgroup, timeout bool) []Reply {
	var late []string
	for _, id := range sortedKeys(g.members) {
		if g.members[id].join == 0 {
			late = append(late, id)
		}
	}
	if g.phase != PhaseJoining || (!timeout && (g.delayEnd > 0 || len(late) > 0)) {
		return nil
	}
	if timeout {
		g.end = never // a round that fails now waits for its members to join again
	}
	var out []Reply
	if len(late) > 0 { // members that did not join again leave the group
		r.Store.Apply(contract.GroupRemove{Group: name, Members: late, TimeMs: r.ms(), Reason: "expire"})
		if out = drop(g, late); len(g.members) == 0 {
			return out
		}
	}
	leader := g.members[g.leader]
	for _, id := range sortedKeys(g.members) {
		if m := g.members[id]; g.members[g.leader] == nil && (leader == nil || m.order < leader.order) {
			leader = m
		}
	}
	install := contract.GroupInstall{Group: name, ProtocolType: g.protoType, Generation: g.generation + 1, Leader: leader.MemberID, TimeMs: r.ms()}
	for _, p := range leader.protocols {
		if install.Members = metadataFor(g, p.Name); install.Members != nil {
			install.Protocol = p.Name
			break
		}
	}
	code := runtime.ErrInconsistentGroupProtocol
	if install.Members != nil {
		code = r.Store.Apply(install).Err
	}
	if code != runtime.OK {
		return append(out, r.answerAll(g, joinOf, Reply{Err: code})...)
	}
	g.phase, g.generation, g.protocol, g.leader = PhaseSyncing, install.Generation, install.Protocol, leader.MemberID
	g.start, g.end = r.now, r.now+longestRebalance(g)
	for _, gm := range install.Members {
		m := g.members[gm.MemberID]
		m.assignment = nil
		result := JoinResult{Generation: g.generation, Protocol: g.protocol, Leader: g.leader, MemberID: gm.MemberID}
		if gm.MemberID == g.leader {
			result.Members = install.Members
		}
		out = append(out, r.answer(m, &m.join, Reply{Body: result}))
	}
	return out
}

// metadataFor lists the members, in id order, with their metadata for
// protocol; nil when a member does not have it.
func metadataFor(g *rgroup, protocol string) []contract.GroupMember {
	var list []contract.GroupMember
	for _, id := range sortedKeys(g.members) {
		m := g.members[id]
		i := slices.IndexFunc(m.protocols, func(p Protocol) bool { return p.Name == protocol })
		if i < 0 {
			return nil
		}
		gm := m.GroupMember
		gm.Metadata = nonEmpty(m.protocols[i].Metadata)
		list = append(list, gm)
	}
	return list
}

// drop removes members from memory. Their waiting calls get ErrUnknownMember.
// With no member left the group is Empty; it keeps its generation.
func drop(g *rgroup, ids []string) []Reply {
	var out []Reply
	for _, id := range ids {
		for _, call := range []CallID{g.members[id].join, g.members[id].sync} {
			if call != 0 {
				out = append(out, Reply{Call: call, Err: runtime.ErrUnknownMember})
			}
		}
		delete(g.members, id)
	}
	if len(g.members) == 0 {
		g.phase, g.leader, g.delayEnd = PhaseEmpty, "", 0
	}
	return out
}

// remove logs that members left (a leave or an expiry) and drops them. Then an
// open round may end, or a Stable or CompletingRebalance group opens a round.
func (r *Rebalance) remove(name string, g *rgroup, ids []string, reason string) ([]Reply, runtime.ErrCode) {
	if len(ids) == 0 {
		return nil, runtime.OK
	}
	if code := r.Store.Apply(contract.GroupRemove{Group: name, Members: ids, TimeMs: r.ms(), Reason: reason}).Err; code != runtime.OK {
		return nil, code
	}
	out := drop(g, ids)
	switch g.phase {
	case PhaseJoining:
		out = append(out, r.complete(name, g, false)...)
	case PhaseSyncing, PhaseStable:
		out = append(out, r.openRound(g, false)...)
	}
	return out, runtime.OK
}

func (r *Rebalance) leave(in Leave) []Reply {
	codes := make([]runtime.ErrCode, len(in.Members))
	var gone []string
	for i, id := range in.Members {
		if r.member(in.Group, id) == nil {
			codes[i] = runtime.ErrUnknownMember
		} else if !slices.Contains(gone, id) {
			gone = append(gone, id)
		}
	}
	out, code := r.remove(in.Group, r.groups[in.Group], gone, "leave")
	for i := range codes {
		if codes[i] == runtime.OK {
			codes[i] = code
		}
	}
	return append(out, Reply{Call: in.Call, Body: codes})
}

// heartbeat checks a member and its generation, and starts its session again.
func (r *Rebalance) heartbeat(group, id string, generation int32) runtime.ErrCode {
	m := r.member(group, id)
	switch {
	case m == nil:
		return runtime.ErrUnknownMember
	case generation != r.groups[group].generation:
		return runtime.ErrIllegalGeneration
	}
	r.touch(m)
	if r.groups[group].phase == PhaseStable {
		return runtime.OK
	}
	return runtime.ErrRebalanceInProgress
}

// syncGroup answers at once, parks a follower, or stores the leader's
// assignment (Store checks it) and answers every waiting follower.
func (r *Rebalance) syncGroup(in Sync) []Reply {
	code := r.heartbeat(in.Group, in.MemberID, in.Generation)
	g, m := r.groups[in.Group], r.member(in.Group, in.MemberID)
	switch {
	case code == runtime.OK:
		return []Reply{{Call: in.Call, Body: m.assignment}}
	case code != runtime.ErrRebalanceInProgress || g.phase != PhaseSyncing:
		return []Reply{{Call: in.Call, Err: code}}
	case in.MemberID != g.leader:
		var out []Reply
		if m.sync != 0 {
			out = append(out, r.answer(m, &m.sync, Reply{Err: runtime.ErrRebalanceInProgress}))
		}
		m.sync = in.Call
		return out
	}
	assign := contract.GroupAssign{Group: in.Group, Generation: g.generation, Leader: g.leader, TimeMs: r.ms(), Assignments: in.Assignments}
	if code := r.Store.Apply(assign).Err; code != runtime.OK {
		return []Reply{{Call: in.Call, Err: code}}
	}
	g.phase = PhaseStable
	var out []Reply
	for _, id := range sortedKeys(g.members) {
		mm := g.members[id]
		mm.assignment = nil
		if i := slices.IndexFunc(in.Assignments, func(a contract.GroupAssignment) bool { return a.Member == id }); i >= 0 {
			mm.assignment = nonEmpty(in.Assignments[i].Bytes)
		}
		r.touch(mm)
		if mm.sync != 0 {
			out = append(out, r.answer(mm, &mm.sync, Reply{Body: mm.assignment}))
		}
	}
	return append(out, Reply{Call: in.Call, Body: m.assignment})
}

// abandon forgets a waiting call. Its member waits no more, and its session
// starts again.
func (r *Rebalance) abandon(call CallID) {
	for _, name := range sortedKeys(r.groups) {
		for _, id := range sortedKeys(r.groups[name].members) {
			for _, field := range []func(*rmember) *CallID{joinOf, syncOf} {
				if m := r.groups[name].members[id]; call != 0 && *field(m) == call {
					r.answer(m, field(m), Reply{})
				}
			}
		}
	}
}

// roleChanged starts or stops serving. Stopping answers every waiting call
// with ErrUnavailable and drops the memory. Starting reads every group from
// Store: a group is Stable when its assignment is for its generation and its
// leader is a member, Empty with no member, else in a round. Each session
// starts now; the join order is the join time, then the id.
func (r *Rebalance) roleChanged(active bool) []Reply {
	if active == r.active {
		return nil
	}
	r.active = active
	var out []Reply
	for _, name := range sortedKeys(r.groups) {
		out = append(out, r.answerAll(r.groups[name], joinOf, Reply{Err: runtime.ErrUnavailable})...)
		out = append(out, r.answerAll(r.groups[name], syncOf, Reply{Err: runtime.ErrUnavailable})...)
	}
	r.groups = map[string]*rgroup{}
	if !active {
		return out
	}
	listed, _ := r.Store.Apply(contract.ListGroups{}).Resp.(contract.ListGroupsResp)
	for _, s := range listed.Groups {
		d, _ := r.Store.Apply(contract.DescribeGroup{Group: s.Name}).Resp.(contract.DescribeGroupResp)
		g := &rgroup{phase: PhaseEmpty, generation: d.Generation, protoType: d.ProtocolType, protocol: d.Protocol, leader: d.Leader, members: map[string]*rmember{}}
		slices.SortStableFunc(d.Members, func(a, b contract.GroupMemberInfo) int { return cmp.Compare(a.JoinedTimeMs, b.JoinedTimeMs) })
		for i, info := range d.Members {
			g.members[info.MemberID] = &rmember{GroupMember: info.GroupMember, order: i + 1, assignment: info.Assignment}
			r.touch(g.members[info.MemberID])
		}
		g.joined = len(d.Members)
		r.groups[s.Name] = g
		switch {
		case len(g.members) > 0 && d.AssignedGeneration == d.Generation && g.members[d.Leader] != nil:
			g.phase = PhaseStable
		case len(g.members) > 0:
			r.openRound(g, false)
		}
	}
	return out
}
