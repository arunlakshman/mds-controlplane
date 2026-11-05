# Rebalance: the group protocol as a pure machine

Status: design note for phase P6 (task T6.1). Review it before T6.2 starts.

## 1. Purpose and scope

`domain/rebalance` replaces the coordinator in `groups/`. It is a machine in the shape of
`sim.Process`: `Step(cx, input) -> outputs`. It has no locks, goroutines, channels or clock. It only
reads time and randomness from `cx`. The same machine runs in production (a second `live.Run`) and in
the simulator (a second dst process per node).

Scope: the classic group protocol only (decision Q9): join, sync, heartbeat, leave, offset commit and
fetch, describe, list. No static membership (the instance id is kept, not used), no cooperative
rebalance, no KIP-848. The log holds decisions only; phases, waiting calls and timers are memory only.

The independent model is `internal/spec/rebalance.go` (`spec.Rebalance`). Section 10 says how it
differs from the machine.

## 2. Inputs and outputs

`Call` is a call id (uint64) that the host gives each RPC, unique in one run of the process. `Req` is
a `runtime.RequestID` that the machine gives each submit, unique in one run of the process.

| Input | Fields |
|---|---|
| `Join` | Call, Group, MemberID ("" asks for a new member), GroupInstanceID, ClientID, ClientHost, SessionTimeoutMs, RebalanceTimeoutMs (0 means the session timeout), ProtocolType, Protocols (list of Name and Metadata, in the member's order of preference) |
| `Sync` | Call, Group, MemberID, Generation, Assignments (list of Member and Bytes; only the leader's are used) |
| `Heartbeat` | Call, Group, MemberID, Generation |
| `Leave` | Call, Group, Members (list of member ids) |
| `OffsetCommit` | Call, `contract.OffsetCommit` (Group, Member, Generation, Offsets). The machine sets TimeMs. |
| `OffsetFetch` | Call, `contract.OffsetFetch` (Group, Partitions) |
| `Describe` | Call, Groups (list of names) |
| `List` | Call |
| `CallAbandoned` | Call. The RPC context ended. The call gets no reply. |
| `TimerFired` | `runtime.TimerFired` (ID, Gen). A Gen that is not the current one for ID is ignored. |
| `RoleChanged` | Active: the replica is the active appender and Ready. |
| `CommandDone` | Req, Done (`runtime.ControlPlaneDone`: Err, Outcome, Resp) |
| `ReadDone` | Req, Done |

| Output | Fields |
|---|---|
| `SubmitCommand` | Req, Op: `GroupInstall`, `GroupAssign`, `GroupRemove` or `OffsetCommit` |
| `SubmitRead` | Req, Op: `ListGroups`, `DescribeGroup` or `OffsetFetch` |
| `Reply` | Call, Err, and the answer of the call: Join (Generation, Protocol, Leader, MemberID, Members: id, instance id, metadata; leader only), Sync (Assignment), Leave (one code per member), OffsetCommit (one code per offset), OffsetFetch, Describe (one description per name), List |
| `sim.SetTimer` | ID, After, Body: `runtime.TimerFired` |
| `sim.CancelTimer` | ID |

Expected codes in Reply.Err: OK, ErrInvalid, ErrUnavailable, ErrUnknownMember, ErrIllegalGeneration,
ErrRebalanceInProgress, ErrInconsistentGroupProtocol, ErrTooLarge, and per offset ErrUnknownPartition.

## 3. Phases as data

The machine is in one of three states:

- `Standby`: not serving. Every call gets ErrUnavailable.
- `Loading`: reading every group from the log (after RoleChanged active, or after a broken term).
  Every call gets ErrUnavailable.
- `Serving`: a map from group name to a group.

A group has fields every phase shares (generation, protocol type, protocol, leader, members; a member
has its session settings, protocol list, join order and session timer), and one phase value that
holds only what that phase needs:

| Phase | Holds | Left by |
|---|---|---|
| `Empty` | nothing (generation kept) | a join: to `Joining`, initial |
| `Joining` (PreparingRebalance) | round start and end, the end of the initial delay (if any), the parked join of each member | round done: `Syncing`; no member left: `Empty`; install refused or no common protocol: stays `Joining` |
| `Syncing` (CompletingRebalance) | sync deadline, the parked sync of each follower | leader's assignment logged: `Stable`; deadline: `Joining`; join, leave or expiry: `Joining` |
| `Stable` | the assignment of each member | join, leave or expiry: `Joining` |

So a parked sync in `Joining`, or a parked join in `Stable`, cannot be written. Any phase goes to
`Empty` when the last member leaves.

Waiting for a log write is not a phase. A group with a write in flight has a queue (section 6).

## 4. Rules

Today's tests are in `groups/coordinator_test.go` (C), `groups/rules_test.go` (R) and
`groups/waitjoin_test.go` (W). "code" means today's code has the rule but no test checks it.

| # | Rule | Today's test | Input / phase |
|---|---|---|---|
| 1 | Join with no group, no protocol type, no protocols, or session timeout <= 0: ErrInvalid. | R TestEmptyMemberIDAndBadJoins | Join |
| 2 | Not serving: every call gets ErrUnavailable. | R TestCloseWakesParkedCalls, R TestStoreFailureBreaksTermAndRebuildRecovers | any / Standby, Loading |
| 3 | A group with members refuses another protocol type at once: ErrInconsistentGroupProtocol. | R TestProtocolMismatch | Join |
| 4 | Empty member id: a new member with id ClientID + "-" + random (random alone without a client id). | R TestEmptyMemberIDAndBadJoins | Join |
| 5 | A member id that is not "" and not a member: ErrUnknownMember. | R TestEmptyMemberIDAndBadJoins | Join |
| 6 | Rebalance timeout 0 means the session timeout. | code | Join |
| 7 | A join parks until its round ends. A newer join of the same member answers the older one ErrRebalanceInProgress. | code | Join / Joining |
| 8 | A join on Empty opens an initial round: it waits the initial delay (default 3 s) for more members. Each new member starts the wait again, but never past the round end. | C TestOneMemberJoinsAndSyncs | Join / Empty, Joining |
| 9 | A join on Stable or Syncing opens a round at once, with no delay. Parked syncs get ErrRebalanceInProgress. | C TestSecondMemberRebalancesStableGroup | Join / Stable, Syncing |
| 10 | Round end = round start + the largest rebalance timeout. A member with a longer one moves it later. | R TestLeaderNeverSyncs, W TestJoinCanceledThenRoundCompletes | Joining |
| 11 | A round is done when every member has a parked join and the initial delay passed. | C TestSecondMemberRebalancesStableGroup | Join / Joining |
| 12 | At the round end, members with no parked join are removed (logged `GroupRemove`, reason expire). | W TestJoinCanceledThenRoundCompletes | TimerFired / Joining |
| 13 | No member left: Empty. The generation is kept. | C TestLeaveRebalances | Joining |
| 14 | The leader stays if it is still a member, else the member that joined first leads. | C TestSecondMemberRebalancesStableGroup | Joining |
| 15 | The protocol is the first in the leader's list that every member has. None: every parked join gets ErrInconsistentGroupProtocol, and the group stays in its round with its members. | R TestProtocolMismatch | Joining |
| 16 | A done round logs `GroupInstall` (generation + 1, members by id, each with the metadata of the chosen protocol), then goes to Syncing. Each parked join gets generation, protocol, leader and its own id; only the leader gets the member list. | C TestOneMemberJoinsAndSyncs, C TestSecondMemberRebalancesStableGroup, R TestConcurrentJoinsOneRound | Joining -> Syncing |
| 17 | An install refused with ErrTooLarge or ErrInvalid gives that code to the parked joins. The round stays open. | code | CommandDone / Joining |
| 18 | Sync, heartbeat: unknown group or member is ErrUnknownMember, then another generation is ErrIllegalGeneration. | R TestSyncRules, C TestHeartbeatTable | Sync, Heartbeat |
| 19 | Sync in Joining: ErrRebalanceInProgress. | R TestSyncRules | Sync / Joining |
| 20 | A follower's sync in Syncing parks until the leader's assignment is logged. A newer sync of the same member answers the older one ErrRebalanceInProgress. | C TestSecondMemberRebalancesStableGroup | Sync / Syncing |
| 21 | The leader's sync: a member assigned that is not a member is ErrUnknownMember, a member assigned twice is ErrInvalid, both before any write. | R TestSyncRules | Sync / Syncing |
| 22 | The leader's assignment is logged (`GroupAssign`), then Stable. The leader and every parked follower get their own bytes (none: empty). Every member's session starts again. | C TestOneMemberJoinsAndSyncs, C TestSessionExpiryRemovesSilentMember | Syncing -> Stable |
| 23 | Sync in Stable: the member's stored assignment, at once. A follower's Assignments are ignored. | R TestSyncRules, R TestConcurrentJoinsOneRound | Sync / Stable |
| 24 | Sync deadline (install time + the largest rebalance timeout): back to Joining; parked syncs get ErrRebalanceInProgress. | R TestLeaderNeverSyncs | TimerFired / Syncing |
| 25 | Heartbeat with the right member and generation: Stable is OK, any other phase (Syncing too) is ErrRebalanceInProgress. Both renew the session. | C TestHeartbeatTable, C TestLeaveRebalances | Heartbeat |
| 26 | Leave: one code per listed member, in order. Unknown is ErrUnknownMember (all of them for an unknown group). The known ones leave in one logged `GroupRemove` (reason leave), each once. A refused write gives its code to the known ones. | C TestLeaveRebalances | Leave |
| 27 | After members are removed: no member left is Empty; Joining may now be done; Stable or Syncing opens a round. Their parked calls get ErrUnknownMember. | C TestLeaveRebalances, C TestSessionExpiryRemovesSilentMember | Leave, TimerFired |
| 28 | A member silent for its session timeout is removed (logged `GroupRemove`, reason expire), then rule 27. | C TestSessionExpiryRemovesSilentMember | TimerFired |
| 29 | Join, sync, heartbeat and the member's own offset commit start its session again. | C TestSessionExpiryRemovesSilentMember; commit: code | all calls |
| 30 | NEW: a member with a parked join or sync does not expire. Its session starts again when the call ends. Today it can expire while it waits (question 1). | none | Joining, Syncing |
| 31 | Offset commit is a write. The log decides membership and generation (manual commit: generation -1, no member, only for a group with no members) and gives one code per offset. A failed write is ErrUnavailable and does not end the term. | R TestOffsetCommitChecks | OffsetCommit |
| 32 | OffsetFetch, Describe and List read the log, not memory. Describe gives the log's state (Empty, Stable, PreparingSync). List shows installed groups only. | R TestOffsetCommitChecks, C TestOneMemberJoinsAndSyncs | reads |
| 33 | An abandoned parked join or sync gets no reply and is forgotten. The member stays, but it no longer counts as joined. | W TestJoinCtxEndsWhileParked, W TestJoinCanceledThenRoundCompletes, R TestLeaderNeverSyncs | CallAbandoned |
| 34 | Losing the role: every parked call gets ErrUnavailable, every timer is cancelled, memory is dropped. | R TestCloseWakesParkedCalls | RoleChanged |
| 35 | Taking the role: every group is read from the log. Each member's session starts now (the grace window, decision Q5). Stable if the assigned generation is the generation and the leader is a member; Empty with no member; else a round from now, with no delay. Join order: join time, then id. | R TestRebuildKeepsMembersAndExpiresSilent | RoleChanged, ReadDone |
| 36 | A write that failed in a way that may have changed the log ends the term (section 6). | R TestStoreFailureBreaksTermAndRebuildRecovers | CommandDone |
| 37 | Many joins at the same time make one round with one leader. For the machine these are just inputs in order. | R TestConcurrentJoinsOneRound | Join |

The model checks rules 1 to 35. It leaves out 36 (it has no faults) and has nothing to check in 37.
Rule 34 "every timer is cancelled" and the queue of section 6 are machine details.

## 5. Timers

- Session: one timer per member, `After` = its session timeout. Set again by rule 29. Cancelled when
  the member is removed, while it has a parked call (rule 30), and on role loss.
- Round end or sync deadline: one timer per group (a group is in one phase). Set when a round opens,
  moved by rule 10, set again at the install (rule 24). Cancelled when the phase changes.
- Initial delay: one timer per group in an initial round. Cancelled when the round ends.
- Each timer has an ID that the machine gives out, and a Gen. `TimerFired` carries both. The machine
  keeps the current Gen of each live ID and ignores any other. This is needed: in `live`, a timer that
  fired just before its `CancelTimer` has already put its body in the inbox.
- Two timers are not in the design but are needed (questions 3 and 5): a write deadline per in-flight
  write, and a retry while `Loading`.

## 6. Waiting for a log write

- A group with a `SubmitCommand` in flight is busy. Every later input for that group (calls,
  `TimerFired` of its timers, `CallAbandoned` of its calls) waits in a queue, in order. This is what
  the group mutex does today.
- On `CommandDone` the machine applies the answer, then takes the queued inputs one by one. An input
  that submits a new write stops the drain until its own `CommandDone`.
- An abandoned call that waits in the queue is taken out with no reply. If its write is in flight,
  the answer of the write is still applied; only the reply is dropped.
- `List`, `RoleChanged` and inputs for other groups do not wait.
- Settle. OK: apply. ErrTooLarge or ErrInvalid: nothing was written; the caller gets the code and the
  term goes on. Anything else (ErrUnavailable, ErrTimeout, ErrIO, and also a business code such as
  ErrIllegalGeneration, which shows memory and log disagree) may have changed the log or shows they
  differ: the term ends. The caller, every parked call and every queued call get ErrUnavailable,
  every timer is cancelled, memory is dropped, and the machine goes to `Loading` to read the state
  again.
- Today a failed expiry write tries again after 1 s (`retryAfter`), but `settle` has already broken
  the term, so the retry never runs. The machine drops it.

## 7. Term and role

- The host sends `RoleChanged{Active}` only when "active appender and Ready" changes.
- Active false: go to `Standby` (rule 34). A late `CommandDone` or `ReadDone` has an unknown Req and
  is ignored. There is no ErrNotLeader code: ErrUnavailable tells the client to try the next address,
  as today.
- Active true: go to `Loading`: `SubmitRead(ListGroups)`, then one `SubmitRead(DescribeGroup)` per
  group. When all are done, go to `Serving` (rule 35).
- A broken term (section 6) goes to `Loading` too, without waiting for a role change.
- Request ids never repeat in one run, so an answer from an earlier term can not be taken for a new one.

## 8. Time, ids and randomness

- `cx.Now()` (monotonic) for every timer and deadline. `cx.Wall()` in milliseconds for `TimeMs` in
  logged commands. Nothing else reads time.
- A new member id is 16 bytes from `cx.Rand()` as base32 text (today: `crypto/rand.Text`), after
  ClientID and a dash. In `live` the stream is seeded from the OS once. Member ids are not secrets:
  Describe returns them.
- Call ids come from the host, request and timer ids from counters in the machine state.
- The model takes time as an argument of `Step`, and a new member's id as `Join.NewID`.

## 9. Hosting

Production, so that `replica` and `live` do not change:

- `server.Node` starts a second `live.Run` with the machine, its own inbox and the same clock.
- Its `Exec`: `SubmitCommand` becomes `ControlPlaneDeliver{From: rebalance, Req, Op}` to the replica
  inbox; `SubmitRead` becomes `LocalRead{From: rebalance, Req, Op}`; `Reply` goes to the RPC that waits.
- The replica answers with `ControlPlaneReply{To: rebalance}`. `Node.reply` delivers it to the
  rebalance inbox as `CommandDone` or `ReadDone`. `Node.reply` must look at `To` first: today it
  deletes `waiters[Req]` before it checks `To`, and the two processes count request ids apart.
- The replica loop's `AfterStep` sends `RoleChanged` when `IsActive() && Ready()` changes. This
  replaces `watchGroups` (the 100 ms poll), `nodeGroups` and `groupRole`.
- A gRPC handler gives the call an id, delivers the input and waits for the Reply. When its context
  ends it delivers `CallAbandoned` and answers ErrTimeout with MaybeApplied, as today.

Simulator: each node gets a second dst process that crashes and restarts with the replica. The
existing `simNode` wrapper sends `RoleChanged` after each replica step; a small wrapper of the
rebalance process turns its submits into sends to the replica, and the replica's replies with
`To: rebalance` go back to it.

## 10. The model

`spec.Rebalance.Step(now, input) []Reply`. Inputs: `Join`, `Sync`, `Heartbeat`, `Leave`, `StoreCall`
(offset commit and fetch, describe, list), `Abandon`, `RoleChanged`. Deadlines up to `now` fire first,
earliest first. The logged half of the state is kept in a `spec.ControlPlane` (the metadata model),
which also answers the reads. No faults, no queue, no timers or requests: those are how the machine
gets there, not what it answers. A conformance test (T6.3) sends both the same inputs and matches
replies by call id, because the order inside one step may differ. It should not draw two deadlines at
the same instant, or accept either order.

## Questions for the user

1. Rule 30. Today a member that waits in a join or sync can expire by its session timer. Kafka does
   not expire it. Kafka clients use a rebalance timeout (5 min) much longer than the session (45 s),
   so with today's rule fast members are removed while the round waits for a slow one. The model uses
   the Kafka rule. Keep the Kafka rule (my advice), or today's?
2. Rule 25. A heartbeat in CompletingRebalance is ErrRebalanceInProgress today (tested). Kafka answers
   OK. The model keeps today's rule. Keep it?
3. The design has no write deadline. The replica does not always answer: a soft crash in the simulator
   drops its answers (`m.answers = nil`), and a stalled log never answers. Today each write has a 10 s
   `WriteTimeout`. I propose a timer per in-flight write; when it fires, the term ends (section 6).
4. After a write that may have changed the log, the design reads the state again with `LocalRead`. A
   write that timed out can still apply after that read, so memory is behind the log. Installs are
   safe (the log checks generation + 1, the next one fails and the term ends again), but a late
   `GroupRemove` can remove a member that memory still has, until the next install. Options: read with
   `ControlPlaneDeliver` (waits for a fresh tail), or wait for the replica's late answer, or accept it.
   Today has the same gap.
5. A load read fails while the node is not Ready, and `RoleChanged` is sent only on a change, so
   nothing starts the load again. I propose a retry timer (100 ms, like today's poll) in `Loading`.
6. OffsetFetch, Describe and List need no group memory. They could go from the handler straight to
   `LocalRead`, behind the same role bit, and leave the machine and the model. Keep them in the
   machine (the design), or move them out?
7. Offset commit and the queue: today a commit does not wait for a group write and does not end the
   term when it fails. I propose the same: it does not queue (the log decides at apply time, rule 2 of
   the plan), and a failure is only ErrUnavailable.
8. `CommandDone` and `ReadDone` are both one `ControlPlaneReply`; the machine knows which Req was which.
   One input would do. Keep two?
9. Describe shows the log's state (Empty, Stable, PreparingSync), not the memory phase that Kafka shows
   (PreparingRebalance, CompletingRebalance). Keep it?
10. The model is 559 lines, against a target of 300. The rules above need it. To cut, I would drop
    rule 35 (takeover) from the model and check it in the machine's own tests. Accept the size?
