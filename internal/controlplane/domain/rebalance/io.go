package rebalance

import (
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// CallID names one RPC. The host gives each call its own id, unique in one
// run of the process. Every call gets one Reply, unless it is abandoned.
type CallID uint64

// Protocol, Member and Assignment are the plain call types of the group
// protocol. They are written here by hand until T6.6 generates them into this
// package (today the generated copies are in package groups, which uses sync
// and time, so the machine can not import it).
type (
	// Protocol is one protocol a member supports, with its metadata.
	Protocol struct {
		Name     string
		Metadata []byte
	}
	// Member is one member in the join answer. Only the leader gets the list.
	Member struct {
		ID         string
		InstanceID string
		Metadata   []byte
	}
	// Assignment is the bytes the leader gives one member.
	Assignment struct {
		Member string
		Bytes  []byte
	}
)

// The inputs (docs/rebalance.md section 3). TimerFired is runtime.TimerFired.
type (
	// Join is a JoinGroup call. An empty MemberID asks for a new member.
	// RebalanceTimeoutMs 0 means the session timeout.
	Join struct {
		Call               CallID
		Group              string
		MemberID           string
		GroupInstanceID    string
		ClientID           string
		ClientHost         string
		SessionTimeoutMs   int32
		RebalanceTimeoutMs int32
		ProtocolType       string
		Protocols          []Protocol // in the member's order of preference
	}
	// Sync is a SyncGroup call. Only the leader's Assignments are used.
	Sync struct {
		Call        CallID
		Group       string
		MemberID    string
		Generation  int32
		Assignments []Assignment
	}
	// Heartbeat is a Heartbeat call.
	Heartbeat struct {
		Call       CallID
		Group      string
		MemberID   string
		Generation int32
	}
	// Leave is a LeaveGroup call. The answer has one code per member.
	Leave struct {
		Call    CallID
		Group   string
		Members []string
	}
	// OffsetCommit is an OffsetCommit call. The machine sets Op.TimeMs.
	OffsetCommit struct {
		Call CallID
		Op   contract.OffsetCommit
	}
	// Describe is a DescribeGroups call.
	Describe struct {
		Call   CallID
		Groups []string
	}
	// List is a ListGroups call.
	List struct {
		Call CallID
	}
	// CallAbandoned says the RPC context of Call ended. The call gets no reply.
	CallAbandoned struct {
		Call CallID
	}
	// RoleChanged says whether the replica is now the active appender and Ready.
	RoleChanged struct {
		Active bool
	}
	// ReplicaReply is the replica's answer to a SubmitCommand or a SubmitRead.
	ReplicaReply struct {
		Req  runtime.RequestID
		Done runtime.ControlPlaneDone
	}
)

// The outputs other than sim.SetTimer and sim.CancelTimer.
type (
	// SubmitCommand asks the replica to log Op: GroupInstall, GroupAssign,
	// GroupRemove or OffsetCommit. The answer comes back as a ReplicaReply.
	SubmitCommand struct {
		Req runtime.RequestID
		Op  runtime.ControlPlaneOperation
	}
	// SubmitRead asks the replica to read Op: ListGroups or DescribeGroup.
	// Fresh false is a LocalRead. Fresh true is a read that waits for a fresh
	// check of the log tail (a ControlPlaneDeliver read).
	SubmitRead struct {
		Req   runtime.RequestID
		Op    runtime.ControlPlaneOperation
		Fresh bool
	}
	// Reply answers Call. Body is nil when Err is not OK, and for a heartbeat.
	Reply struct {
		Call CallID
		Err  runtime.ErrCode
		Body Answer
	}
)

// Answer is the body of a Reply: JoinAnswer, SyncAnswer, LeaveAnswer,
// CommitAnswer, DescribeAnswer or ListAnswer.
type Answer interface{ isAnswer() }

type (
	// JoinAnswer is the answer of a join. Only the leader gets Members.
	JoinAnswer struct {
		Generation int32
		Protocol   string
		Leader     string
		MemberID   string
		Members    []Member
	}
	// SyncAnswer is the assignment of the member that synced.
	SyncAnswer struct {
		Assignment []byte
	}
	// LeaveAnswer has one code per member of the call, in order.
	LeaveAnswer struct {
		Errs []runtime.ErrCode
	}
	// CommitAnswer has one code per offset of the call, in order.
	CommitAnswer struct {
		contract.OffsetCommitResp
	}
	// DescribeAnswer has one description per group of the call, in order.
	DescribeAnswer struct {
		Groups []GroupDescription
	}
	// ListAnswer has every group in memory, by name.
	ListAnswer struct {
		Groups []contract.GroupSummary
	}
)

// GroupDescription is the log's description of one group, with the state
// from memory (Dead for a group the machine does not have).
type GroupDescription struct {
	Name string
	contract.DescribeGroupResp
}

func (JoinAnswer) isAnswer()     {}
func (SyncAnswer) isAnswer()     {}
func (LeaveAnswer) isAnswer()    {}
func (CommitAnswer) isAnswer()   {}
func (DescribeAnswer) isAnswer() {}
func (ListAnswer) isAnswer()     {}
