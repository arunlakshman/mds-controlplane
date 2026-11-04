// Package contract is the control plane's request and response vocabulary: the
// operations a broker can ask for (commit a segment, find segments, topics,
// consumer groups, offsets) and the answers. It is plain data.
//
// contract imports runtime. runtime must never import contract: runtime carries
// an operation only as the runtime.ControlPlaneOperation interface, which the
// types here implement.
package contract

import (
	"fmt"

	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

// Offset is a partition log offset.
type Offset int64

// BrokerAddr is one broker's advertised address, as Metadata lists it.
type BrokerAddr struct {
	NodeID int32
	Host   string
	Port   int32
}

// TopicPartition names one partition of a topic.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// NewTopicPartition builds a TopicPartition.
func NewTopicPartition(topic string, partition int32) TopicPartition {
	return TopicPartition{Topic: topic, Partition: partition}
}

func (topicPartition TopicPartition) String() string {
	return fmt.Sprintf("%s-%d", topicPartition.Topic, topicPartition.Partition)
}

// CommitSegment registers one uploaded object holding batches for many
// partitions and assigns their offsets atomically: every batch is assigned or
// none is.
//
// Each batch gets Base = the partition's next offset, the end of its newest
// run (0 for an empty log), in request order: a later batch of the same
// partition continues after the earlier one. Offsets are contiguous per
// partition and never reused, whatever the interleaving of committers.
//
// MUST be idempotent per SegmentKey: a replay of a committed key with an
// identical request returns the originally assigned ranges, never new ones
// (so a retry after MaybeApplied is safe); the same key with a different
// request is ErrConflict. ErrConflict is also the answer for a LogAppend that
// the log refused because its slot is at or below the floor (not a commit,
// Outcome NotApplied). Those are the only two uses.
type CommitSegment struct {
	SegmentKey string
	Batches    []PendingBatch
}

// PendingBatch describes one uploaded batch awaiting offset assignment.
type PendingBatch struct {
	TopicPartition TopicPartition
	Records        int64 // record count in the batch
	Loc            runtime.SegmentRef
}

// CommitSegmentResp has one entry per request batch, in request order.
type CommitSegmentResp struct{ Assigned []BatchRange }

// BatchRange is one batch of a partition's log: records [Base, Base+Count)
// stored at Loc.
type BatchRange struct {
	TopicPartition TopicPartition
	Base           Offset
	Count          int64
	Loc            runtime.SegmentRef
}

// FindSegments finds which segments hold offsets >= From for a partition.
type FindSegments struct {
	TopicPartition TopicPartition
	From           Offset
	MaxBytes       int32
}

// FindSegmentsResp lists the batches holding offsets >= From in offset order. End is
// the partition end offset (the next offset to be assigned, ListOffsets
// latest), derived from the newest run.
type FindSegmentsResp struct {
	Ranges []BatchRange
	End    Offset
}

// DescribeTopics answers Kafka Metadata requests.
type DescribeTopics struct{ Topics []string } // empty = all

// DescribeTopicsResp answers DescribeTopics.
type DescribeTopicsResp struct{ Topics []TopicInfo }

// TopicInfo describes one topic.
type TopicInfo struct {
	Name       string
	Partitions int32
}

// CreateTopic creates a topic with at least Partitions partitions. ErrInvalid
// for an empty name or Partitions <= 0. The reply has no response.
//
// With FailIfExists the create is exclusive: if the topic already exists the
// answer is ErrTopicExists and nothing changes. The check and the create are
// one step in the control plane, so two creates of the same new name cannot
// both succeed. Without it an existing topic is not an error, and the topic
// keeps the larger of its old and new partition counts.
type CreateTopic struct {
	Name         string
	Partitions   int32
	FailIfExists bool
}

// GroupMember is one member of a consumer group, as the join phase gives it.
// Metadata is the subscription of the protocol the group chose.
type GroupMember struct {
	MemberID           string
	ClientID           string
	ClientHost         string
	GroupInstanceID    string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	Metadata           []byte
}

// GroupMemberInfo is a member as the state keeps it: its join time and the
// assignment the leader stored for it (empty until GroupAssign).
type GroupMemberInfo struct {
	GroupMember
	JoinedTimeMs int64
	Assignment   []byte
}

// GroupInstall is the result of a join phase. It starts generation
// Generation with exactly Members and clears every assignment. Generation
// must be the current generation + 1 (a new group is at generation 0), else
// ErrIllegalGeneration. Members must be non-empty, Leader must be one of
// them, and member ids must be unique (else ErrInvalid). The reply has no
// response.
type GroupInstall struct {
	Group        string
	ProtocolType string
	Protocol     string
	Generation   int32
	Leader       string
	TimeMs       int64
	Members      []GroupMember
}

// GroupAssign stores the leader's assignment (SyncGroup) for generation
// Generation. Generation must be the current one (else ErrIllegalGeneration),
// Leader must be the group leader (else ErrUnknownMember), and every member
// in Assignments must be a member (else ErrUnknownMember). Every member
// gets the bytes listed for it, or none. The reply has no response.
type GroupAssign struct {
	Group       string
	Generation  int32
	Leader      string
	TimeMs      int64
	Assignments []GroupAssignment
}

// GroupAssignment is the assignment bytes of one member.
type GroupAssignment struct {
	Member string
	Bytes  []byte
}

// GroupRemove removes members from a group (a leave, or a logged expiry).
// Unknown members are ignored. Reason is "leave" or "expire" and is kept
// nowhere. The generation does not change. The reply has no response.
type GroupRemove struct {
	Group   string
	Members []string
	TimeMs  int64
	Reason  string
}

// OffsetCommit stores committed offsets for a group. Generation >= 0 is a
// managed commit: the group must exist (else ErrUnknownMember), Member must
// be a member (else ErrUnknownMember) and Generation must be the current one
// (else ErrIllegalGeneration). Generation -1 with an empty Member is a manual
// commit, allowed only when the group has no members (else
// ErrIllegalGeneration). Any other generation is ErrIllegalGeneration.
// Each offset is checked alone: an unknown partition gets ErrUnknownPartition
// and an offset below 0 gets ErrInvalid, and the others still commit.
type OffsetCommit struct {
	Group      string
	Member     string
	Generation int32
	TimeMs     int64
	Offsets    []PartitionOffset
}

// PartitionOffset is one committed offset of a group. A read of a partition
// that has no commit gives Offset -1 and LeaderEpoch -1.
type PartitionOffset struct {
	TopicPartition TopicPartition
	Offset         int64
	LeaderEpoch    int32
	Metadata       string
}

// OffsetCommitResp has one error per request offset, in request order.
type OffsetCommitResp struct{ Errs []runtime.ErrCode }

// OffsetFetch reads committed offsets of a group. Empty Partitions means
// every committed partition, sorted by topic and partition.
type OffsetFetch struct {
	Group      string
	Partitions []TopicPartition
}

// OffsetFetchResp has one entry per partition, in the order asked.
type OffsetFetchResp struct{ Offsets []PartitionOffset }

// DescribeGroup reads one group. A group that was never installed has
// Exists false. State is "Empty" with no members, "Stable" when the stored
// assignment is for the current generation, else "PreparingSync".
type DescribeGroup struct{ Group string }

// DescribeGroupResp answers DescribeGroup. Members are sorted by member id.
type DescribeGroupResp struct {
	Exists             bool
	Generation         int32
	ProtocolType       string
	Protocol           string
	Leader             string
	AssignedGeneration int32
	State              string
	Members            []GroupMemberInfo
}

// ListGroups lists every group that was ever installed.
type ListGroups struct{}

// ListGroupsResp answers ListGroups, sorted by group name.
type ListGroupsResp struct{ Groups []GroupSummary }

// GroupSummary is one line of ListGroupsResp.
type GroupSummary struct {
	Name         string
	ProtocolType string
	State        string
}

// GroupProtocol is one protocol a join offers, with its metadata.
type GroupProtocol struct {
	Name     string
	Metadata []byte
}

// GroupJoin is a JoinGroup call (classic protocol, decision Q9). An empty
// MemberID asks for a new member. The answer comes when the join round ends,
// so the call can last a whole rebalance: the broker sets a long Timeout.
type GroupJoin struct {
	Group              string
	MemberID           string
	GroupInstanceID    string
	ClientID           string
	ClientHost         string
	SessionTimeoutMs   int32
	RebalanceTimeoutMs int32
	ProtocolType       string
	Protocols          []GroupProtocol
}

// IsControlPlaneOperation marks CommitSegment as a control-plane operation.
func (CommitSegment) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks FindSegments as a control-plane operation.
func (FindSegments) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks DescribeTopics as a control-plane operation.
func (DescribeTopics) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks CreateTopic as a control-plane operation.
func (CreateTopic) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks GroupInstall as a control-plane operation.
func (GroupInstall) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks GroupAssign as a control-plane operation.
func (GroupAssign) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks GroupRemove as a control-plane operation.
func (GroupRemove) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks OffsetCommit as a control-plane operation.
func (OffsetCommit) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks OffsetFetch as a control-plane operation.
func (OffsetFetch) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks DescribeGroup as a control-plane operation.
func (DescribeGroup) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks ListGroups as a control-plane operation.
func (ListGroups) IsControlPlaneOperation() {}

// IsControlPlaneOperation marks GroupJoin as a control-plane operation.
func (GroupJoin) IsControlPlaneOperation() {}

// IsControlPlaneResponse marks CommitSegmentResp as a control-plane response.
func (CommitSegmentResp) IsControlPlaneResponse() {}

// IsControlPlaneResponse marks FindSegmentsResp as a control-plane response.
func (FindSegmentsResp) IsControlPlaneResponse() {}

// IsControlPlaneResponse marks DescribeTopicsResp as a control-plane response.
func (DescribeTopicsResp) IsControlPlaneResponse() {}

// IsControlPlaneResponse marks OffsetCommitResp as a control-plane response.
func (OffsetCommitResp) IsControlPlaneResponse() {}

// IsControlPlaneResponse marks OffsetFetchResp as a control-plane response.
func (OffsetFetchResp) IsControlPlaneResponse() {}

// IsControlPlaneResponse marks DescribeGroupResp as a control-plane response.
func (DescribeGroupResp) IsControlPlaneResponse() {}

// IsControlPlaneResponse marks ListGroupsResp as a control-plane response.
func (ListGroupsResp) IsControlPlaneResponse() {}
