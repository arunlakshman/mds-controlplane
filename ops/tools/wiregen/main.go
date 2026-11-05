// Command wiregen writes the plain value types of the control-plane contract and
// the code that converts them to and from the generated api/v1 messages, so the
// .proto file is the single source and nothing is translated by hand.
//
// It reads the message layout from the generated api/v1 package and the list of
// messages below (Go name, field renames and the doc comment). It writes two files:
//
//	internal/controlplane/contract/wire_types_gen.go   the value structs (no wire import)
//	internal/controlplane/wire/wire_gen.go             <Message>ToCore and <Message>FromCore
//
// Run it with make wire. make wire-check fails when the files on disk are out of
// date. The value types stay plain: they are compared, hashed and used as map keys
// by the deterministic core, which must not import protobuf.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/format"
	"log"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"
)

// spec says how one proto message becomes one Go type.
type spec struct {
	Msg string // proto message name
	Go  string // Go type name in package contract ("" when Ext is set)
	Ext string // an existing type in another package, such as runtime.SegmentRef; not written
	Doc string // doc comment lines of the Go type, with the leading //
	// File is the proto file of the message: "" for controlplane.proto, "log" for
	// controlplane_log.proto (internal messages that are not part of the service).
	File string
	// NoWire writes only the value struct and no converters (the message is never sent).
	NoWire bool
	// Embed names a message field that becomes an embedded field of the struct.
	Embed string
	// Fields renames a field: "proto_name": "GoName" or "proto_name": "GoName:NamedType",
	// where NamedType is a named integer type of package contract (for example Offset).
	Fields map[string]string
}

var specs = []spec{
	{Msg: "TopicPartition", Go: "TopicPartition", Ext: "", Doc: `// TopicPartition names one partition of a topic.`, Fields: map[string]string{}},
	{Msg: "SegmentRef", Go: "", Ext: "runtime.SegmentRef", Doc: ``, Fields: map[string]string{}},
	{Msg: "PendingBatch", Go: "PendingBatch", Ext: "", Doc: `// PendingBatch describes one uploaded batch awaiting offset assignment.`, Fields: map[string]string{"tp": "TopicPartition"}},
	{Msg: "BatchRange", Go: "BatchRange", Ext: "", Doc: `// BatchRange is one batch of a partition's log: records [Base, Base+Count)
// stored at Loc.`, Fields: map[string]string{"tp": "TopicPartition", "base": "Base:Offset"}},
	{Msg: "TopicInfo", Go: "TopicInfo", Ext: "", Doc: `// TopicInfo describes one topic.`, Fields: map[string]string{}},
	{Msg: "CommitSegmentRequest", Go: "CommitSegment", Ext: "", Doc: `// CommitSegment registers one uploaded object holding batches for many
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
// Outcome NotApplied). Those are the only two uses.`, Fields: map[string]string{}},
	{Msg: "CommitSegmentResponse", Go: "CommitSegmentResp", Ext: "", Doc: `// CommitSegmentResp has one entry per request batch, in request order.`, Fields: map[string]string{}},
	{Msg: "LookupRequest", Go: "FindSegments", Ext: "", Doc: `// FindSegments finds which segments hold offsets >= From for a partition.`, Fields: map[string]string{"tp": "TopicPartition", "from": "From:Offset"}},
	{Msg: "LookupResponse", Go: "FindSegmentsResp", Ext: "", Doc: `// FindSegmentsResp lists the batches holding offsets >= From in offset order. End is
// the partition end offset (the next offset to be assigned, ListOffsets
// latest), derived from the newest run.`, Fields: map[string]string{"end": "End:Offset"}},
	{Msg: "DescribeTopicsRequest", Go: "DescribeTopics", Ext: "", Doc: `// DescribeTopics answers Kafka Metadata requests.`, Fields: map[string]string{}},
	{Msg: "DescribeTopicsResponse", Go: "DescribeTopicsResp", Ext: "", Doc: `// DescribeTopicsResp answers DescribeTopics.`, Fields: map[string]string{}},
	{Msg: "CreateTopicRequest", Go: "CreateTopic", Ext: "", Doc: `// CreateTopic creates a topic with at least Partitions partitions. ErrInvalid
// for an empty name or Partitions <= 0. The reply has no response.
//
// With FailIfExists the create is exclusive: if the topic already exists the
// answer is ErrTopicExists and nothing changes. The check and the create are
// one step in the control plane, so two creates of the same new name cannot
// both succeed. Without it an existing topic is not an error, and the topic
// keeps the larger of its old and new partition counts.`, Fields: map[string]string{"topic": "Name"}},
	{Msg: "LogGroupMember", Go: "GroupMember", File: "log", NoWire: true, Doc: `// GroupMember is one member of a consumer group, as the join phase gives it.
// Metadata is the subscription of the protocol the group chose.`, Fields: map[string]string{}},
	{Msg: "LogGroupMemberInfo", Go: "GroupMemberInfo", File: "log", NoWire: true, Embed: "member", Doc: `// GroupMemberInfo is a member as the state keeps it: its join time and the
// assignment the leader stored for it (empty until GroupAssign).`, Fields: map[string]string{}},
	{Msg: "LogGroupAssignment", Go: "GroupAssignment", File: "log", NoWire: true, Doc: `// GroupAssignment is the assignment bytes of one member.`, Fields: map[string]string{"data": "Bytes"}},
	{Msg: "LogGroupInstall", Go: "GroupInstall", File: "log", NoWire: true, Doc: `// GroupInstall is the result of a join phase. It starts generation
// Generation with exactly Members and clears every assignment. Generation
// must be the current generation + 1 (a new group is at generation 0), else
// ErrIllegalGeneration. Members must be non-empty, Leader must be one of
// them, and member ids must be unique (else ErrInvalid). The reply has no
// response.`, Fields: map[string]string{}},
	{Msg: "LogGroupAssign", Go: "GroupAssign", File: "log", NoWire: true, Doc: `// GroupAssign stores the leader's assignment (SyncGroup) for generation
// Generation. Generation must be the current one (else ErrIllegalGeneration),
// Leader must be the group leader (else ErrUnknownMember), and every member
// in Assignments must be a member (else ErrUnknownMember). Every member
// gets the bytes listed for it, or none. The reply has no response.`, Fields: map[string]string{}},
	{Msg: "LogGroupRemove", Go: "GroupRemove", File: "log", NoWire: true, Doc: `// GroupRemove removes members from a group (a leave, or a logged expiry).
// Unknown members are ignored. Reason is "leave" or "expire" and is kept
// nowhere. The generation does not change. The reply has no response.`, Fields: map[string]string{}},
	{Msg: "LogDescribeGroup", Go: "DescribeGroup", File: "log", NoWire: true, Doc: `// DescribeGroup reads one group. A group that was never installed has
// Exists false. State is "Empty" with no members, "Stable" when the stored
// assignment is for the current generation, else "PreparingSync".`, Fields: map[string]string{}},
	{Msg: "LogDescribeGroupResponse", Go: "DescribeGroupResp", File: "log", NoWire: true, Doc: `// DescribeGroupResp answers DescribeGroup. Members are sorted by member id.`, Fields: map[string]string{}},
	{Msg: "LogPartitionOffset", Go: "PartitionOffset", File: "log", NoWire: true, Doc: `// PartitionOffset is one committed offset of a group. A read of a partition
// that has no commit gives Offset -1 and LeaderEpoch -1.`, Fields: map[string]string{"tp": "TopicPartition"}},
	{Msg: "LogOffsetCommit", Go: "OffsetCommit", File: "log", NoWire: true, Doc: `// OffsetCommit stores committed offsets for a group. Generation >= 0 is a
// managed commit: the group must exist (else ErrUnknownMember), Member must
// be a member (else ErrUnknownMember) and Generation must be the current one
// (else ErrIllegalGeneration). Generation -1 with an empty Member is a manual
// commit, allowed only when the group has no members (else
// ErrIllegalGeneration). Any other generation is ErrIllegalGeneration.
// Each offset is checked alone: an unknown partition gets ErrUnknownPartition
// and an offset below 0 gets ErrInvalid, and the others still commit.`, Fields: map[string]string{}},
	{Msg: "LogOffsetCommitResponse", Go: "OffsetCommitResp", File: "log", NoWire: true, Doc: `// OffsetCommitResp has one error per request offset, in request order.`, Fields: map[string]string{"errs": "Errs:runtime.ErrCode"}},
	{Msg: "LogOffsetFetch", Go: "OffsetFetch", File: "log", NoWire: true, Doc: `// OffsetFetch reads committed offsets of a group. Empty Partitions means
// every committed partition, sorted by topic and partition.`, Fields: map[string]string{}},
	{Msg: "LogOffsetFetchResponse", Go: "OffsetFetchResp", File: "log", NoWire: true, Doc: `// OffsetFetchResp has one entry per partition, in the order asked.`, Fields: map[string]string{}},
	{Msg: "LogListGroups", Go: "ListGroups", File: "log", NoWire: true, Doc: `// ListGroups lists every group that was ever installed.`, Fields: map[string]string{}},
	{Msg: "LogGroupSummary", Go: "GroupSummary", File: "log", NoWire: true, Doc: `// GroupSummary is one line of ListGroupsResp.`, Fields: map[string]string{}},
	{Msg: "LogListGroupsResponse", Go: "ListGroupsResp", File: "log", NoWire: true, Doc: `// ListGroupsResp answers ListGroups, sorted by group name.`, Fields: map[string]string{}},
	{Msg: "PartitionOffsetCommit", Ext: "contract.PartitionOffset", Fields: map[string]string{"tp": "TopicPartition"}},
	{Msg: "PartitionOffset", Ext: "contract.PartitionOffset", Fields: map[string]string{"tp": "TopicPartition"}},
	{Msg: "GroupOffsetCommitRequest", Ext: "contract.OffsetCommit", Fields: map[string]string{"member_id": "Member"}},
	{Msg: "GroupOffsetCommitResponse", Ext: "contract.OffsetCommitResp", Fields: map[string]string{"errs": "Errs:runtime.ErrCode"}},
	{Msg: "GroupOffsetFetchRequest", Ext: "contract.OffsetFetch", Fields: map[string]string{}},
	{Msg: "GroupOffsetFetchResponse", Ext: "contract.OffsetFetchResp", Fields: map[string]string{}},
	{Msg: "ListGroupsRequest", Ext: "contract.ListGroups", Fields: map[string]string{}},
	{Msg: "GroupSummary", Ext: "contract.GroupSummary", Fields: map[string]string{}},
	{Msg: "ListGroupsResponse", Ext: "contract.ListGroupsResp", Fields: map[string]string{}},
}

const header = "// Code generated by ops/tools/wiregen from internal/controlplane/api/v1/controlplane.proto. DO NOT EDIT.\n\n"

func main() {
	check := flag.Bool("check", false, "fail if the generated files are out of date, and write nothing")
	root := flag.String("root", ".", "repository root")
	flag.Parse()
	g := &gen{byMsg: map[string]spec{}}
	for _, s := range specs {
		g.byMsg[s.Msg] = s
	}
	types, err := g.types()
	if err != nil {
		log.Fatal(err)
	}
	wire, err := g.wire()
	if err != nil {
		log.Fatal(err)
	}
	files := map[string][]byte{
		filepath.Join(*root, "internal/controlplane/contract/wire_types_gen.go"): types,
		filepath.Join(*root, "internal/controlplane/wire/wire_gen.go"):           wire,
	}
	stale := false
	for path, want := range files {
		if *check {
			have, err := os.ReadFile(path) //nolint:gosec // G304: the path is built from the -root flag
			if err != nil || !bytes.Equal(have, want) {
				fmt.Fprintf(os.Stderr, "wiregen: %s is out of date; run make wire\n", path)
				stale = true
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			log.Fatal(err)
		}
		//nolint:gosec // G306: source files are meant to be readable by all
		if err := os.WriteFile(path, want, 0o644); err != nil {
			log.Fatal(err)
		}
	}
	if stale {
		os.Exit(1)
	}
}

type gen struct{ byMsg map[string]spec }

func (g *gen) descriptor(msg string) protoreflect.MessageDescriptor {
	if md := cpv1.File_v1_controlplane_proto.Messages().ByName(protoreflect.Name(msg)); md != nil {
		return md
	}
	return cpv1.File_v1_controlplane_log_proto.Messages().ByName(protoreflect.Name(msg))
}

// camel turns a proto field name into the Go name that protoc-gen-go gives the
// field of a message: segment_key -> SegmentKey, member_id -> MemberId.
func camel(name string) string {
	return join(name, false)
}

// goCamel is camel in Go style for the value types, where "id" is an initialism:
// member_id -> MemberID.
func goCamel(name string) string {
	return join(name, true)
}

func join(name string, initialisms bool) string {
	var b strings.Builder
	for part := range strings.SplitSeq(name, "_") {
		switch {
		case part == "":
		case initialisms && part == "id":
			b.WriteString("ID")
		default:
			b.WriteString(strings.ToUpper(part[:1]) + part[1:])
		}
	}
	return b.String()
}

// field is one field of a message, with its Go side worked out.
type field struct {
	proto   string // Go name in the generated message (Tp)
	goName  string // Go name in the value type (TopicPartition)
	named   string // a named integer type of contract, or ""
	repeat  bool
	embed   bool
	kind    protoreflect.Kind
	msgName string
}

func (g *gen) fields(s spec) ([]field, error) {
	md := g.descriptor(s.Msg)
	if md == nil {
		return nil, fmt.Errorf("no message %s in the proto file", s.Msg)
	}
	var out []field
	for i := 0; i < md.Fields().Len(); i++ {
		fd := md.Fields().Get(i)
		name := string(fd.Name())
		f := field{proto: camel(name), goName: goCamel(name), repeat: fd.IsList(), kind: fd.Kind()}
		f.embed = name == s.Embed
		if rule, ok := s.Fields[name]; ok {
			goName, named, _ := strings.Cut(rule, ":")
			f.goName, f.named = goName, named
		}
		if fd.Kind() == protoreflect.MessageKind {
			f.msgName = string(fd.Message().Name())
			if _, ok := g.byMsg[f.msgName]; !ok {
				return nil, fmt.Errorf("%s.%s: message %s has no spec", s.Msg, name, f.msgName)
			}
		}
		switch f.kind {
		case protoreflect.StringKind, protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.BoolKind, protoreflect.BytesKind, protoreflect.MessageKind, protoreflect.EnumKind:
		default:
			return nil, fmt.Errorf("%s.%s: kind %s is not supported by wiregen", s.Msg, name, f.kind)
		}
		out = append(out, f)
	}
	return out, nil
}

// goType is the Go type of a field in the value struct.
func (g *gen) goType(f field) string {
	var t string
	switch f.kind {
	case protoreflect.StringKind:
		t = "string"
	case protoreflect.Int32Kind:
		t = "int32"
	case protoreflect.Int64Kind:
		t = "int64"
	case protoreflect.BoolKind:
		t = "bool"
	case protoreflect.BytesKind:
		return "[]byte"
	case protoreflect.EnumKind:
		t = f.named
	case protoreflect.MessageKind:
		s := g.byMsg[f.msgName]
		t = s.Go
		if s.Ext != "" {
			t = s.Ext
		}
	}
	if f.named != "" {
		t = f.named
	}
	if f.repeat {
		return "[]" + t
	}
	return t
}

func (g *gen) types() ([]byte, error) {
	var body bytes.Buffer
	for _, s := range specs {
		if s.Ext != "" {
			continue
		}
		fs, err := g.fields(s)
		if err != nil {
			return nil, err
		}
		if s.Doc != "" {
			body.WriteString(s.Doc + "\n")
		}
		fmt.Fprintf(&body, "type %s struct {\n", s.Go)
		for _, f := range fs {
			if f.embed {
				fmt.Fprintf(&body, "\t%s\n", g.goType(f))
				continue
			}
			fmt.Fprintf(&body, "\t%s %s\n", f.goName, g.goType(f))
		}
		body.WriteString("}\n\n")
	}
	var b bytes.Buffer
	b.WriteString(header + "package contract\n\n")
	if strings.Contains(body.String(), "runtime.") {
		b.WriteString("import \"github.com/manifesterio/mds-controlplane/internal/runtime\"\n\n")
	}
	b.Write(body.Bytes())
	return format.Source(b.Bytes())
}

// namedType qualifies a named type of package contract for use in package wire.
func namedType(name string) string {
	if strings.Contains(name, ".") {
		return name
	}
	return "contract." + name
}

// qualified is the Go type of a spec as written outside package contract.
func qualified(s spec) string {
	if s.Ext != "" {
		return s.Ext
	}
	return "contract." + s.Go
}

func (g *gen) wire() ([]byte, error) {
	var body bytes.Buffer
	for _, s := range specs {
		if s.NoWire {
			continue
		}
		fs, err := g.fields(s)
		if err != nil {
			return nil, err
		}
		q := qualified(s)
		fmt.Fprintf(&body, "// %sToCore rebuilds a %s from a %s message.\nfunc %sToCore(m *cpv1.%s) %s {\n\treturn %s{\n", s.Msg, q, s.Msg, s.Msg, s.Msg, q, q)
		for _, f := range fs {
			fmt.Fprintf(&body, "\t\t%s: %s,\n", f.goName, g.toCore(f))
		}
		body.WriteString("\t}\n}\n\n")
		fmt.Fprintf(&body, "// %sFromCore builds a %s message from a %s.\nfunc %sFromCore(v %s) *cpv1.%s {\n\treturn &cpv1.%s{\n", s.Msg, s.Msg, q, s.Msg, q, s.Msg, s.Msg)
		for _, f := range fs {
			fmt.Fprintf(&body, "\t\t%s: %s,\n", f.proto, g.fromCore(f))
		}
		body.WriteString("\t}\n}\n\n")
	}
	var b bytes.Buffer
	b.WriteString(header)
	b.WriteString(`package wire

import (
`)
	if strings.Contains(body.String(), "slices.") {
		b.WriteString("\t\"slices\"\n\n")
	}
	b.WriteString("\tcpv1 \"github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1\"\n\t\"github.com/manifesterio/mds-controlplane/internal/controlplane/contract\"\n")
	if strings.Contains(body.String(), "runtime.") {
		b.WriteString("\t\"github.com/manifesterio/mds-controlplane/internal/runtime\"\n")
	}
	b.WriteString(`)

// mapSlice converts every element. An empty input gives nil, the same as a loop
// that appends to a nil slice.
func mapSlice[From, To any](in []From, convert func(From) To) []To {
	if len(in) == 0 {
		return nil
	}
	out := make([]To, len(in))
	for i, item := range in {
		out[i] = convert(item)
	}
	return out
}

`)
	b.Write(body.Bytes())
	return format.Source(b.Bytes())
}

func (g *gen) toCore(f field) string {
	get := "m.Get" + f.proto + "()"
	switch {
	case f.kind == protoreflect.EnumKind && f.repeat:
		return fmt.Sprintf("mapSlice(%s, ErrCodeToCore)", get)
	case f.kind == protoreflect.EnumKind:
		return "ErrCodeToCore(" + get + ")"
	case f.kind == protoreflect.MessageKind && f.repeat:
		return fmt.Sprintf("mapSlice(%s, %sToCore)", get, f.msgName)
	case f.kind == protoreflect.MessageKind:
		return fmt.Sprintf("%sToCore(%s)", f.msgName, get)
	case f.repeat || f.kind == protoreflect.BytesKind:
		return "slices.Clone(" + get + ")"
	case f.named != "":
		return namedType(f.named) + "(" + get + ")"
	}
	return get
}

func (g *gen) fromCore(f field) string {
	v := "v." + f.goName
	switch {
	case f.kind == protoreflect.EnumKind && f.repeat:
		return fmt.Sprintf("mapSlice(%s, ErrCodeFromCore)", v)
	case f.kind == protoreflect.EnumKind:
		return "ErrCodeFromCore(" + v + ")"
	case f.kind == protoreflect.MessageKind && f.repeat:
		return fmt.Sprintf("mapSlice(%s, %sFromCore)", v, f.msgName)
	case f.kind == protoreflect.MessageKind:
		return fmt.Sprintf("%sFromCore(%s)", f.msgName, v)
	case f.repeat || f.kind == protoreflect.BytesKind:
		return "slices.Clone(" + v + ")"
	case f.named != "":
		return "int64(" + v + ")"
	}
	return v
}
