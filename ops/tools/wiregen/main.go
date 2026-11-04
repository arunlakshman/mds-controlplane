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
	return cpv1.File_v1_controlplane_proto.Messages().ByName(protoreflect.Name(msg))
}

// camel turns a proto field name into its Go name: segment_key -> SegmentKey.
func camel(name string) string {
	var b strings.Builder
	for part := range strings.SplitSeq(name, "_") {
		if part != "" {
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
		f := field{proto: camel(name), goName: camel(name), repeat: fd.IsList(), kind: fd.Kind()}
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
		case protoreflect.StringKind, protoreflect.Int32Kind, protoreflect.Int64Kind, protoreflect.BoolKind, protoreflect.BytesKind, protoreflect.MessageKind:
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
	b.WriteString(`// Package wire converts the contract value types to and from the generated api/v1
// messages. Every function is generated (see ops/tools/wiregen); none is written
// by hand. A ToCore function reads the message with its Get methods, so a nil
// message or a missing part gives a zero value and never a panic.
package wire

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
	case f.kind == protoreflect.MessageKind && f.repeat:
		return fmt.Sprintf("mapSlice(%s, %sToCore)", get, f.msgName)
	case f.kind == protoreflect.MessageKind:
		return fmt.Sprintf("%sToCore(%s)", f.msgName, get)
	case f.repeat || f.kind == protoreflect.BytesKind:
		return "slices.Clone(" + get + ")"
	case f.named != "":
		return "contract." + f.named + "(" + get + ")"
	}
	return get
}

func (g *gen) fromCore(f field) string {
	v := "v." + f.goName
	switch {
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
