package controlplaneclient

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	cpv1 "github.com/manifesterio/mds-controlplane/internal/controlplane/api/v1"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/executor"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/replica"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/server"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

func realClient(t *testing.T) *Client {
	t.Helper()
	n := server.New(server.Config{
		Machine: replica.Config{
			CatchUpEvery: runtime.Duration(time.Millisecond),
			Rank:         0, Heartbeat: runtime.Duration(time.Second),
		},
		Log: executor.NewMemoryLog(), Objects: executor.NewMemoryObjects(),
		Self: runtime.NodeInstance{Node: "cp-0"},
	})
	if err := n.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	n.Register(gs)
	go func() { _ = gs.Serve(ln) }()
	t.Cleanup(func() {
		gs.GracefulStop()
		_ = n.Close()
	})
	return New(ln.Addr().String(), nil)
}

func TestClientAgainstMachine(t *testing.T) {
	c := realClient(t)
	ctx := t.Context()
	var ops []string
	c.Observe(func(operation string, _ time.Duration) { ops = append(ops, operation) })

	if err := c.CreateTopic(ctx, "t", 2); err != nil {
		t.Fatal(err)
	}
	topicPartition := contract.NewTopicPartition("t", 0)
	cs, err := c.Call(ctx, contract.CommitSegment{SegmentKey: "s1", Batches: []contract.PendingBatch{
		{TopicPartition: topicPartition, Records: 4, Loc: runtime.SegmentRef{Key: "s1", Off: 0, Len: 40}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if as := cs.(contract.CommitSegmentResp).Assigned; len(as) != 1 || as[0].Count != 4 {
		t.Fatalf("assigned %+v", as)
	}
	lk, err := c.Call(ctx, contract.FindSegments{TopicPartition: topicPartition, From: 0, MaxBytes: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if l := lk.(contract.FindSegmentsResp); len(l.Ranges) != 1 || l.End != 4 {
		t.Fatalf("lookup %+v", l)
	}
	ds, err := c.Call(ctx, contract.DescribeTopics{Topics: []string{"t"}})
	if err != nil || len(ds.(contract.DescribeTopicsResp).Topics) != 1 {
		t.Fatalf("describe %v %v", ds, err)
	}
	if len(ops) != 4 {
		t.Errorf("observer saw %v", ops)
	}

	_, err = c.Call(ctx, contract.CommitSegment{SegmentKey: "s1", Batches: []contract.PendingBatch{
		{TopicPartition: topicPartition, Records: 1, Loc: runtime.SegmentRef{Key: "s1", Len: 1}},
	}})
	be, ok := errors.AsType[*Error](err)
	if !ok || be.Code != runtime.ErrConflict {
		t.Fatalf("want conflict, got %v", err)
	}
}

func TestCallCtxAndUnknownOp(t *testing.T) {
	c := realClient(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Call(ctx, contract.CommitSegment{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ctx: %v", err)
	}
	_, err := c.Call(t.Context(), nil)
	be, ok := errors.AsType[*Error](err)
	if !ok || be.Code != runtime.ErrInvalid {
		t.Fatalf("unknown op: %v", err)
	}
}

func TestCallDialError(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	_, err = New(addr, nil).Call(t.Context(), contract.CommitSegment{
		SegmentKey: "k", Batches: []contract.PendingBatch{{TopicPartition: contract.NewTopicPartition("t", 0), Records: 1}},
	})
	if err == nil {
		t.Fatal("expected error")
	}
	code, out := Classifier{}.Classify(runtime.ControlPlaneCall{Op: contract.CommitSegment{}}, err)
	if code != runtime.ErrUnavailable && code != runtime.ErrIO {
		t.Errorf("dial: %v %v (%v)", code, out, err)
	}
}

func TestClassifyBusinessAndTimeout(t *testing.T) {
	be := &Error{Code: runtime.ErrConflict}
	code, out := Classifier{}.Classify(runtime.ControlPlaneCall{Op: contract.CommitSegment{}}, be)
	if code != runtime.ErrConflict || out != runtime.NotApplied {
		t.Fatalf("business: %v %v", code, out)
	}
	ce := &CallError{Op: "CommitSegment", Sent: true, Code: codes.DeadlineExceeded, Err: context.DeadlineExceeded}
	code, out = Classifier{}.Classify(runtime.ControlPlaneCall{Op: contract.CommitSegment{}}, ce)
	if code != runtime.ErrTimeout || out != runtime.MaybeApplied {
		t.Fatalf("timeout: %v %v", code, out)
	}
	_, out = Classifier{}.Classify(runtime.ControlPlaneCall{Op: contract.FindSegments{}}, ce)
	if out != runtime.NotApplied {
		t.Fatalf("read timeout outcome %v", out)
	}
	ce = &CallError{Op: "x", Sent: false, Err: syscall.ECONNREFUSED}
	code, out = Classifier{}.Classify(runtime.ControlPlaneCall{Op: contract.CommitSegment{}}, ce)
	if code != runtime.ErrUnavailable || out != runtime.NotApplied {
		t.Fatalf("unsent: %v %v", code, out)
	}
}

func TestFailoverSkipsUnavailable(t *testing.T) {
	bad := New("127.0.0.1:1", nil) // nothing listening
	// Preferred bad, then a real node.
	good := realClient(t)
	c := New("127.0.0.1:1,"+good.addrs[0], nil)
	_ = bad
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := c.CreateTopic(ctx, "t", 1); err != nil {
		t.Fatal(err)
	}
}

// TestClientCallCreateTopic runs the CreateTopic operation through Call, as the
// broker's ControlPlaneCall does.
func TestClientCallCreateTopic(t *testing.T) {
	c := realClient(t)
	ctx := t.Context()
	if _, err := c.Call(ctx, contract.CreateTopic{Name: "made", Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	response, err := c.Call(ctx, contract.DescribeTopics{Topics: []string{"made"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := response.(contract.DescribeTopicsResp).Topics; len(got) != 1 || got[0].Partitions != 3 {
		t.Fatalf("topics %+v", got)
	}
	_, err = c.Call(ctx, contract.CreateTopic{Name: "", Partitions: 1})
	if be, ok := errors.AsType[*Error](err); !ok || be.Code != runtime.ErrInvalid {
		t.Fatalf("empty name: %v", err)
	}
}

func TestMapRPCErrDetail(t *testing.T) {
	st := status.New(codes.AlreadyExists, "ErrConflict")
	st, err := st.WithDetails(&cpv1.ErrorDetail{
		Code: cpv1.ErrCode_ERR_CODE_CONFLICT, Outcome: cpv1.Outcome_OUTCOME_NOT_APPLIED,
	})
	if err != nil {
		t.Fatal(err)
	}
	got := convertRPCError("CommitSegment", st.Err())
	be, ok := errors.AsType[*Error](got)
	if !ok || be.Code != runtime.ErrConflict {
		t.Fatalf("got %v", got)
	}
	if ce, ok := errors.AsType[*CallError](convertRPCError("x", errors.New("plain"))); !ok || !ce.Sent {
		t.Fatalf("plain: %v", convertRPCError("x", errors.New("plain")))
	}
	for _, tc := range []struct {
		code codes.Code
		want runtime.ErrCode
	}{
		{codes.Unavailable, runtime.ErrUnavailable},
		{codes.InvalidArgument, runtime.ErrInvalid},
		{codes.NotFound, runtime.ErrNotFound},
		{codes.AlreadyExists, runtime.ErrConflict},
	} {
		got := convertRPCError("op", status.Error(tc.code, "x"))
		be, ok := errors.AsType[*Error](got)
		if !ok || be.Code != tc.want {
			t.Fatalf("%v: %v", tc.code, got)
		}
	}
	got = convertRPCError("op", status.Error(codes.DeadlineExceeded, "late"))
	ce, ok := errors.AsType[*CallError](got)
	if !ok || ce.Code != codes.DeadlineExceeded {
		t.Fatalf("deadline: %v", got)
	}
	got = convertRPCError("op", status.Error(codes.Internal, "boom"))
	if _, ok := errors.AsType[*CallError](got); !ok {
		t.Fatalf("internal: %v", got)
	}
}
