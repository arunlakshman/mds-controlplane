package controlplanetest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc/codes"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/controlplane/controlplaneclient"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

func TestClassifier(t *testing.T) {
	commit := runtime.ControlPlaneCall{Op: contract.CommitSegment{}}
	look := runtime.ControlPlaneCall{Op: contract.FindSegments{}}
	desc := runtime.ControlPlaneCall{Op: contract.DescribeTopics{}}
	dial := &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	reset := &net.OpError{Op: "read", Err: syscall.ECONNRESET}
	dns := &net.DNSError{Err: "no such host", Name: "cp"}
	ce := func(sent bool, code codes.Code, err error) error {
		return &controlplaneclient.CallError{Op: "x", Sent: sent, Code: code, Err: err}
	}
	cases := []struct {
		name string
		a    runtime.IOCall
		err  error
		code runtime.ErrCode
		out  runtime.Outcome
	}{
		{"business too large", commit, &controlplaneclient.Error{Code: runtime.ErrTooLarge}, runtime.ErrTooLarge, runtime.NotApplied},
		{"business conflict", commit, &controlplaneclient.Error{Code: runtime.ErrConflict}, runtime.ErrConflict, runtime.NotApplied},
		{"business invalid lookup", look, &controlplaneclient.Error{Code: runtime.ErrInvalid}, runtime.ErrInvalid, runtime.NotApplied},
		{"refused commit", commit, ce(false, codes.OK, dial), runtime.ErrUnavailable, runtime.NotApplied},
		{"dns commit", commit, ce(false, codes.OK, dns), runtime.ErrUnavailable, runtime.NotApplied},
		{"unavailable commit", commit, ce(true, codes.Unavailable, errors.New("unavailable")), runtime.ErrUnavailable, runtime.NotApplied},
		{"internal commit", commit, ce(true, codes.Internal, errors.New("boom")), runtime.ErrIO, runtime.MaybeApplied},
		{"internal lookup", look, ce(true, codes.Internal, nil), runtime.ErrIO, runtime.NotApplied},
		{"deadline sent commit", commit, ce(true, codes.DeadlineExceeded, context.DeadlineExceeded), runtime.ErrTimeout, runtime.MaybeApplied},
		{"deadline sent describe", desc, ce(true, codes.DeadlineExceeded, context.DeadlineExceeded), runtime.ErrTimeout, runtime.NotApplied},
		{"deadline before conn", commit, ce(false, codes.OK, context.DeadlineExceeded), runtime.ErrTimeout, runtime.NotApplied},
		{"reset commit", commit, ce(true, codes.OK, reset), runtime.ErrIO, runtime.MaybeApplied},
		{"eof commit", commit, ce(true, codes.OK, io.ErrUnexpectedEOF), runtime.ErrIO, runtime.MaybeApplied},
		{"reset lookup", look, ce(true, codes.OK, reset), runtime.ErrIO, runtime.NotApplied},
		{"unknown commit", commit, errors.New("?"), runtime.ErrIO, runtime.MaybeApplied},
		{"unknown lookup", look, errors.New("?"), runtime.ErrIO, runtime.NotApplied},
		{"wrapped business", commit, fmt.Errorf("w: %w", &controlplaneclient.Error{Code: runtime.ErrConflict}), runtime.ErrConflict, runtime.NotApplied},
	}
	for _, c := range cases {
		code, out := controlplaneclient.Classifier{}.Classify(c.a, c.err)
		if code != c.code || out != c.out {
			t.Errorf("%s: got %v/%v want %v/%v", c.name, code, out, c.code, c.out)
		}
	}
}

func TestClassifierLiveDial(t *testing.T) {
	l, _ := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	addr := l.Addr().String()
	_ = l.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, err := controlplaneclient.New(addr, nil).Call(ctx, contract.CommitSegment{SegmentKey: "k", Batches: []contract.PendingBatch{
		{TopicPartition: contract.NewTopicPartition("t", 0), Records: 1},
	}})
	if err == nil {
		t.Fatal("want error")
	}
	c, o := controlplaneclient.Classifier{}.Classify(runtime.ControlPlaneCall{Op: contract.CommitSegment{}}, err)
	if c != runtime.ErrUnavailable && c != runtime.ErrIO {
		t.Errorf("refused: %v %v (%v)", c, o, err)
	}
}
