package observation

import (
	"strings"
	"testing"

	"github.com/manifesterio/mds-controlplane/internal/controlplane/contract"
	"github.com/manifesterio/mds-controlplane/internal/runtime"
)

var allCodes = []runtime.ErrCode{
	runtime.OK, runtime.ErrTimeout, runtime.ErrUnavailable, runtime.ErrNotFound, runtime.ErrConflict,
	runtime.ErrConnClosed, runtime.ErrTooLarge, runtime.ErrCorrupt, runtime.ErrInvalid, runtime.ErrIO,
}

var allOutcomes = []runtime.Outcome{runtime.NotApplied, runtime.MaybeApplied, runtime.Applied}

func TestNilAndEmptySlicesAreEqual(t *testing.T) {
	br := []contract.BatchRange{{Base: 1, Count: 2}}
	tests := []struct {
		name  string
		left  ControlPlaneResult
		right ControlPlaneResult
		equal bool
	}{
		{"nil and empty assigned", ControlPlaneResult{}, ControlPlaneResult{Assigned: []contract.BatchRange{}}, true},
		{"nil and empty ranges", ControlPlaneResult{}, ControlPlaneResult{Ranges: []contract.BatchRange{}}, true},
		{"nil and empty topics", ControlPlaneResult{}, ControlPlaneResult{Topics: []contract.TopicInfo{}}, true},
		{"same ranges", ControlPlaneResult{Ranges: br}, ControlPlaneResult{Ranges: []contract.BatchRange{{Base: 1, Count: 2}}}, true},
		{"different count", ControlPlaneResult{Ranges: br}, ControlPlaneResult{Ranges: []contract.BatchRange{{Base: 1, Count: 3}}}, false},
		{"different end", ControlPlaneResult{End: 1}, ControlPlaneResult{End: 2}, false},
	}
	for _, tc := range tests {
		if got := tc.left.Equal(tc.right); got != tc.equal {
			t.Errorf("%s: Equal returned %v, but %v was expected.", tc.name, got, tc.equal)
		}
	}
	objects := []struct {
		name  string
		left  ObjectResult
		right ObjectResult
		equal bool
	}{
		{"nil and empty data", ObjectResult{}, ObjectResult{Data: []byte{}}, true},
		{"nil and empty keys", ObjectResult{}, ObjectResult{Keys: []string{}}, true},
		{"different data", ObjectResult{Data: []byte("a")}, ObjectResult{Data: []byte("b")}, false},
		{"different keys", ObjectResult{Keys: []string{"a"}}, ObjectResult{Keys: []string{"a", "b"}}, false},
	}
	for _, tc := range objects {
		if got := tc.left.Equal(tc.right); got != tc.equal {
			t.Errorf("%s: Equal returned %v, but %v was expected.", tc.name, got, tc.equal)
		}
	}
}

func TestNormalizeUsesNilForEmpty(t *testing.T) {
	o := ObjectResult{Data: []byte{}, Keys: []string{}}.Normalize()
	if o.Data != nil || o.Keys != nil {
		t.Errorf("Normalize left an empty slice in an object result.")
	}
	c := ControlPlaneResult{Assigned: []contract.BatchRange{}, Ranges: []contract.BatchRange{}, Topics: []contract.TopicInfo{}}.Normalize()
	if c.Assigned != nil || c.Ranges != nil || c.Topics != nil {
		t.Errorf("Normalize left an empty slice in a control-plane result.")
	}
}

func TestEveryCodeAndOutcomeIsCompared(t *testing.T) {
	for _, code := range allCodes {
		for _, outcome := range allOutcomes {
			base := ObjectResult{Code: code, Outcome: outcome}
			same := base
			if !base.Equal(same) {
				t.Errorf("Code %s with outcome %d is not equal to itself.", code, outcome)
			}
			for _, other := range allCodes {
				if other != code && base.Equal(ObjectResult{Code: other, Outcome: outcome}) {
					t.Errorf("Codes %s and %s compare as equal.", code, other)
				}
			}
			for _, other := range allOutcomes {
				if other != outcome && (ControlPlaneResult{Code: code, Outcome: outcome}).Equal(ControlPlaneResult{Code: code, Outcome: other}) {
					t.Errorf("Outcomes %d and %d compare as equal.", outcome, other)
				}
			}
		}
	}
}

func TestObjectConverters(t *testing.T) {
	tests := []struct {
		name string
		in   runtime.IOResult
		want ObjectResult
	}{
		{"put", runtime.ObjectPutDone{Err: runtime.ErrTimeout, Outcome: runtime.MaybeApplied}, ObjectResult{Code: runtime.ErrTimeout, Outcome: runtime.MaybeApplied}},
		{"get", runtime.ObjectGetDone{Data: []byte("x")}, ObjectResult{Data: []byte("x")}},
		{"get not found", runtime.ObjectGetDone{Err: runtime.ErrNotFound}, ObjectResult{Code: runtime.ErrNotFound}},
		{"delete", runtime.ObjectDeleteDone{Outcome: runtime.Applied}, ObjectResult{Outcome: runtime.Applied}},
		{"list", runtime.ObjectListDone{Keys: []string{"a", "b"}}, ObjectResult{Keys: []string{"a", "b"}}},
	}
	for _, tc := range tests {
		got := FromObjectCompletion(tc.in)
		if msg := Diff(tc.want, got); msg != "" {
			t.Errorf("%s: %s", tc.name, msg)
		}
	}
}

func TestObjectConverterPanicsOnOtherType(t *testing.T) {
	defer func() {
		r := recover()
		msg, _ := r.(string)
		if !strings.Contains(msg, "needs an object store completion") {
			t.Errorf("The panic message was %v, which does not explain the mistake.", r)
		}
	}()
	FromObjectCompletion(runtime.ControlPlaneDone{})
}

func TestControlPlaneConverter(t *testing.T) {
	br := []contract.BatchRange{{TopicPartition: contract.TopicPartition{Topic: "t", Partition: 1}, Base: 5, Count: 2}}
	tests := []struct {
		name string
		in   runtime.ControlPlaneDone
		want ControlPlaneResult
	}{
		{"nil response", runtime.ControlPlaneDone{Err: runtime.ErrUnavailable}, ControlPlaneResult{Code: runtime.ErrUnavailable}},
		{"commit", runtime.ControlPlaneDone{Resp: contract.CommitSegmentResp{Assigned: br}, Outcome: runtime.Applied}, ControlPlaneResult{Outcome: runtime.Applied, Assigned: br}},
		{"lookup", runtime.ControlPlaneDone{Resp: contract.FindSegmentsResp{Ranges: br, End: 7}}, ControlPlaneResult{Ranges: br, End: 7}},
		{"describe", runtime.ControlPlaneDone{Resp: contract.DescribeTopicsResp{Topics: []contract.TopicInfo{{Name: "t", Partitions: 3}}}}, ControlPlaneResult{Topics: []contract.TopicInfo{{Name: "t", Partitions: 3}}}},
	}
	for _, tc := range tests {
		got := FromControlPlaneDone(tc.in)
		if msg := DiffControlPlane(tc.want, got); msg != "" {
			t.Errorf("%s: %s", tc.name, msg)
		}
	}
}

func TestControlPlaneConverterPanicsOnUnknownResponse(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Errorf("FromControlPlaneDone did not panic on an unknown response type.")
		}
	}()
	FromControlPlaneDone(runtime.ControlPlaneDone{Resp: unknownResp{}})
}

type unknownResp struct{ runtime.ControlPlaneResponse }

func TestDiffMessages(t *testing.T) {
	if msg := Diff(ObjectResult{}, ObjectResult{Data: []byte{}}); msg != "" {
		t.Errorf("Diff reported %q for nil and empty data.", msg)
	}
	tests := []struct {
		name     string
		expected ObjectResult
		actual   ObjectResult
		want     string
	}{
		{"code", ObjectResult{}, ObjectResult{Code: runtime.ErrIO},
			"The field code differs: the expected result has OK, but the actual result has ErrIO."},
		{"outcome", ObjectResult{}, ObjectResult{Outcome: runtime.Applied},
			"The field outcome differs: the expected result has NotApplied, but the actual result has Applied."},
		{"data", ObjectResult{Data: []byte("a")}, ObjectResult{Data: []byte("b")},
			"The field data differs: the expected result has (len 1) \"a\", but the actual result has (len 1) \"b\"."},
		{"keys", ObjectResult{Keys: []string{"a"}}, ObjectResult{},
			"The field keys differs: the expected result has (len 1) [\"a\"], but the actual result has (len 0) []."},
	}
	for _, tc := range tests {
		if got := Diff(tc.expected, tc.actual); got != tc.want {
			t.Errorf("%s: Diff returned %q, but %q was expected.", tc.name, got, tc.want)
		}
	}
	cp := DiffControlPlane(ControlPlaneResult{End: 1}, ControlPlaneResult{End: 2})
	if want := "The field end differs: the expected result has 1, but the actual result has 2."; cp != want {
		t.Errorf("DiffControlPlane returned %q, but %q was expected.", cp, want)
	}
	cp = DiffControlPlane(ControlPlaneResult{}, ControlPlaneResult{Topics: []contract.TopicInfo{{Name: "t", Partitions: 2}}})
	if !strings.HasPrefix(cp, "The field topics differs") {
		t.Errorf("DiffControlPlane returned %q for different topics.", cp)
	}
}

func TestDescribeTreatsNilAndEmptyTheSame(t *testing.T) {
	if a, b := (ObjectResult{}).Describe(), (ObjectResult{Data: []byte{}, Keys: []string{}}).Describe(); a != b {
		t.Errorf("Describe differs for nil and empty: %q and %q.", a, b)
	}
	if a, b := (ControlPlaneResult{}).Describe(), (ControlPlaneResult{Assigned: []contract.BatchRange{}, Topics: []contract.TopicInfo{}}).Describe(); a != b {
		t.Errorf("Describe differs for nil and empty: %q and %q.", a, b)
	}
	got := (ControlPlaneResult{End: 4, Ranges: []contract.BatchRange{{Base: 1}}}).Describe()
	for _, part := range []string{"code OK", "outcome NotApplied", "assigned (len 0)", "ranges (len 1)", "end 4", "topics (len 0)"} {
		if !strings.Contains(got, part) {
			t.Errorf("Describe output %q does not contain %q.", got, part)
		}
	}
}
