package simulator

import (
	"github.com/manifesterio/mds-controlplane/internal/runtime"
	"github.com/manifesterio/mds-controlplane/internal/verify/speccheck"
)

// specModel is the reference model the oracle compares with. This file is the
// only place in the simulator that imports speccheck: to use another reference
// model, change newSpec and nothing else.
//
// The model sees the writes in the order of the log, so it needs no "maybe
// applied" state: a write whose answer was lost is simply in the log or not.
type specModel interface {
	// Apply runs one operation (a write or a read) and returns its answer.
	Apply(op runtime.ControlPlaneOperation) runtime.ControlPlaneDone
}

func newSpec() specModel { return &speccheckModel{cp: speccheck.NewControlPlane()} }

type speccheckModel struct {
	cp  *speccheck.ControlPlane
	req runtime.RequestID
}

func (m *speccheckModel) Apply(op runtime.ControlPlaneOperation) runtime.ControlPlaneDone {
	m.req++
	d, _ := m.cp.Exec(runtime.ControlPlaneCall{Req: m.req, Op: op}).(runtime.ControlPlaneDone)
	return d
}
