// Package speccheck holds the specs of the outside world that the control plane
// talks to: the object store (ObjectStore) and the control plane itself
// (ControlPlane). A spec is a small program that shows the correct behavior. It
// has no faults, it runs one operation at a time, and it keeps plain data. It
// is short so that a reader can see that it is correct.
//
// ObjectStore and ControlPlane answer a runtime.IOCall with a runtime.IOResult
// through Exec. Both have a StateHash.
//
// A spec is also used as a test fake (the adapters over the specs are in
// speccheck/specsystem). The main use is as an oracle: a checker we trust to say
// if a result is correct. Each real component is compared with its spec, step by
// step, using the helpers in checker. The real adapters are compared with the
// specs: the S3 object store against ObjectStore, and the control-plane stores
// (in-memory store, gRPC client, applier on MemoryKeyValueStore and on Pebble)
// against ControlPlane.
//
//	Tests: internal/system/s3store/conform_test.go and
//	internal/controlplane/controlplanetest/conform_test.go.
//	The S3 test needs MDS_S3_ENDPOINT.
//
// The specs themselves are checked in this package. The unit tests check each
// rule. ObjectStore is compared with a sorted-slice model. ControlPlane is
// compared with a naive model made of maps and loops (controlplane_model_test.go)
// and its invariants are checked: offsets are contiguous, the end offset equals
// the committed counts, and a repeated commit gives the same answer.
//
// Terms:
//
//	oracle  a checker we trust to say if a result is correct.
//	spec    a short, fault-free program that shows the correct behavior.
package speccheck
