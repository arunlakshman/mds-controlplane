// Package controlplanetest is test support for the control plane. The property
// tests that compare the stores, the replica and the gRPC stack with the model in
// internal/spec are in test/conformance.
//
// Test support, used by tests in other packages:
//   - StartMem and Start (grpcserver.go) run an in-process gRPC control plane node.
//   - fakedynamodb is an in-memory DynamoDB for the log tests.
//   - metadatastore runs a metadata as one node on a local slot counter with
//     no log. It is also the GroupStore of the groups tests.
//   - storecall gives tests a typed view of the single Exec method of the stores.
//
// Nothing outside tests imports this tree.
package controlplanetest
