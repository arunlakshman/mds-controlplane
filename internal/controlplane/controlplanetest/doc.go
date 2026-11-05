// Package controlplanetest is test support for the control plane. The property
// tests that compare the stores, the replica and the gRPC stack with the model in
// internal/spec are in test/conformance.
//
// Test support, used by tests in other packages:
//   - StartMem and Start (grpcserver.go) run an in-process gRPC control plane node.
//   - fakedynamodb is an in-memory DynamoDB for the log tests.
//
// Nothing outside tests imports this tree.
package controlplanetest
