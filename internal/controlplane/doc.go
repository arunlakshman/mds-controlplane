// Package controlplane is the control-plane service tree (renamed from cp/).
// Production mdscontrolplane serves gRPC via controlplane/server over internal/system/live
// and controlplane/replica (CP-DSM-7). Brokers dial the gRPC API (api/v1) with their own client.
//
// The gRPC wire contract is controlplane/api/v1 (decision 57). The reference
// model in internal/spec stays the independent oracle: it shares the schema
// types, not the apply code (decision 21).
//
// Broker membership (heartbeat and the live broker set) is one feature in
// several packages. Its file is brokers_handler.go in each; the map is at the top of
// server/brokers_handler.go.
//
// Tests sit next to the code they test. The model conformance tests are in
// test/conformance, and the end-to-end tests of the binary in test/integration. Helpers under
// this tree (metadata, executor) support the
// state machine's fold and IO. Not deterministic (goroutines, net): it is not
// checked by ops/tools/determinismcheck.
//
// # Map of the packages
//
//   - api/v1: the gRPC wire contract, defined in controlplane.proto.
//   - wire: generated (ops/tools/wiregen, from the .proto) conversions between the
//     contract value types and the api/v1 messages. No hand-written code.
//   - server: the gRPC adapter for the control-plane machine. Each handler reads the
//     generated api/v1 message and builds the contract value itself (the conversions
//     sit at the end of its file). It has one file
//     per context: topics_handler.go, groups_handler.go, brokers_handler.go and
//     status_handler.go. node.go is
//     only the node's life: start, stop, the run loop and the roles.
//   - (outside this tree) internal/system/live: the production runner of a dst
//     process. One goroutine owns Step, timers and inject work. It imports only dst
//     and the standard library.
//   - replica: one node's copy of the replicated log: leader or standby,
//     catch-up, checkpoint and restore. It has a Step that takes one event and returns dst values and IO calls.
//   - metadata: the kernel of the replicated state. It sends each operation to
//     the module that owns it, owns the envelope of a log entry (kind and id)
//     and the checkpoint, and applies commands in order to an ordered
//     key-value store.
//   - module: the one interface that each context implements to take part in
//     the replicated state: Mode, Write and Read, and its own log entry kinds
//     and their bytes (Kinds, KindOf, Encode, Decode).
//   - topics: the topics and segments context (CreateTopic, DescribeTopics,
//     CommitSegment, FindSegments). It owns the T, E, R, K and S keys.
//   - metadb/engine: the small ordered key-value seam under metadata and groups/storage,
//     with an in-memory store and the record codec they share.
//   - metadb/engine/pebble: the Pebble-backed key-value store that
//     metadata uses.
//   - executor: runs the control-plane IOCalls outside Step, and owns the Log
//     and ObjectStore ports.
//   - executor/dynamolog and executor/sqllog: the DynamoDB and Postgres
//     implementations of executor.Log. executor/logtest is their shared test
//     suite.
//   - membership: the broker membership context: which brokers are alive (leases).
//     Its state is not logged, so it is not a module.
//   - groups/storage: the consumer group context in the replicated state (the G,
//     M and O keys): the fold of the group writes and the group reads. It does
//     not know about topics.
//   - groups: the consumer group coordinator of the active node. It
//     speaks the classic group protocol only. It owns the Store port that the
//     node implements over the log.
//   - controlplanemetrics: the Prometheus instrumentation. It covers HTTP
//     handler time, DynamoDB call time, in-flight requests and request sizes.
//
// Time: all time comes from one interface, runtime.Clock (Mono, Wall, AfterFunc).
// system/realclock is the production clock and system/fakeclock is the one for
// tests (the simulator uses dst/clock on its own scheduler). The domain and server packages may not call time.Now,
// time.AfterFunc and the like (TestControlPlaneTimeComesFromClock).
//
// How they fit together, from the imports:
//
//   - server imports live, replica, executor and groups.
//   - replica imports metadata. metadata imports module, topics and groups/storage.
//     topics and groups/storage import module and engine, and not each other. executor imports only runtime. dynamolog and sqllog
//     import executor. Domain packages import no third-party package; see
//     TestControlPlaneLayers in architecture_test.go.
package controlplane
