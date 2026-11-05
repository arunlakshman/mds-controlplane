// Package controlplane is the control-plane service tree (renamed from cp/).
// Production mdscontrolplane serves gRPC via controlplane/server over internal/system/live
// and controlplane/replica (CP-DSM-7). Brokers dial the gRPC API (api/controlplane/v1) with their own client.
//
// The gRPC wire contract is controlplane/api/controlplane/v1 (decision 57). The reference
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
//   - api/controlplane/v1: the gRPC wire contract (the service and its own messages),
//     defined in controlplane.proto. It imports api/controlplane/shared/v1.
//   - api/controlplane/shared/v1: the messages that the service and the log entries both use.
//     No service, so no grpc.
//   - api/controlplane/log/v1: the log entries. It imports the shared package only.
//   - api/controlplane/options/v1: the custom options that say how a message becomes
//     a contract value type (read by the plugin ops/tools/protoc-gen-mdswire).
//   - wire: generated (ops/tools/protoc-gen-mdswire, from the .proto) conversions
//     between the contract value types and the api messages. No hand-written code.
//   - server: the gRPC adapter for the control-plane machine. Each handler reads the
//     generated api message and builds the contract value itself (the conversions
//     sit at the end of its file). It has one file
//     per context: topics_handler.go, groups_handler.go, brokers_handler.go and
//     status_handler.go. node.go is
//     only the node's life: start, stop, the run loop and the roles.
//   - (outside this tree) internal/system/live: the production runner of a dst
//     process. One goroutine owns Step, timers and inject work. It imports only dst
//     and the standard library.
//   - replica: one node's copy of the replicated log: leader or standby,
//     catch-up, checkpoint and restore. It has a Step that takes one event and returns dst values and IO calls.
//   - metadb: the kernel of the replicated state. It keeps one table of the
//     schemas of the contexts (metadb.Schema: one row per command or read) and
//     sends each operation to its row. It owns the log entry format (protobuf,
//     the log package of api/) and the checkpoint, and applies commands in
//     order to an ordered key-value store.
//   - domain/topic: the topics and segments context (CreateTopic, DescribeTopics,
//     CommitSegment, FindSegments). It owns the T, E, R, K and S keys. Its
//     rules are pure: Decide reads a View and returns events, and Apply writes
//     the events.
//   - metadb/engine: the small ordered key-value seam under metadata and domain/group,
//     with an in-memory store and the record codec they share.
//   - metadb/engine/pebble: the Pebble-backed key-value store that
//     metadata uses.
//   - executor: runs the control-plane IOCalls outside Step, and owns the Log
//     and ObjectStore ports.
//   - executor/dynamolog and executor/sqllog: the DynamoDB and Postgres
//     implementations of executor.Log. executor/logtest is their shared test
//     suite.
//   - domain/broker: the broker context: which brokers are alive (leases).
//     Its state is not logged, so it is not a schema.
//   - domain/group: the consumer group context in the replicated state (the G,
//     M and O keys): GroupInstall, GroupAssign, GroupRemove and OffsetCommit,
//     and the group reads. Its rules are pure like the topic ones: Decide reads
//     a View and returns events, and Apply writes the events. It does not know
//     about topics: the partition count comes in as a function at wiring.
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
//   - replica imports metadb. metadb imports domain/topic and domain/group.
//     domain/topic and domain/group import engine, and not each other or metadb.
//     executor imports only runtime. dynamolog and sqllog
//     import executor. Domain packages import no third-party package; see
//     TestControlPlaneLayers in architecture_test.go.
package controlplane
