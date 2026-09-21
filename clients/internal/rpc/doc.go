// Package rpc is the transport-independent RPC layer of the RPC lab
// (suggested-improvements §8, §16, §34). It sits ABOVE clients/internal/transport
// and reuses its Requester/Responder/Publisher/Consumer primitives — it does not
// reimplement per-transport backends. Here live the pieces that are the same for
// every transport:
//
//   - the routing envelope helpers (Pack/Unpack an application message into the
//     rpc.v1.Request/Response google.protobuf.Any payload);
//   - the Correlator that matches asynchronous replies back to callers by
//     request_id, with one implementation of the timeout/cancel/duplicate races;
//   - the Client and Handler interfaces every transport binding satisfies, so
//     applications see one rpc.Call(ctx, req) API (§17);
//   - Status/error mapping and per-transport Capabilities metadata (§34).
//
// The rpc.v1 envelope is a separate track from proto-bench's workloads.v1
// measurement Envelope; the two coexist (an Any payload may itself carry a
// workloads.v1 message with its own Envelope).
package rpc
