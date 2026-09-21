package rpc

// Capabilities describes what a transport binding actually provides, so the
// benchmark tooling can label results honestly and avoid misleading comparisons
// between systems that solve different problems (§30, §34). Each binding returns
// its own Capabilities from Client.Capabilities().
type Capabilities struct {
	// NativeRequestReply: the transport has a first-class request/reply
	// primitive (gRPC, NATS core) rather than one synthesized from pub/sub +
	// a correlator.
	NativeRequestReply bool
	// DurableRequests: an in-flight request survives a broker/consumer restart
	// (JetStream, RabbitMQ quorum, Valkey streams, Redpanda).
	DurableRequests bool
	// DurableResponses: a response survives the caller being absent when it
	// arrives (it can be consumed after reconnect rather than orphaned).
	DurableResponses bool
	// Replay: delivered messages can be re-read from a durable log/stream.
	Replay bool
	// ServerDiscovery: the transport can report "no responder" without waiting
	// for the application timeout (NATS core, §28).
	ServerDiscovery bool
	// Streaming: the binding supports a long-lived bidirectional stream (§10).
	Streaming bool
	// AtLeastOnce: delivery may duplicate, so idempotency matters (§29).
	AtLeastOnce bool
}
