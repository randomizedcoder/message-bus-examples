// Package envelope fills, stamps, and interprets the Envelope that rides on
// every proto-bench request, response, and bus payload. RTT is always taken
// from the driver's monotonic clock; envelope wall-clock timestamps are used
// only for one-way estimates, correlation, and anomaly detection (design §8.1).
package envelope

import (
	"crypto/sha256"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
)

// Carrier is implemented by every proto-bench request, response, and bus
// payload — each carries `Envelope envelope = 1` (design §3). Of extracts it so
// a transport can stamp generically without knowing the concrete message type.
type Carrier interface {
	GetEnvelope() *workloadsv1.Envelope
}

// Of returns m's envelope, or nil if m carries none / it is unset. Nil-safe: the
// generated GetEnvelope handles a nil message.
func Of(m proto.Message) *workloadsv1.Envelope {
	if c, ok := m.(Carrier); ok {
		return c.GetEnvelope()
	}
	return nil
}

// Fill populates the client-side envelope fields (run id, message id, sequence,
// send time, codec, transport, fixture). message_id is a raw 16-byte UUIDv7.
func Fill(env *workloadsv1.Envelope, runID string, seq uint64, c workloadsv1.Codec, t workloadsv1.Transport, fixture string) error {
	id, err := uuid.NewV7()
	if err != nil {
		return err
	}
	b, err := id.MarshalBinary()
	if err != nil {
		return err
	}
	env.RunId = runID
	env.MessageId = b
	env.Sequence = seq
	env.ClientSendTime = timestamppb.Now()
	env.Codec = c
	env.Transport = t
	env.Fixture = fixture
	return nil
}

// StampReceive records server_receive_time and the responder id when a request
// arrives; wire is the raw payload the responder received. When integrity is on
// it also records request_wire_bytes and request_sha256.
func StampReceive(env *workloadsv1.Envelope, responderID string, wire []byte, integrity bool) {
	env.ServerReceiveTime = timestamppb.Now()
	env.ResponderId = responderID
	if integrity {
		env.RequestWireBytes = uint32(len(wire))
		sum := sha256.Sum256(wire)
		env.RequestSha256 = sum[:]
	}
}

// StampSend records server_send_time just before the responder replies.
func StampSend(env *workloadsv1.Envelope) {
	env.ServerSendTime = timestamppb.Now()
}

// Sum256 is the integrity hash over the exact bytes a responder received.
func Sum256(b []byte) [32]byte { return sha256.Sum256(b) }

// RTT is the monotonic round-trip time; sendMono and recvMono are time.Now()
// readings on the driver (they retain a monotonic component), never envelope
// wall-clock timestamps.
func RTT(sendMono, recvMono time.Time) time.Duration { return recvMono.Sub(sendMono) }

// OneWay estimates forward (client→server) and reverse (server→client) one-way
// latencies from the envelope wall-clock timestamps, corrected by the clock
// offset for the responder's region. clientRecv is the driver's wall-clock
// reading when the response arrived. ok is false unless both server timestamps
// are present. Values are never clamped — negatives are kept (design §8.1).
func OneWay(env *workloadsv1.Envelope, clientRecv time.Time, offset time.Duration) (fwd, rev time.Duration, ok bool) {
	if env.GetServerReceiveTime() == nil || env.GetServerSendTime() == nil || env.GetClientSendTime() == nil {
		return 0, 0, false
	}
	send := env.GetClientSendTime().AsTime()
	srvRecv := env.GetServerReceiveTime().AsTime()
	srvSend := env.GetServerSendTime().AsTime()
	fwd = srvRecv.Sub(send) - offset
	rev = clientRecv.Sub(srvSend) + offset
	return fwd, rev, true
}

// Anomaly classifies clock/ordering pathologies for the
// mbbench_clock_anomalies_total{kind} counter (design §8.1).
type Anomaly uint8

const (
	AnomalyNegativeOneWay Anomaly = iota
	AnomalyServerTimesReversed
	AnomalyFutureSend
)

func (a Anomaly) String() string {
	switch a {
	case AnomalyNegativeOneWay:
		return "negative_one_way"
	case AnomalyServerTimesReversed:
		return "server_times_reversed"
	case AnomalyFutureSend:
		return "future_send"
	default:
		return "unknown"
	}
}

// Classify returns the anomalies present for one round trip. fwd/rev are the
// OneWay results; clientRecv is the driver receive reading.
func Classify(env *workloadsv1.Envelope, clientRecv time.Time, fwd, rev time.Duration) []Anomaly {
	var out []Anomaly
	if srvRecv, srvSend := env.GetServerReceiveTime(), env.GetServerSendTime(); srvRecv != nil && srvSend != nil {
		if srvSend.AsTime().Before(srvRecv.AsTime()) {
			out = append(out, AnomalyServerTimesReversed)
		}
	}
	if send := env.GetClientSendTime(); send != nil && send.AsTime().After(clientRecv) {
		out = append(out, AnomalyFutureSend)
	}
	if fwd < 0 || rev < 0 {
		out = append(out, AnomalyNegativeOneWay)
	}
	return out
}
