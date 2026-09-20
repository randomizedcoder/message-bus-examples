package rmqtransport

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/corpus"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/pool"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
)

func TestTelemetryQueue(t *testing.T) {
	tests := []struct {
		description string
		region      string
		expected    string
	}{
		{"typical region", "us-west-2", "wl.telemetry.us-west-2"},
		{"another region", "ap-south-1", "wl.telemetry.ap-south-1"},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			if got := TelemetryQueue(tc.region); got != tc.expected {
				t.Errorf("TelemetryQueue(%q) = %q, want %q", tc.region, got, tc.expected)
			}
		})
	}
}

// fakeQMsg implements transport.Msg plus Redelivered, standing in for a
// quorum-queue delivery (qMsg wraps an amqp.Delivery from a live broker).
type fakeQMsg struct{ r bool }

func (f fakeQMsg) Bytes() []byte      { return nil }
func (f fakeQMsg) Ack() error         { return nil }
func (f fakeQMsg) Codec() codec.Codec { return codec.Proto }
func (f fakeQMsg) Redelivered() bool  { return f.r }

// plainMsg implements transport.Msg only (no Redelivered), like an MQTT msg.
type plainMsg struct{}

func (plainMsg) Bytes() []byte      { return nil }
func (plainMsg) Ack() error         { return nil }
func (plainMsg) Codec() codec.Codec { return codec.Proto }

func TestRedelivered(t *testing.T) {
	tests := []struct {
		description string
		msg         transport.Msg
		expected    bool
	}{
		{"first delivery", fakeQMsg{r: false}, false},
		{"redelivery", fakeQMsg{r: true}, true},
		{"non-quorum msg", plainMsg{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			if got := Redelivered(tc.msg); got != tc.expected {
				t.Errorf("Redelivered(%v) = %v, want %v", tc.msg, got, tc.expected)
			}
		})
	}
}

func TestQMsgCodecFromContentType(t *testing.T) {
	tests := []struct {
		description string
		ct          string
		expected    string
	}{
		{"proto content-type", codec.ContentType(codec.Proto), "proto"},
		{"protojson content-type", codec.ContentType(codec.ProtoJSON), "protojson"},
		// vtproto shares proto's wire format + Content-Type → decoded as proto.
		{"vtproto content-type resolves to proto", codec.ContentType(codec.VT), "proto"},
		{"empty content-type falls back to proto", "", "proto"},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			m := &qMsg{d: amqp.Delivery{ContentType: tc.ct, Body: []byte("payload"), Redelivered: true}}
			if got := m.Codec().Name(); got != tc.expected {
				t.Errorf("Codec().Name() = %q, want %q", got, tc.expected)
			}
			if string(m.Bytes()) != "payload" {
				t.Errorf("Bytes() = %q, want %q", m.Bytes(), "payload")
			}
			if !m.Redelivered() {
				t.Errorf("Redelivered() = false, want true")
			}
		})
	}
}

func TestQuorumDecodeRoundTrip(t *testing.T) {
	c := corpus.New(42)
	sample := c.Telemetry(3)
	tests := []struct {
		description string
		cdc         codec.Codec
	}{
		{"proto round-trip", codec.Proto},
		{"protojson round-trip", codec.ProtoJSON},
		{"vtproto round-trip", codec.VT},
	}
	opts := transport.Options{Codec: codec.Proto, Pool: pool.NopBuffers{}, Region: "us-west-2"}
	newTelemetry := func() proto.Message { return &workloadsv1.TelemetrySample{} }
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			wire, err := tc.cdc.MarshalAppend(nil, sample)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			m := &qMsg{d: amqp.Delivery{ContentType: codec.ContentType(tc.cdc), Body: wire}}
			got, err := Decode(m, newTelemetry, "us-west-2/agent", opts)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			ts, ok := got.(*workloadsv1.TelemetrySample)
			if !ok {
				t.Fatalf("Decode returned %T, want *TelemetrySample", got)
			}
			// Decode receive-stamps the envelope, so compare the payload with both
			// envelopes cleared — the codec must round-trip the body exactly.
			want := proto.Clone(sample).(*workloadsv1.TelemetrySample)
			want.Envelope = nil
			ts.Envelope = nil
			if !proto.Equal(ts, want) {
				t.Errorf("decoded payload != original")
			}
		})
	}
}
