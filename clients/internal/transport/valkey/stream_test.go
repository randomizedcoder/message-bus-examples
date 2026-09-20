package valkeytransport

import (
	"testing"

	"google.golang.org/protobuf/proto"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/corpus"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/pool"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
)

func TestTelemetryStream(t *testing.T) {
	tests := []struct {
		description string
		region      string
		expected    string
	}{
		{"typical region", "us-west-2", "wl:us-west-2:telemetry"},
		{"another region", "ap-south-1", "wl:ap-south-1:telemetry"},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			if got := TelemetryStream(tc.region); got != tc.expected {
				t.Errorf("TelemetryStream(%q) = %q, want %q", tc.region, got, tc.expected)
			}
		})
	}
}

// fakeStreamMsg implements transport.Msg plus Redelivered, standing in for a
// stream delivery (streamMsg's Ack needs a live client).
type fakeStreamMsg struct{ r bool }

func (f fakeStreamMsg) Bytes() []byte      { return nil }
func (f fakeStreamMsg) Ack() error         { return nil }
func (f fakeStreamMsg) Codec() codec.Codec { return codec.Proto }
func (f fakeStreamMsg) Redelivered() bool  { return f.r }

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
		{"first delivery", fakeStreamMsg{r: false}, false},
		{"re-read pending", fakeStreamMsg{r: true}, true},
		{"non-stream msg", plainMsg{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			if got := Redelivered(tc.msg); got != tc.expected {
				t.Errorf("Redelivered(%v) = %v, want %v", tc.msg, got, tc.expected)
			}
		})
	}
}

func TestStreamMsgCodecFromField(t *testing.T) {
	tests := []struct {
		description string
		ct          string
		expected    string
	}{
		{"proto ct", codec.ContentType(codec.Proto), "proto"},
		{"protojson ct", codec.ContentType(codec.ProtoJSON), "protojson"},
		// vtproto shares proto's wire format + content-type → decoded as proto.
		{"vtproto ct resolves to proto", codec.ContentType(codec.VT), "proto"},
		{"empty ct falls back to proto", "", "proto"},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			m := &streamMsg{ct: tc.ct, data: []byte("payload"), redelivered: true}
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

func TestStreamDecodeRoundTrip(t *testing.T) {
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
			m := &streamMsg{ct: codec.ContentType(tc.cdc), data: wire}
			got, err := Decode(m, newTelemetry, "us-west-2/agent", opts)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			ts, ok := got.(*workloadsv1.TelemetrySample)
			if !ok {
				t.Fatalf("Decode returned %T, want *TelemetrySample", got)
			}
			// Decode receive-stamps the envelope, so compare payload with both
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
