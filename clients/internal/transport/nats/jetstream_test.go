package natstransport

import (
	"testing"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/corpus"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/pool"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
)

func TestTelemetrySubject(t *testing.T) {
	tests := []struct {
		description string
		region      string
		expected    string
	}{
		{"typical region", "us-west-2", "wl.us-west-2.telemetry"},
		{"another region", "ap-south-1", "wl.ap-south-1.telemetry"},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			if got := TelemetrySubject(tc.region); got != tc.expected {
				t.Errorf("TelemetrySubject(%q) = %q, want %q", tc.region, got, tc.expected)
			}
		})
	}
}

func TestDurableName(t *testing.T) {
	tests := []struct {
		description string
		region      string
		expected    string
	}{
		{"durable per region", "us-west-2", "AGENT_us-west-2"},
		{"another region", "eu-west-1", "AGENT_eu-west-1"},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			if got := durableName(tc.region); got != tc.expected {
				t.Errorf("durableName(%q) = %q, want %q", tc.region, got, tc.expected)
			}
		})
	}
}

// fakeJSMsg implements transport.Msg plus NumDelivered, standing in for a
// JetStream delivery (jsMsg needs a live broker to answer Metadata()).
type fakeJSMsg struct{ n uint64 }

func (f fakeJSMsg) Bytes() []byte        { return nil }
func (f fakeJSMsg) Ack() error           { return nil }
func (f fakeJSMsg) Codec() codec.Codec   { return codec.Proto }
func (f fakeJSMsg) NumDelivered() uint64 { return f.n }

// plainMsg implements transport.Msg only (no NumDelivered), like an MQTT msg.
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
		{"first delivery", fakeJSMsg{n: 1}, false},
		{"second delivery", fakeJSMsg{n: 2}, true},
		{"many redeliveries", fakeJSMsg{n: 9}, true},
		{"zero treated as first", fakeJSMsg{n: 0}, false},
		{"non-jetstream msg", plainMsg{}, false},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			if got := Redelivered(tc.msg); got != tc.expected {
				t.Errorf("Redelivered(%v) = %v, want %v", tc.msg, got, tc.expected)
			}
		})
	}
}

func TestJSMsgCodecFromHeader(t *testing.T) {
	tests := []struct {
		description string
		ct          string
		expected    string
	}{
		{"proto content-type", codec.ContentType(codec.Proto), "proto"},
		{"protojson content-type", codec.ContentType(codec.ProtoJSON), "protojson"},
		// vtproto shares proto's wire format + Content-Type, so a vtproto payload
		// is decoded by the proto codec (the bytes are identical) — §7.3.
		{"vtproto content-type resolves to proto", codec.ContentType(codec.VT), "proto"},
		{"empty header falls back to proto", "", "proto"},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			m := nats.NewMsg(TelemetrySubject("us-west-2"))
			if tc.ct != "" {
				m.Header.Set(contentType, tc.ct)
			}
			m.Data = []byte("payload")
			jm := &jsMsg{m: m}
			if got := jm.Codec().Name(); got != tc.expected {
				t.Errorf("Codec().Name() = %q, want %q", got, tc.expected)
			}
			if string(jm.Bytes()) != "payload" {
				t.Errorf("Bytes() = %q, want %q", jm.Bytes(), "payload")
			}
			if jm.NumDelivered() != 1 { // no JetStream reply subject → treated as first delivery
				t.Errorf("NumDelivered() = %d, want 1 (no metadata)", jm.NumDelivered())
			}
		})
	}
}

func TestDecodeRoundTrip(t *testing.T) {
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
			m := nats.NewMsg(TelemetrySubject("us-west-2"))
			m.Header.Set(contentType, codec.ContentType(tc.cdc))
			m.Data = wire
			got, err := Decode(&jsMsg{m: m}, newTelemetry, "us-west-2/agent", opts)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			ts, ok := got.(*workloadsv1.TelemetrySample)
			if !ok {
				t.Fatalf("Decode returned %T, want *TelemetrySample", got)
			}
			// Decode receive-stamps the envelope (responder id + timestamp), so
			// compare the payload with both envelopes cleared — the codec must
			// round-trip the body exactly regardless of the stamp.
			want := proto.Clone(sample).(*workloadsv1.TelemetrySample)
			want.Envelope = nil
			ts.Envelope = nil
			if !proto.Equal(ts, want) {
				t.Errorf("decoded payload != original")
			}
		})
	}
}
