package envelope_test

import (
	"encoding/hex"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
)

func TestFill(t *testing.T) {
	var env workloadsv1.Envelope
	if err := envelope.Fill(&env, "run-1", 7,
		workloadsv1.Codec_CODEC_PROTO, workloadsv1.Transport_TRANSPORT_NATS_JETSTREAM, "medium"); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		description string
		got         any
		want        any
	}{
		{"run id is set", env.GetRunId(), "run-1"},
		{"sequence is set", env.GetSequence(), uint64(7)},
		{"message id is 16 bytes", len(env.GetMessageId()), 16},
		{"codec is set", env.GetCodec(), workloadsv1.Codec_CODEC_PROTO},
		{"transport is set", env.GetTransport(), workloadsv1.Transport_TRANSPORT_NATS_JETSTREAM},
		{"fixture is set", env.GetFixture(), "medium"},
		{"client send time is set", env.GetClientSendTime() != nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			if tc.got != tc.want {
				t.Fatalf("got %v, want %v", tc.got, tc.want)
			}
		})
	}

	// UUIDv7 message ids are unique across fills.
	var env2 workloadsv1.Envelope
	_ = envelope.Fill(&env2, "run-1", 8, workloadsv1.Codec_CODEC_PROTO, workloadsv1.Transport_TRANSPORT_NATS_JETSTREAM, "medium")
	if string(env.GetMessageId()) == string(env2.GetMessageId()) {
		t.Fatal("expected distinct message ids across fills")
	}
}

func TestSum256(t *testing.T) {
	// sha256("") is a well-known vector.
	sum := envelope.Sum256(nil)
	const want = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	if got := hex.EncodeToString(sum[:]); got != want {
		t.Fatalf("Sum256(nil)=%s, want %s", got, want)
	}
}

func TestStampReceiveIntegrity(t *testing.T) {
	var env workloadsv1.Envelope
	wire := []byte("payload")
	envelope.StampReceive(&env, "us-east-1/pod-0", wire, true)
	if env.GetResponderId() != "us-east-1/pod-0" {
		t.Fatalf("responder id not set: %q", env.GetResponderId())
	}
	if env.GetServerReceiveTime() == nil {
		t.Fatal("server receive time not set")
	}
	if env.GetRequestWireBytes() != uint32(len(wire)) {
		t.Fatalf("request_wire_bytes=%d, want %d", env.GetRequestWireBytes(), len(wire))
	}
	if len(env.GetRequestSha256()) != 32 {
		t.Fatalf("request_sha256 len=%d, want 32", len(env.GetRequestSha256()))
	}

	// integrity off leaves the fields empty.
	var env2 workloadsv1.Envelope
	envelope.StampReceive(&env2, "r", wire, false)
	if env2.GetRequestWireBytes() != 0 || len(env2.GetRequestSha256()) != 0 {
		t.Fatal("integrity off should not set wire bytes / sha256")
	}
}

func TestRTT(t *testing.T) {
	start := time.Now()
	if got := envelope.RTT(start, start.Add(5*time.Millisecond)); got != 5*time.Millisecond {
		t.Fatalf("RTT=%v, want 5ms", got)
	}
}

func TestOneWayAndClassify(t *testing.T) {
	base := time.Unix(1_700_000_000, 0).UTC()
	ts := func(d time.Duration) *timestamppb.Timestamp { return timestamppb.New(base.Add(d)) }

	tests := []struct {
		description   string
		env           *workloadsv1.Envelope
		clientRecv    time.Time
		offset        time.Duration
		wantOK        bool
		wantFwd       time.Duration
		wantRev       time.Duration
		wantAnomalies []envelope.Anomaly
	}{
		{
			description: "positive: well-ordered probe, zero offset",
			env:         &workloadsv1.Envelope{ClientSendTime: ts(0), ServerReceiveTime: ts(1 * time.Millisecond), ServerSendTime: ts(2 * time.Millisecond)},
			clientRecv:  base.Add(3 * time.Millisecond),
			offset:      0,
			wantOK:      true,
			wantFwd:     1 * time.Millisecond,
			wantRev:     1 * time.Millisecond,
		},
		{
			description: "boundary: missing server times → not ok",
			env:         &workloadsv1.Envelope{ClientSendTime: ts(0)},
			clientRecv:  base.Add(3 * time.Millisecond),
			wantOK:      false,
		},
		{
			description:   "corner: server times reversed is flagged",
			env:           &workloadsv1.Envelope{ClientSendTime: ts(0), ServerReceiveTime: ts(2 * time.Millisecond), ServerSendTime: ts(1 * time.Millisecond)},
			clientRecv:    base.Add(3 * time.Millisecond),
			wantOK:        true,
			wantFwd:       2 * time.Millisecond,
			wantRev:       2 * time.Millisecond,
			wantAnomalies: []envelope.Anomaly{envelope.AnomalyServerTimesReversed},
		},
		{
			description:   "corner: negative one-way from a large offset",
			env:           &workloadsv1.Envelope{ClientSendTime: ts(0), ServerReceiveTime: ts(1 * time.Millisecond), ServerSendTime: ts(2 * time.Millisecond)},
			clientRecv:    base.Add(3 * time.Millisecond),
			offset:        5 * time.Millisecond,
			wantOK:        true,
			wantFwd:       1*time.Millisecond - 5*time.Millisecond,
			wantRev:       1*time.Millisecond + 5*time.Millisecond,
			wantAnomalies: []envelope.Anomaly{envelope.AnomalyNegativeOneWay},
		},
		{
			description:   "corner: client send after receipt is a future_send anomaly",
			env:           &workloadsv1.Envelope{ClientSendTime: ts(10 * time.Millisecond), ServerReceiveTime: ts(1 * time.Millisecond), ServerSendTime: ts(2 * time.Millisecond)},
			clientRecv:    base.Add(3 * time.Millisecond),
			wantOK:        true,
			wantFwd:       -9 * time.Millisecond,
			wantRev:       1 * time.Millisecond,
			wantAnomalies: []envelope.Anomaly{envelope.AnomalyFutureSend, envelope.AnomalyNegativeOneWay},
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			fwd, rev, ok := envelope.OneWay(tc.env, tc.clientRecv, tc.offset)
			if ok != tc.wantOK {
				t.Fatalf("ok=%v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if fwd != tc.wantFwd || rev != tc.wantRev {
				t.Fatalf("fwd=%v rev=%v, want fwd=%v rev=%v", fwd, rev, tc.wantFwd, tc.wantRev)
			}
			got := envelope.Classify(tc.env, tc.clientRecv, fwd, rev)
			if !sameAnomalies(got, tc.wantAnomalies) {
				t.Fatalf("anomalies=%v, want %v", got, tc.wantAnomalies)
			}
		})
	}
}

func sameAnomalies(got, want []envelope.Anomaly) bool {
	if len(got) != len(want) {
		return false
	}
	set := map[envelope.Anomaly]bool{}
	for _, a := range got {
		set[a] = true
	}
	for _, a := range want {
		if !set[a] {
			return false
		}
	}
	return true
}

// TestServerDuration: the responder service time is server_send - server_receive
// when both stamps are present, and 0 (so the caller leaves the column blank)
// for a nil envelope, a missing stamp, or out-of-order stamps.
func TestServerDuration(t *testing.T) {
	base := time.Unix(1700000000, 0)
	ts := func(d time.Duration) *timestamppb.Timestamp { return timestamppb.New(base.Add(d)) }
	tests := []struct {
		description string
		env         *workloadsv1.Envelope
		expected    time.Duration
	}{
		{"nil envelope", nil, 0},
		{"both stamps, 250µs service", &workloadsv1.Envelope{ServerReceiveTime: ts(0), ServerSendTime: ts(250 * time.Microsecond)}, 250 * time.Microsecond},
		{"zero service (same instant)", &workloadsv1.Envelope{ServerReceiveTime: ts(time.Second), ServerSendTime: ts(time.Second)}, 0},
		{"missing send stamp", &workloadsv1.Envelope{ServerReceiveTime: ts(0)}, 0},
		{"missing receive stamp", &workloadsv1.Envelope{ServerSendTime: ts(0)}, 0},
		{"neither stamp", &workloadsv1.Envelope{}, 0},
		{"reversed stamps clamp to 0", &workloadsv1.Envelope{ServerReceiveTime: ts(time.Millisecond), ServerSendTime: ts(0)}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			if got := envelope.ServerDuration(tc.env); got != tc.expected {
				t.Errorf("ServerDuration = %v, want %v", got, tc.expected)
			}
		})
	}
}
