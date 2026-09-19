package codec_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/corpus"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/pool"
)

var codecs = []struct {
	name string
	c    codec.Codec
}{
	{"proto", codec.Proto},
	{"protojson", codec.ProtoJSON},
	{"vtproto", codec.VT},
}

// mapFree fixtures encode identically across proto and vtproto (no map-order
// nondeterminism); the rest are only checked for equal size + round-trip.
var mapFree = map[corpus.Fixture]bool{corpus.Tiny: true, corpus.Small: true, corpus.Max: true, corpus.Sparse: true}

// TestRoundTrip encodes then decodes every fixture with every codec and
// requires proto.Equal to the original.
func TestRoundTrip(t *testing.T) {
	c := corpus.New(42)
	bp := pool.NopBuffers{}
	for _, f := range corpus.AllFixtures {
		for _, cc := range codecs {
			t.Run(string(f)+"/"+cc.name, func(t *testing.T) {
				msg, err := c.Message(f)
				if err != nil {
					t.Fatal(err)
				}
				buf, err := codec.Encode(cc.c, bp, msg)
				if err != nil {
					t.Fatalf("encode: %v", err)
				}
				got := msg.ProtoReflect().New().Interface()
				if err := cc.c.Unmarshal(*buf, got); err != nil {
					t.Fatalf("decode: %v", err)
				}
				if !proto.Equal(msg, got) {
					t.Fatalf("round trip mismatch for %s/%s", f, cc.name)
				}
			})
		}
	}
}

// TestVTProtoWireParity: vtproto and proto are wire-compatible. Sizes match for
// every fixture; bytes match exactly for map-free fixtures; and cross-codec
// decode (encode with one, decode with the other) yields an equal message.
func TestVTProtoWireParity(t *testing.T) {
	c := corpus.New(7)
	bp := pool.NopBuffers{}
	for _, f := range corpus.AllFixtures {
		t.Run(string(f), func(t *testing.T) {
			msg, err := c.Message(f)
			if err != nil {
				t.Fatal(err)
			}
			pb, err := codec.Encode(codec.Proto, bp, msg)
			if err != nil {
				t.Fatal(err)
			}
			vb, err := codec.Encode(codec.VT, bp, msg)
			if err != nil {
				t.Fatal(err)
			}
			if len(*pb) != len(*vb) {
				t.Fatalf("size mismatch: proto=%d vtproto=%d", len(*pb), len(*vb))
			}
			if mapFree[f] && string(*pb) != string(*vb) {
				t.Fatalf("map-free fixture %s: proto and vtproto bytes differ", f)
			}
			// vt bytes decode with the proto codec and vice versa.
			g1 := msg.ProtoReflect().New().Interface()
			if err := codec.Proto.Unmarshal(*vb, g1); err != nil || !proto.Equal(msg, g1) {
				t.Fatalf("proto decode of vt bytes failed: err=%v", err)
			}
			g2 := msg.ProtoReflect().New().Interface()
			if err := codec.VT.Unmarshal(*pb, g2); err != nil || !proto.Equal(msg, g2) {
				t.Fatalf("vt decode of proto bytes failed: err=%v", err)
			}
		})
	}
}

// TestProtoJSONSpecialForms checks the ProtoJSON mappings that differ from a
// naive encoding/json: FieldMask as a comma path list, int64 as a string,
// bytes as base64, Timestamp/Duration as strings (design §3.6, demo step 4).
func TestProtoJSONSpecialForms(t *testing.T) {
	digest := make([]byte, 32)
	for i := range digest {
		digest[i] = byte(i)
	}
	req := &workloadsv1.DeployRequest{
		Envelope:       &workloadsv1.Envelope{RunId: "r", ClientSendTime: nowTS()},
		Ref:            &workloadsv1.WorkloadRef{},
		IdempotencyKey: "",
		RequestedAt:    nowTS(),
		Op: &workloadsv1.DeployRequest_Update{Update: &workloadsv1.UpdateWorkload{
			Spec: &workloadsv1.WorkloadSpec{
				Containers: []*workloadsv1.ContainerSpec{{
					Name:      "app-0",
					Image:     &workloadsv1.ImageSource{Repository: "r", Ref: &workloadsv1.ImageSource_Digest{Digest: digest}},
					Resources: &workloadsv1.Resources{Requests: &workloadsv1.ResourceQuantity{CpuMillis: 200}},
				}},
			},
			UpdateMask: fieldMask("replicas", "labels"),
		}},
	}
	buf, err := codec.Encode(codec.ProtoJSON, pool.NopBuffers{}, req)
	if err != nil {
		t.Fatal(err)
	}
	js := string(*buf)

	tests := []struct {
		description string
		want        string
	}{
		{"FieldMask renders as a comma-joined path list", `"updateMask":"replicas,labels"`},
		{"int64 renders as a quoted string", `"cpuMillis":"200"`},
		{"bytes render as base64 (first digest byte 0x00..)", `"digest":"AAECAwQF`},
		{"Timestamp renders as an RFC3339 string", `"requestedAt":"`},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			if !strings.Contains(js, tc.want) {
				t.Fatalf("ProtoJSON missing %q\njson: %s", tc.want, js)
			}
		})
	}
}

// TestUnknownFields: binary preserves unknown fields; ProtoJSON with
// DiscardUnknown=false rejects them (design §3.6, rule 7).
func TestUnknownFields(t *testing.T) {
	t.Run("positive: binary preserves an unknown field", func(t *testing.T) {
		ping := &workloadsv1.PingRequest{Envelope: &workloadsv1.Envelope{RunId: "r", ClientSendTime: nowTS()}}
		raw, err := codec.Proto.MarshalAppend(nil, ping)
		if err != nil {
			t.Fatal(err)
		}
		// append unknown field 99, varint 42
		raw = protowire.AppendTag(raw, 99, protowire.VarintType)
		raw = protowire.AppendVarint(raw, 42)
		var got workloadsv1.PingRequest
		if err := codec.Proto.Unmarshal(raw, &got); err != nil {
			t.Fatalf("binary decode with unknown field failed: %v", err)
		}
		if len(got.ProtoReflect().GetUnknown()) == 0 {
			t.Fatal("expected unknown field to be preserved in binary decode")
		}
	})
	t.Run("negative: ProtoJSON rejects an unknown field", func(t *testing.T) {
		err := codec.ProtoJSON.Unmarshal([]byte(`{"envelope":{"runId":"r"},"bogusField":1}`), &workloadsv1.PingRequest{})
		if err == nil {
			t.Fatal("expected ProtoJSON to reject an unknown field with DiscardUnknown=false")
		}
	})
}

// TestInvalidInput: malformed bytes/JSON return typed errors, not panics.
func TestInvalidInput(t *testing.T) {
	tests := []struct {
		description string
		c           codec.Codec
		in          []byte
	}{
		{"negative: proto rejects a truncated varint", codec.Proto, []byte{0xff, 0xff}},
		{"negative: vtproto rejects a truncated varint", codec.VT, []byte{0xff, 0xff}},
		{"negative: protojson rejects non-JSON", codec.ProtoJSON, []byte("not json")},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			if err := tc.c.Unmarshal(tc.in, &workloadsv1.PingRequest{}); err == nil {
				t.Fatalf("%s: expected an error", tc.c.Name())
			}
		})
	}
}

// TestByName / TestContentType cover the small lookup helpers.
func TestByName(t *testing.T) {
	tests := []struct {
		description string
		in          string
		wantName    string
		wantErr     bool
	}{
		{"positive: empty defaults to proto", "", "proto", false},
		{"positive: proto", "proto", "proto", false},
		{"positive: protojson", "protojson", "protojson", false},
		{"positive: vtproto", "vtproto", "vtproto", false},
		{"negative: unknown", "capnproto", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			c, err := codec.ByName(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil || c.Name() != tc.wantName {
				t.Fatalf("got (%v, %v), want name %q", c, err, tc.wantName)
			}
		})
	}
}

func nowTS() *timestamppb.Timestamp { return timestamppb.Now() }

func fieldMask(paths ...string) *fieldmaskpb.FieldMask { return &fieldmaskpb.FieldMask{Paths: paths} }
