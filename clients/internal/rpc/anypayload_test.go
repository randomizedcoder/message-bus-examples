package rpc

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	benchmarkv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/benchmark/v1"
)

func TestPackUnpackRoundTrip(t *testing.T) {
	tests := []struct {
		description string
		msg         proto.Message
		wantTypeURL string
	}{
		{
			description: "customer lookup request round-trips",
			msg:         &benchmarkv1.CustomerLookupRequest{CustomerId: "11111111-1111-1111-1111-111111111111", Region: "us-west-2"},
			wantTypeURL: "type.googleapis.com/benchmark.v1.CustomerLookupRequest",
		},
		{
			description: "customer lookup response with repeated fields round-trips",
			msg: &benchmarkv1.CustomerLookupResponse{
				CustomerId: "22222222-2222-2222-2222-222222222222",
				Name:       "Ada",
				Email:      "ada@example.com",
				AccountIds: []string{"33333333-3333-3333-3333-333333333333"},
			},
			wantTypeURL: "type.googleapis.com/benchmark.v1.CustomerLookupResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			any, err := Pack(tt.msg)
			if err != nil {
				t.Fatalf("Pack: %v", err)
			}
			if any.GetTypeUrl() != tt.wantTypeURL {
				t.Fatalf("type_url = %q, want %q", any.GetTypeUrl(), tt.wantTypeURL)
			}

			// Unpack (registry resolution) yields an equal message.
			got, err := Unpack(any)
			if err != nil {
				t.Fatalf("Unpack: %v", err)
			}
			if !proto.Equal(got, tt.msg) {
				t.Fatalf("Unpack mismatch:\n got %v\nwant %v", got, tt.msg)
			}

			// UnpackInto the concrete type yields an equal message.
			into := tt.msg.ProtoReflect().New().Interface()
			if err := UnpackInto(any, into); err != nil {
				t.Fatalf("UnpackInto: %v", err)
			}
			if !proto.Equal(into, tt.msg) {
				t.Fatalf("UnpackInto mismatch:\n got %v\nwant %v", into, tt.msg)
			}
		})
	}
}

func TestPackUnpackErrors(t *testing.T) {
	tests := []struct {
		description string
		run         func() error
	}{
		{
			description: "Pack nil message errors",
			run:         func() error { _, err := Pack(nil); return err },
		},
		{
			description: "Unpack nil Any errors",
			run:         func() error { _, err := Unpack(nil); return err },
		},
		{
			description: "Unpack unknown type_url errors",
			run: func() error {
				_, err := Unpack(&anypb.Any{TypeUrl: "type.googleapis.com/does.not.Exist", Value: []byte{0x08, 0x01}})
				return err
			},
		},
		{
			description: "UnpackInto nil target errors",
			run: func() error {
				any, _ := Pack(&benchmarkv1.CustomerLookupRequest{CustomerId: "11111111-1111-1111-1111-111111111111", Region: "us-west-2"})
				return UnpackInto(any, nil)
			},
		},
		{
			description: "UnpackInto wrong target type errors",
			run: func() error {
				any, _ := Pack(&benchmarkv1.CustomerLookupRequest{CustomerId: "11111111-1111-1111-1111-111111111111", Region: "us-west-2"})
				return UnpackInto(any, &benchmarkv1.CustomerLookupResponse{})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if err := tt.run(); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
}
