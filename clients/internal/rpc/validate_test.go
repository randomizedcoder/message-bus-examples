package rpc

import (
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
)

// validRequest returns a Request that satisfies every protovalidate rule; tests
// mutate one field at a time to prove each rule (§7).
func validRequest(t *testing.T) *rpcv1.Request {
	t.Helper()
	req, err := NewRequest("customer", "Lookup", samplePayload(), 5*time.Second)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	return req
}

func TestValidateRequest(t *testing.T) {
	tests := []struct {
		description string
		mutate      func(r *rpcv1.Request)
		wantValid   bool
	}{
		{"canonical request is valid", func(r *rpcv1.Request) {}, true},
		{"non-uuid request_id is invalid", func(r *rpcv1.Request) { r.RequestId = "not-a-uuid" }, false},
		{"empty service is invalid", func(r *rpcv1.Request) { r.Service = "" }, false},
		{"uppercase service violates pattern", func(r *rpcv1.Request) { r.Service = "Customer" }, false},
		{"empty method is invalid", func(r *rpcv1.Request) { r.Method = "" }, false},
		{"missing payload is invalid", func(r *rpcv1.Request) { r.Payload = nil }, false},
		{"missing timeout is invalid", func(r *rpcv1.Request) { r.Timeout = nil }, false},
		{"zero timeout is invalid (boundary)", func(r *rpcv1.Request) { r.Timeout = durationpb.New(0) }, false},
		{"timeout over 300s is invalid (boundary)", func(r *rpcv1.Request) { r.Timeout = durationpb.New(301 * time.Second) }, false},
		{"timeout exactly 300s is valid (boundary)", func(r *rpcv1.Request) { r.Timeout = durationpb.New(300 * time.Second) }, true},
		{"missing client_sent_at is invalid", func(r *rpcv1.Request) { r.ClientSentAt = nil }, false},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			req := validRequest(t)
			tt.mutate(req)
			err := protovalidate.Validate(req)
			if tt.wantValid && err != nil {
				t.Fatalf("expected valid, got: %v", err)
			}
			if !tt.wantValid && err == nil {
				t.Fatal("expected a validation error, got nil")
			}
		})
	}
}

func TestValidateResponse(t *testing.T) {
	okPayload, err := Pack(&rpcv1.Request{}) // any registered message works as a payload here
	if err != nil {
		t.Fatalf("Pack: %v", err)
	}
	base := func() *rpcv1.Response {
		return &rpcv1.Response{
			RequestId:    "11111111-1111-1111-1111-111111111111",
			Status:       rpcv1.Status_STATUS_OK,
			Payload:      okPayload,
			ClientSentAt: timestamppb.Now(),
		}
	}

	tests := []struct {
		description string
		mutate      func(r *rpcv1.Response)
		wantValid   bool
	}{
		{"OK response with payload is valid", func(r *rpcv1.Response) {}, true},
		{"OK response without payload violates CEL", func(r *rpcv1.Response) { r.Payload = nil }, false},
		{"error response without payload is valid", func(r *rpcv1.Response) {
			r.Status = rpcv1.Status_STATUS_NOT_FOUND
			r.Payload = nil
		}, true},
		{"unspecified status is invalid", func(r *rpcv1.Response) { r.Status = rpcv1.Status_STATUS_UNSPECIFIED }, false},
		{"non-uuid request_id is invalid", func(r *rpcv1.Response) { r.RequestId = "x" }, false},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			resp := base()
			tt.mutate(resp)
			verr := protovalidate.Validate(resp)
			if tt.wantValid && verr != nil {
				t.Fatalf("expected valid, got: %v", verr)
			}
			if !tt.wantValid && verr == nil {
				t.Fatal("expected a validation error, got nil")
			}
		})
	}
}
