package rpc

import (
	"testing"
	"time"

	"github.com/google/uuid"

	benchmarkv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/benchmark/v1"
	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
)

func samplePayload() *benchmarkv1.CustomerLookupRequest {
	return &benchmarkv1.CustomerLookupRequest{CustomerId: "11111111-1111-1111-1111-111111111111", Region: "us-west-2"}
}

func TestNewRequest(t *testing.T) {
	tests := []struct {
		description string
		service     string
		method      string
		payload     interface{ Reset() }
		timeout     time.Duration
		wantErr     bool
	}{
		{"valid request builds", "customer", "Lookup", samplePayload(), 5 * time.Second, false},
		{"nil payload errors", "customer", "Lookup", nil, 5 * time.Second, true},
		{"zero timeout errors (boundary)", "customer", "Lookup", samplePayload(), 0, true},
		{"negative timeout errors", "customer", "Lookup", samplePayload(), -time.Second, true},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			var req *rpcv1.Request
			var err error
			if tt.payload == nil {
				req, err = NewRequest(tt.service, tt.method, nil, tt.timeout)
			} else {
				req, err = NewRequest(tt.service, tt.method, samplePayload(), tt.timeout)
			}
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if _, uerr := uuid.Parse(req.GetRequestId()); uerr != nil {
				t.Fatalf("request_id %q is not a UUID: %v", req.GetRequestId(), uerr)
			}
			if req.GetService() != tt.service || req.GetMethod() != tt.method {
				t.Fatalf("service/method = %q/%q, want %q/%q", req.GetService(), req.GetMethod(), tt.service, tt.method)
			}
			if req.GetClientSentAt() == nil {
				t.Fatal("client_sent_at not stamped")
			}
			if req.GetTimeout().AsDuration() != tt.timeout {
				t.Fatalf("timeout = %s, want %s", req.GetTimeout().AsDuration(), tt.timeout)
			}
			if got, want := req.GetPayload().GetTypeUrl(), "type.googleapis.com/benchmark.v1.CustomerLookupRequest"; got != want {
				t.Fatalf("payload type_url = %q, want %q", got, want)
			}
		})
	}
}

func TestNewResponseOK(t *testing.T) {
	req, err := NewRequest("customer", "Lookup", samplePayload(), time.Second)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := NewResponse(req, &benchmarkv1.CustomerLookupResponse{CustomerId: "11111111-1111-1111-1111-111111111111", Name: "Ada", Email: "ada@example.com"})
	if err != nil {
		t.Fatalf("NewResponse: %v", err)
	}
	if resp.GetStatus() != rpcv1.Status_STATUS_OK {
		t.Fatalf("status = %v, want OK", resp.GetStatus())
	}
	if resp.GetRequestId() != req.GetRequestId() || resp.GetService() != "customer" || resp.GetMethod() != "Lookup" {
		t.Fatal("response did not echo request identity")
	}
	if resp.GetPayload() == nil {
		t.Fatal("OK response must carry a payload")
	}
}

func TestErrorResponse(t *testing.T) {
	req, err := NewRequest("customer", "Lookup", samplePayload(), time.Second)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp := ErrorResponse(req, ErrNotFound)
	if resp.GetStatus() != rpcv1.Status_STATUS_NOT_FOUND {
		t.Fatalf("status = %v, want NOT_FOUND", resp.GetStatus())
	}
	if resp.GetErrorMessage() == "" {
		t.Fatal("error_message should be set")
	}
	if resp.GetPayload() != nil {
		t.Fatal("error response must not carry a payload")
	}
	if resp.GetRequestId() != req.GetRequestId() {
		t.Fatal("error response did not echo request_id")
	}
}
