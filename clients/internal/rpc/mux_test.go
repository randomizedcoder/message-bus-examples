package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	benchmarkv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/benchmark/v1"
	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
)

const (
	goodUUID = "11111111-1111-1111-1111-111111111111"
	goodReg  = "us-west-2"

	testTimeout = 5 * time.Second
)

// lookupMux builds a Mux with customer.Lookup registered. The handler echoes the
// id, or returns errRet when it is non-nil, so error classification is testable.
func lookupMux(t *testing.T, validate bool, errRet error) *Mux {
	t.Helper()
	m := NewMux(validate)
	m.Handle("customer", "Lookup",
		func() proto.Message { return &benchmarkv1.CustomerLookupRequest{} },
		func(_ context.Context, in proto.Message) (proto.Message, error) {
			if errRet != nil {
				return nil, errRet
			}
			r := in.(*benchmarkv1.CustomerLookupRequest)
			return &benchmarkv1.CustomerLookupResponse{CustomerId: r.GetCustomerId()}, nil
		},
	)
	return m
}

// mustReq builds a routed Request for service.method around payload.
func mustReq(t *testing.T, service, method string, payload proto.Message) *rpcv1.Request {
	t.Helper()
	req, err := NewRequest(service, method, payload, testTimeout)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	return req
}

func TestMuxDispatch(t *testing.T) {
	tests := []struct {
		description string
		validate    bool
		handlerErr  error
		req         func(t *testing.T) *rpcv1.Request
		expect      rpcv1.Status
	}{
		{
			description: "registered method with valid payload returns OK",
			validate:    true,
			req: func(t *testing.T) *rpcv1.Request {
				return mustReq(t, "customer", "Lookup", &benchmarkv1.CustomerLookupRequest{CustomerId: goodUUID, Region: goodReg})
			},
			expect: rpcv1.Status_STATUS_OK,
		},
		{
			description: "unknown service.method is NOT_FOUND",
			validate:    true,
			req: func(t *testing.T) *rpcv1.Request {
				return mustReq(t, "customer", "Delete", &benchmarkv1.CustomerLookupRequest{CustomerId: goodUUID, Region: goodReg})
			},
			expect: rpcv1.Status_STATUS_NOT_FOUND,
		},
		{
			description: "payload of the wrong concrete type fails to decode as INVALID_ARGUMENT",
			validate:    true,
			req: func(t *testing.T) *rpcv1.Request {
				// A CustomerLookupResponse packed where a Request is expected.
				return mustReq(t, "customer", "Lookup", &benchmarkv1.CustomerLookupResponse{CustomerId: goodUUID})
			},
			expect: rpcv1.Status_STATUS_INVALID_ARGUMENT,
		},
		{
			description: "validation on rejects a bad region as INVALID_ARGUMENT",
			validate:    true,
			req: func(t *testing.T) *rpcv1.Request {
				return mustReq(t, "customer", "Lookup", &benchmarkv1.CustomerLookupRequest{CustomerId: goodUUID, Region: "nope!"})
			},
			expect: rpcv1.Status_STATUS_INVALID_ARGUMENT,
		},
		{
			description: "validation off lets the same bad region through to the handler (OK)",
			validate:    false,
			req: func(t *testing.T) *rpcv1.Request {
				return mustReq(t, "customer", "Lookup", &benchmarkv1.CustomerLookupRequest{CustomerId: goodUUID, Region: "nope!"})
			},
			expect: rpcv1.Status_STATUS_OK,
		},
		{
			description: "handler *Error is surfaced with its own Status",
			validate:    true,
			handlerErr:  Errorf(rpcv1.Status_STATUS_UNAVAILABLE, "backend down"),
			req: func(t *testing.T) *rpcv1.Request {
				return mustReq(t, "customer", "Lookup", &benchmarkv1.CustomerLookupRequest{CustomerId: goodUUID, Region: goodReg})
			},
			expect: rpcv1.Status_STATUS_UNAVAILABLE,
		},
		{
			description: "handler sentinel ErrNotFound classifies to NOT_FOUND",
			validate:    true,
			handlerErr:  ErrNotFound,
			req: func(t *testing.T) *rpcv1.Request {
				return mustReq(t, "customer", "Lookup", &benchmarkv1.CustomerLookupRequest{CustomerId: goodUUID, Region: goodReg})
			},
			expect: rpcv1.Status_STATUS_NOT_FOUND,
		},
		{
			description: "opaque handler error classifies to INTERNAL",
			validate:    true,
			handlerErr:  errors.New("boom"),
			req: func(t *testing.T) *rpcv1.Request {
				return mustReq(t, "customer", "Lookup", &benchmarkv1.CustomerLookupRequest{CustomerId: goodUUID, Region: goodReg})
			},
			expect: rpcv1.Status_STATUS_INTERNAL,
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			m := lookupMux(t, tt.validate, tt.handlerErr)
			resp := m.Dispatch(context.Background(), tt.req(t))
			if resp.GetStatus() != tt.expect {
				t.Fatalf("status = %s, want %s (err=%q)", resp.GetStatus(), tt.expect, resp.GetErrorMessage())
			}
			// Identity is always echoed.
			if resp.GetRequestId() == "" {
				t.Errorf("response request_id is empty")
			}
			// The server timeline is stamped on every dispatch (§20).
			if resp.GetServerReceivedAt() == nil || resp.GetServerSentAt() == nil {
				t.Errorf("server_received_at/server_sent_at not stamped")
			}
			// On OK the payload must be present and decode; on non-OK it is absent.
			if tt.expect == rpcv1.Status_STATUS_OK {
				if resp.GetPayload() == nil {
					t.Fatalf("OK response has no payload")
				}
				out := &benchmarkv1.CustomerLookupResponse{}
				if err := UnpackInto(resp.GetPayload(), out); err != nil {
					t.Fatalf("decode OK payload: %v", err)
				}
				if resp.GetGatewayBInvoke() == nil || resp.GetServiceResp() == nil {
					t.Errorf("OK response missing invoke/service_resp stamps")
				}
			} else if resp.GetPayload() != nil {
				t.Errorf("non-OK response carries a payload")
			}
		})
	}
}

func TestMuxHandleDuplicatePanics(t *testing.T) {
	m := NewMux(false)
	newReq := func() proto.Message { return &benchmarkv1.CustomerLookupRequest{} }
	h := func(context.Context, proto.Message) (proto.Message, error) { return nil, nil }
	m.Handle("customer", "Lookup", newReq, h)

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on duplicate route, got none")
		}
	}()
	m.Handle("customer", "Lookup", newReq, h)
}
