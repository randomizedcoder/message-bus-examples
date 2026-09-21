package rpc

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
)

// NewRequest builds a routing Request: a fresh request_id, client_sent_at
// stamped now (T0, §20), the timeout, and payload packed as an Any (§5). It does
// not enforce the schema bounds itself — protovalidate does that on the wire —
// but it does reject the two mistakes that produce a request that can never
// validate: a nil payload and a non-positive timeout.
func NewRequest(service, method string, payload proto.Message, timeout time.Duration) (*rpcv1.Request, error) {
	if timeout <= 0 {
		return nil, fmt.Errorf("rpc: NewRequest: timeout must be > 0, got %s", timeout)
	}
	any, err := Pack(payload)
	if err != nil {
		return nil, err
	}
	return &rpcv1.Request{
		RequestId:    uuid.NewString(),
		Service:      service,
		Method:       method,
		ClientSentAt: timestamppb.Now(),
		Timeout:      durationpb.New(timeout),
		Payload:      any,
	}, nil
}

// NewResponse builds an OK Response echoing the request's identity with payload
// packed as an Any. Use ErrorResponse for the failure path.
func NewResponse(req *rpcv1.Request, payload proto.Message) (*rpcv1.Response, error) {
	any, err := Pack(payload)
	if err != nil {
		return nil, err
	}
	return &rpcv1.Response{
		RequestId:    req.GetRequestId(),
		Service:      req.GetService(),
		Method:       req.GetMethod(),
		ClientSentAt: req.GetClientSentAt(),
		Status:       rpcv1.Status_STATUS_OK,
		Payload:      any,
	}, nil
}

// ErrorResponse builds a non-OK Response for req from err, classified via
// StatusOf. The payload is left empty (the schema requires a payload only when
// status is OK).
func ErrorResponse(req *rpcv1.Request, err error) *rpcv1.Response {
	return &rpcv1.Response{
		RequestId:    req.GetRequestId(),
		Service:      req.GetService(),
		Method:       req.GetMethod(),
		ClientSentAt: req.GetClientSentAt(),
		Status:       StatusOf(err),
		ErrorMessage: err.Error(),
	}
}
