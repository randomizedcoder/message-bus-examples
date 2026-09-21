package rpc

import (
	"context"
	"errors"
	"fmt"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
)

// Sentinel errors that transports and handlers return; StatusOf maps them (and
// the standard context errors) onto the wire Status enum (§4). Use errors.Is to
// test them and wrap with fmt.Errorf("...: %w", ...) to add context.
var (
	// ErrNoResponder means no server was listening — NATS core reports this
	// natively without waiting for the timeout (§28), and gateway-B being
	// unreachable maps here too.
	ErrNoResponder = errors.New("rpc: no responder")
	// ErrUnavailable is a generic transient unavailability (broker down,
	// connection dropped mid-flight).
	ErrUnavailable = errors.New("rpc: unavailable")
	// ErrNotFound means the routed service/method has no registered handler.
	ErrNotFound = errors.New("rpc: service or method not found")
	// ErrInvalidArgument means the request failed validation.
	ErrInvalidArgument = errors.New("rpc: invalid argument")
)

// Error carries a Status alongside a message so a handler can set the outcome
// precisely; Response.status and error_message are filled from it.
type Error struct {
	Status  rpcv1.Status
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("rpc: %s: %s", e.Status, e.Message) }

// Errorf builds an *Error with the given Status and formatted message.
func Errorf(s rpcv1.Status, format string, a ...any) *Error {
	return &Error{Status: s, Message: fmt.Sprintf(format, a...)}
}

// StatusOf classifies err into a wire Status. A nil error is STATUS_OK. An
// explicit *Error wins; otherwise the standard context errors and the package
// sentinels are recognised, and anything else is STATUS_INTERNAL. context.Canceled
// has no dedicated enum value, so it maps to STATUS_UNAVAILABLE (the caller went
// away; the operation did not complete).
func StatusOf(err error) rpcv1.Status {
	if err == nil {
		return rpcv1.Status_STATUS_OK
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Status
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return rpcv1.Status_STATUS_TIMEOUT
	case errors.Is(err, context.Canceled):
		return rpcv1.Status_STATUS_UNAVAILABLE
	case errors.Is(err, ErrNoResponder), errors.Is(err, ErrUnavailable):
		return rpcv1.Status_STATUS_UNAVAILABLE
	case errors.Is(err, ErrNotFound):
		return rpcv1.Status_STATUS_NOT_FOUND
	case errors.Is(err, ErrInvalidArgument):
		return rpcv1.Status_STATUS_INVALID_ARGUMENT
	default:
		return rpcv1.Status_STATUS_INTERNAL
	}
}
