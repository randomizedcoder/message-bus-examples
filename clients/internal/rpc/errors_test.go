package rpc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
)

func TestStatusOf(t *testing.T) {
	tests := []struct {
		description string
		err         error
		want        rpcv1.Status
	}{
		{"nil error is OK", nil, rpcv1.Status_STATUS_OK},
		{"deadline exceeded maps to TIMEOUT", context.DeadlineExceeded, rpcv1.Status_STATUS_TIMEOUT},
		{"canceled maps to UNAVAILABLE", context.Canceled, rpcv1.Status_STATUS_UNAVAILABLE},
		{"no responder maps to UNAVAILABLE", ErrNoResponder, rpcv1.Status_STATUS_UNAVAILABLE},
		{"unavailable maps to UNAVAILABLE", ErrUnavailable, rpcv1.Status_STATUS_UNAVAILABLE},
		{"not found maps to NOT_FOUND", ErrNotFound, rpcv1.Status_STATUS_NOT_FOUND},
		{"invalid argument maps to INVALID_ARGUMENT", ErrInvalidArgument, rpcv1.Status_STATUS_INVALID_ARGUMENT},
		{"wrapped sentinel is still recognised", fmt.Errorf("routing: %w", ErrNotFound), rpcv1.Status_STATUS_NOT_FOUND},
		{"explicit *Error wins", Errorf(rpcv1.Status_STATUS_INVALID_ARGUMENT, "bad %s", "field"), rpcv1.Status_STATUS_INVALID_ARGUMENT},
		{"wrapped *Error is unwrapped", fmt.Errorf("ctx: %w", Errorf(rpcv1.Status_STATUS_NOT_FOUND, "x")), rpcv1.Status_STATUS_NOT_FOUND},
		{"unknown error is INTERNAL", errors.New("boom"), rpcv1.Status_STATUS_INTERNAL},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := StatusOf(tt.err); got != tt.want {
				t.Fatalf("StatusOf(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestErrorFormatsStatusAndMessage(t *testing.T) {
	e := Errorf(rpcv1.Status_STATUS_TIMEOUT, "deadline after %dms", 500)
	if e.Status != rpcv1.Status_STATUS_TIMEOUT {
		t.Fatalf("Status = %v, want TIMEOUT", e.Status)
	}
	if got, want := e.Error(), "rpc: STATUS_TIMEOUT: deadline after 500ms"; got != want {
		t.Fatalf("Error() = %q, want %q", got, want)
	}
}
