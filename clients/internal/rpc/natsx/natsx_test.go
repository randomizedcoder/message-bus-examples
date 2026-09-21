package natsx

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
)

func TestSubject(t *testing.T) {
	tests := []struct {
		description string
		service     string
		method      string
		expected    string
	}{
		{
			description: "typical service.method routes under the rpc prefix",
			service:     "customer",
			method:      "Lookup",
			expected:    "rpc.customer.Lookup",
		},
		{
			description: "different method yields a distinct subject (per-method balancing)",
			service:     "customer",
			method:      "Update",
			expected:    "rpc.customer.Update",
		},
		{
			description: "boundary: empty service and method still produce a well-formed subject",
			service:     "",
			method:      "",
			expected:    "rpc..",
		},
		{
			description: "corner: dotted service name nests further under the prefix",
			service:     "billing.v2",
			method:      "Charge",
			expected:    "rpc.billing.v2.Charge",
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := Subject(tt.service, tt.method); got != tt.expected {
				t.Fatalf("Subject(%q, %q) = %q, want %q", tt.service, tt.method, got, tt.expected)
			}
			if SubjectWildcard != "rpc.>" {
				t.Fatalf("SubjectWildcard = %q, want rpc.> so one subscription serves every subject", SubjectWildcard)
			}
		})
	}
}

func TestNatsURL(t *testing.T) {
	tests := []struct {
		description string
		addr        string
		expected    string
	}{
		{
			description: "bare host:port gets the nats scheme prepended",
			addr:        "10.33.33.10:30422",
			expected:    "nats://10.33.33.10:30422",
		},
		{
			description: "a full nats URL is passed through unchanged",
			addr:        "nats://nats.nats.svc:4222",
			expected:    "nats://nats.nats.svc:4222",
		},
		{
			description: "corner: a non-nats scheme is respected (not double-prefixed)",
			addr:        "tls://broker:4222",
			expected:    "tls://broker:4222",
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := natsURL(tt.addr); got != tt.expected {
				t.Fatalf("natsURL(%q) = %q, want %q", tt.addr, got, tt.expected)
			}
		})
	}
}

func TestMapErr(t *testing.T) {
	tests := []struct {
		description    string
		in             error
		expectStatus   rpcv1.Status
		expectSentinel error // errors.Is target the result must match, nil to skip
	}{
		{
			description:  "nil passes through as OK",
			in:           nil,
			expectStatus: rpcv1.Status_STATUS_OK,
		},
		{
			description:    "no-responders maps to ErrNoResponder → UNAVAILABLE (the §28 fast-fail)",
			in:             nats.ErrNoResponders,
			expectStatus:   rpcv1.Status_STATUS_UNAVAILABLE,
			expectSentinel: rpc.ErrNoResponder,
		},
		{
			description:    "wrapped no-responders is still recognised (errors.Is through the wrap)",
			in:             fmt.Errorf("request failed: %w", nats.ErrNoResponders),
			expectStatus:   rpcv1.Status_STATUS_UNAVAILABLE,
			expectSentinel: rpc.ErrNoResponder,
		},
		{
			description:    "nats timeout folds onto context.DeadlineExceeded → TIMEOUT",
			in:             nats.ErrTimeout,
			expectStatus:   rpcv1.Status_STATUS_TIMEOUT,
			expectSentinel: context.DeadlineExceeded,
		},
		{
			description:    "a ctx deadline the caller already carries passes through → TIMEOUT",
			in:             context.DeadlineExceeded,
			expectStatus:   rpcv1.Status_STATUS_TIMEOUT,
			expectSentinel: context.DeadlineExceeded,
		},
		{
			description:  "an unclassified error passes through → INTERNAL",
			in:           errors.New("connection reset"),
			expectStatus: rpcv1.Status_STATUS_INTERNAL,
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			got := mapErr(tt.in)
			if tt.in == nil {
				if got != nil {
					t.Fatalf("mapErr(nil) = %v, want nil", got)
				}
				return
			}
			if tt.expectSentinel != nil && !errors.Is(got, tt.expectSentinel) {
				t.Fatalf("mapErr(%v) = %v; errors.Is(_, %v) = false, want true", tt.in, got, tt.expectSentinel)
			}
			if s := rpc.StatusOf(got); s != tt.expectStatus {
				t.Fatalf("StatusOf(mapErr(%v)) = %s, want %s", tt.in, s, tt.expectStatus)
			}
		})
	}
}
