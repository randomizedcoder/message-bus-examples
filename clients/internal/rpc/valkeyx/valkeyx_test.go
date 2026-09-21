package valkeyx

import (
	"reflect"
	"testing"
)

func TestRequestChannel(t *testing.T) {
	tests := []struct {
		description string
		service     string
		method      string
		expected    string
	}{
		{
			description: "positive: a service.method becomes a per-method request channel",
			service:     "customer",
			method:      "Lookup",
			expected:    "rpc:request:customer:Lookup",
		},
		{
			description: "boundary: empty service/method still yields the fixed shape the pattern matches",
			service:     "",
			method:      "",
			expected:    "rpc:request::",
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := RequestChannel(tt.service, tt.method); got != tt.expected {
				t.Fatalf("RequestChannel(%q, %q) = %q, want %q", tt.service, tt.method, got, tt.expected)
			}
		})
	}
}

func TestResponseChannel(t *testing.T) {
	tests := []struct {
		description string
		clientID    string
		expected    string
	}{
		{
			description: "positive: a client id becomes its private response channel",
			clientID:    "rpc-valkeyx-1234-1",
			expected:    "rpc:response:rpc-valkeyx-1234-1",
		},
		{
			description: "boundary: an empty client id yields a valid but unroutable channel (Call always stamps a real id)",
			clientID:    "",
			expected:    "rpc:response:",
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := ResponseChannel(tt.clientID); got != tt.expected {
				t.Fatalf("ResponseChannel(%q) = %q, want %q", tt.clientID, got, tt.expected)
			}
		})
	}
}

func TestRespStreamName(t *testing.T) {
	tests := []struct {
		description string
		clientID    string
		expected    string
	}{
		{
			description: "positive: a client id becomes its private reply stream key",
			clientID:    "rpc-valkeyx-stream-9-2",
			expected:    "rpc:stream:response:rpc-valkeyx-stream-9-2",
		},
		{
			description: "corner: the stream key prefix differs from the Pub/Sub reply channel prefix so keys and channels never overlap",
			clientID:    "x",
			expected:    "rpc:stream:response:x",
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := respStreamName(tt.clientID); got != tt.expected {
				t.Fatalf("respStreamName(%q) = %q, want %q", tt.clientID, got, tt.expected)
			}
		})
	}
}

func TestSplitSentinels(t *testing.T) {
	tests := []struct {
		description string
		addr        string
		expected    []string
	}{
		{
			description: "positive: a three-Sentinel comma list splits into three host:port entries",
			addr:        "10.33.33.10:30650,10.33.33.11:30651,10.33.33.12:30652",
			expected:    []string{"10.33.33.10:30650", "10.33.33.11:30651", "10.33.33.12:30652"},
		},
		{
			description: "boundary: a single Sentinel yields a one-element slice",
			addr:        "10.33.33.10:30650",
			expected:    []string{"10.33.33.10:30650"},
		},
		{
			description: "corner: surrounding spaces and a trailing comma are trimmed and dropped",
			addr:        " a:1 , b:2 ,",
			expected:    []string{"a:1", "b:2"},
		},
		{
			description: "negative: an all-empty/whitespace list yields an empty slice, not a slice of empties",
			addr:        " , ",
			expected:    []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			got := splitSentinels(tt.addr)
			if !reflect.DeepEqual(got, tt.expected) {
				t.Fatalf("splitSentinels(%q) = %#v, want %#v", tt.addr, got, tt.expected)
			}
		})
	}
}

func TestNewClientIDUnique(t *testing.T) {
	// Two ids minted in the same process must differ so their reply channels do
	// not cross-wire — the atomic sequence guarantees this even at equal pids.
	a := newClientID("rpc-valkeyx")
	b := newClientID("rpc-valkeyx")
	if a == b {
		t.Fatalf("newClientID returned duplicate ids %q and %q", a, b)
	}
}
