package mqttx

import "testing"

func TestBrokerURL(t *testing.T) {
	tests := []struct {
		description string
		addr        string
		expected    string
	}{
		{
			description: "bare host:port gets the default tcp:// scheme (a NodePort address)",
			addr:        "10.33.33.10:30883",
			expected:    "tcp://10.33.33.10:30883",
		},
		{
			description: "a full tcp:// URL passes through unchanged",
			addr:        "tcp://mqtt.mqtt.svc:1883",
			expected:    "tcp://mqtt.mqtt.svc:1883",
		},
		{
			description: "corner: an ssl:// scheme is respected, not double-prefixed",
			addr:        "ssl://broker:8883",
			expected:    "ssl://broker:8883",
		},
		{
			description: "boundary: bare host with no port still gets a tcp:// prefix",
			addr:        "localhost",
			expected:    "tcp://localhost",
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := brokerURL(tt.addr); got != tt.expected {
				t.Fatalf("brokerURL(%q) = %q, want %q", tt.addr, got, tt.expected)
			}
		})
	}
}

func TestRequestTopic(t *testing.T) {
	tests := []struct {
		description string
		service     string
		method      string
		expected    string
	}{
		{
			description: "positive: a service.method becomes a per-method request topic",
			service:     "customer",
			method:      "Lookup",
			expected:    "rpc/request/customer/Lookup",
		},
		{
			description: "boundary: empty service/method still yields the fixed two-level shape the wildcard matches",
			service:     "",
			method:      "",
			expected:    "rpc/request//",
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := RequestTopic(tt.service, tt.method); got != tt.expected {
				t.Fatalf("RequestTopic(%q, %q) = %q, want %q", tt.service, tt.method, got, tt.expected)
			}
		})
	}
}

func TestResponseTopic(t *testing.T) {
	tests := []struct {
		description string
		clientID    string
		expected    string
	}{
		{
			description: "positive: a client id becomes its private response topic",
			clientID:    "rpc-mqttx-1234-1",
			expected:    "rpc/response/rpc-mqttx-1234-1",
		},
		{
			description: "boundary: an empty client id yields a syntactically valid but unroutable topic (Call always stamps a real id)",
			clientID:    "",
			expected:    "rpc/response/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := ResponseTopic(tt.clientID); got != tt.expected {
				t.Fatalf("ResponseTopic(%q) = %q, want %q", tt.clientID, got, tt.expected)
			}
		})
	}
}

// TestResponseTopicMatchesRequestWildcard is a corner guard that the request
// topic a client publishes to is matched by the wildcard the responder
// subscribes to: both are exactly two levels under rpc/request.
func TestResponseTopicMatchesRequestWildcard(t *testing.T) {
	// reqWildcard is rpc/request/+/+ — a RequestTopic must have exactly the
	// prefix plus two more segments for the responder to receive it.
	topic := RequestTopic("svc", "Method")
	const wantPrefix = reqTopicPrefix + "/"
	rest, ok := cutPrefix(topic, wantPrefix)
	if !ok {
		t.Fatalf("RequestTopic %q does not start with %q", topic, wantPrefix)
	}
	if segs := countSegments(rest); segs != 2 {
		t.Fatalf("RequestTopic tail %q has %d segments, want 2 to match %q", rest, segs, reqWildcard)
	}
}

func cutPrefix(s, prefix string) (string, bool) {
	if len(s) < len(prefix) || s[:len(prefix)] != prefix {
		return s, false
	}
	return s[len(prefix):], true
}

func countSegments(s string) int {
	n := 1
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			n++
		}
	}
	return n
}
