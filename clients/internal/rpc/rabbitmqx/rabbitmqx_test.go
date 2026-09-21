package rabbitmqx

import "testing"

func TestURL(t *testing.T) {
	tests := []struct {
		description string
		addr        string
		user        string
		pass        string
		expected    string
	}{
		{
			description: "bare host:port is woven into an amqp URL with credentials and the default vhost",
			addr:        "10.33.33.10:30567",
			user:        "admin",
			pass:        "secret",
			expected:    "amqp://admin:secret@10.33.33.10:30567/",
		},
		{
			description: "a full amqp URL passes through unchanged (credentials already embedded)",
			addr:        "amqp://admin:secret@rabbitmq.rabbitmq.svc:5672/",
			user:        "ignored",
			pass:        "ignored",
			expected:    "amqp://admin:secret@rabbitmq.rabbitmq.svc:5672/",
		},
		{
			description: "corner: an amqps:// scheme is respected, not double-prefixed",
			addr:        "amqps://broker:5671/",
			user:        "u",
			pass:        "p",
			expected:    "amqps://broker:5671/",
		},
		{
			description: "boundary: empty user/pass still yield a syntactically valid URL",
			addr:        "host:5672",
			user:        "",
			pass:        "",
			expected:    "amqp://:@host:5672/",
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := URL(tt.addr, tt.user, tt.pass); got != tt.expected {
				t.Fatalf("URL(%q, %q, %q) = %q, want %q", tt.addr, tt.user, tt.pass, got, tt.expected)
			}
		})
	}
}

func TestModeDistinct(t *testing.T) {
	// The two reply destinations must be distinct modes so a benchmark can compare
	// them (§12); a trivial guard that the constants did not collapse.
	if ModeReplyQueue == ModeDirect {
		t.Fatalf("ModeReplyQueue and ModeDirect must be distinct")
	}
}
