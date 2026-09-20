package corpus_test

import (
	"strings"
	"testing"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/corpus"
)

// TestFixturesValidate asserts every valid fixture passes protovalidate — the
// invariant the demo's validate step and the correctness pass rely on.
func TestFixturesValidate(t *testing.T) {
	c := corpus.New(42)
	for _, f := range corpus.AllFixtures {
		t.Run(string(f), func(t *testing.T) {
			m, err := c.Message(f)
			if err != nil {
				t.Fatal(err)
			}
			if err := protovalidate.Validate(m); err != nil {
				t.Fatalf("fixture %s failed validation: %v", f, err)
			}
		})
	}
}

// TestTelemetryUsageValidate covers the one-way payload fixtures.
func TestTelemetryUsageValidate(t *testing.T) {
	c := corpus.New(42)
	tests := []struct {
		description string
		msg         proto.Message
	}{
		{"telemetry with 1 container", c.Telemetry(1)},
		{"telemetry with 4 containers", c.Telemetry(4)},
		{"telemetry with 16 containers", c.Telemetry(16)},
		{"usage with 1 reading", c.Usage(1)},
		{"usage with 5 readings", c.Usage(5)},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			if err := protovalidate.Validate(tc.msg); err != nil {
				t.Fatalf("validation failed: %v", err)
			}
		})
	}
}

// TestInvalidCases: each demo mutation fails validation with its named
// constraint (design demo step 3 / correctness pass §9.4).
func TestInvalidCases(t *testing.T) {
	for _, ic := range corpus.New(42).InvalidCases() {
		t.Run(ic.Description, func(t *testing.T) {
			err := protovalidate.Validate(ic.Message)
			if err == nil {
				t.Fatalf("expected a validation error for %q", ic.Description)
			}
			if !strings.Contains(err.Error(), ic.WantContains) {
				t.Fatalf("violation for %q missing %q:\n%v", ic.Description, ic.WantContains, err)
			}
		})
	}
}

// TestDeterminism: a given (seed, fixture) is reproducible regardless of call
// order, and different seeds differ.
func TestDeterminism(t *testing.T) {
	tests := []struct {
		description string
		fixture     corpus.Fixture
	}{
		{"tiny is reproducible", corpus.Tiny},
		{"medium is reproducible", corpus.Medium},
		{"large is reproducible", corpus.Large},
		{"dense is reproducible", corpus.Dense},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			a, _ := corpus.New(42).Message(tc.fixture)
			b, _ := corpus.New(42).Message(tc.fixture)
			if !proto.Equal(a, b) {
				t.Fatalf("same seed produced different %s messages", tc.fixture)
			}
			d, _ := corpus.New(99).Message(tc.fixture)
			if proto.Equal(a, d) {
				t.Fatalf("different seeds produced identical %s messages", tc.fixture)
			}
		})
	}
}

// TestSizes: sizes are positive and ordered as designed (tiny < small < medium
// < large, and max is ~960 KiB).
func TestSizes(t *testing.T) {
	sizes, err := corpus.New(42).Sizes()
	if err != nil {
		t.Fatal(err)
	}
	if !(sizes[corpus.Tiny] < sizes[corpus.Small] &&
		sizes[corpus.Small] < sizes[corpus.Medium] &&
		sizes[corpus.Medium] < sizes[corpus.Large]) {
		t.Fatalf("unexpected size ordering: %v", sizes)
	}
	if sizes[corpus.Max] < 900<<10 {
		t.Fatalf("max fixture too small: %d bytes", sizes[corpus.Max])
	}
}

// TestLogChunkBytes: the fan-out payload carries exactly the requested byte
// count, validates, and is deterministic per seed (the logs fan-out driver
// depends on all three — design §3.9).
func TestLogChunkBytes(t *testing.T) {
	tests := []struct {
		description string
		n           int
	}{
		{"1 byte boundary", 1},
		{"64 KiB", 64 << 10},
		{"256 KiB fan-out default", 256 << 10},
		{"960 KiB max-chunk equivalent", 960 << 10},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			a := corpus.New(42).LogChunkBytes(tc.n)
			if got := len(a.GetData()); got != tc.n {
				t.Fatalf("LogChunkBytes(%d) data = %d bytes, want exactly %d", tc.n, got, tc.n)
			}
			if err := protovalidate.Validate(a); err != nil {
				t.Fatalf("LogChunkBytes(%d) failed validation: %v", tc.n, err)
			}
			b := corpus.New(42).LogChunkBytes(tc.n)
			if !proto.Equal(a, b) {
				t.Fatalf("same seed produced different LogChunkBytes(%d)", tc.n)
			}
		})
	}
}
