package natstransport

import (
	"strings"
	"testing"
)

func TestLogsStreamName(t *testing.T) {
	if LogsStreamName != "WL_LOGS" {
		t.Errorf("LogsStreamName = %q, want %q", LogsStreamName, "WL_LOGS")
	}
}

func TestLogsSubject(t *testing.T) {
	tests := []struct {
		description string
		region      string
		workloadID  string
		expected    string
	}{
		{"typical region + workload", "us-west-2", "wl-abc", "wl.us-west-2.logs.wl-abc"},
		{"another region + id", "ap-south-1", "wl-42", "wl.ap-south-1.logs.wl-42"},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			if got := LogsSubject(tc.region, tc.workloadID); got != tc.expected {
				t.Errorf("LogsSubject(%q, %q) = %q, want %q", tc.region, tc.workloadID, got, tc.expected)
			}
		})
	}
}

// TestLogsSubjectTokenShape: a LogsSubject must have exactly four tokens
// (wl / region / logs / workload_id) so it is a subset of the stream's
// `wl.*.logs.*` filter — the two single-token wildcards bind region and id.
func TestLogsSubjectTokenShape(t *testing.T) {
	tests := []struct {
		description string
		region      string
		workloadID  string
		wantThird   string // the fixed literal segment
	}{
		{"region + id are single tokens", "us-west-2", "wl-abc", "logs"},
		{"numeric-suffixed id", "eu-west-1", "wl-7", "logs"},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			parts := strings.Split(LogsSubject(tc.region, tc.workloadID), ".")
			if len(parts) != 4 {
				t.Fatalf("subject %q has %d tokens, want 4 (wl.region.logs.id)", LogsSubject(tc.region, tc.workloadID), len(parts))
			}
			if parts[0] != "wl" || parts[2] != tc.wantThird {
				t.Errorf("subject tokens = %v, want [wl <region> %s <id>]", parts, tc.wantThird)
			}
		})
	}
}
