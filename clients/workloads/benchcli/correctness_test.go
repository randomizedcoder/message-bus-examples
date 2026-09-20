package main

import (
	"strings"
	"testing"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/corpus"
)

func TestParseCodecs(t *testing.T) {
	tests := []struct {
		description string
		in          string
		wantNames   []string
		wantErr     bool
	}{
		{"all three (the default)", "proto,protojson,vtproto", []string{"proto", "protojson", "vtproto"}, false},
		{"single codec", "proto", []string{"proto"}, false},
		{"whitespace and empty entries are dropped", " proto , , vtproto ", []string{"proto", "vtproto"}, false},
		{"unknown codec is an error", "proto,bogus", nil, true},
		{"empty string is an error", "", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			got, err := parseCodecs(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var names []string
			for _, nc := range got {
				names = append(names, nc.name)
			}
			if strings.Join(names, ",") != strings.Join(tt.wantNames, ",") {
				t.Errorf("names = %v, want %v", names, tt.wantNames)
			}
		})
	}
}

func TestTransportEnum(t *testing.T) {
	tests := []struct {
		description string
		name        string
		wantOK      bool
	}{
		{"grpc is a request/reply transport", "grpc", true},
		{"nats is a request/reply transport", "nats", true},
		{"rabbitmq is a request/reply transport", "rabbitmq", true},
		{"valkey is a request/reply transport", "valkey", true},
		{"mqtt is one-way, no correctness enum here", "mqtt", false},
		{"unknown transport", "kafka", false},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			_, ok := transportEnum(tt.name)
			if ok != tt.wantOK {
				t.Errorf("transportEnum(%q) ok = %v, want %v", tt.name, ok, tt.wantOK)
			}
		})
	}
}

// TestInProcessCorrectnessAllPass runs the three cluster-free correctness layers
// over the real corpus and asserts every check passes — the same assertions the
// §9.4 gate makes before any transport is involved.
func TestInProcessCorrectnessAllPass(t *testing.T) {
	c := corpus.New(42)
	codecs, err := parseCodecs("proto,protojson,vtproto")
	if err != nil {
		t.Fatalf("parseCodecs: %v", err)
	}
	var all []check
	all = append(all, codecRoundTrip(c, codecs)...)
	all = append(all, corpusDeterminism(c)...)
	all = append(all, validationChecks(c)...)
	if len(all) == 0 {
		t.Fatal("no checks were produced")
	}
	for _, r := range all {
		if r.failed() {
			t.Errorf("[%s] %s failed: %s", r.group, r.name, r.detail)
		}
	}
	if err := reportChecks(all, ""); err != nil {
		t.Errorf("reportChecks over all-passing checks returned %v", err)
	}
}

// TestValidationChecksInvalidCarryConstraintIDs asserts each invalid-corpus
// mutation is reported as a pass (it failed validation) with its expected
// constraint id surfaced in the detail.
func TestValidationChecksInvalidCarryConstraintIDs(t *testing.T) {
	c := corpus.New(42)
	byName := map[string]check{}
	for _, r := range validationChecks(c) {
		if r.group == "validate-invalid" {
			byName[r.name] = r
		}
	}
	for _, ic := range c.InvalidCases() {
		r, ok := byName[ic.Description]
		if !ok {
			t.Errorf("no validate-invalid check for %q", ic.Description)
			continue
		}
		if r.failed() {
			t.Errorf("invalid case %q was not caught: %s", ic.Description, r.detail)
		}
		if !strings.Contains(r.detail, ic.WantContains) {
			t.Errorf("invalid case %q detail %q missing constraint id %q", ic.Description, r.detail, ic.WantContains)
		}
	}
}

func TestReportChecksFailsClosed(t *testing.T) {
	tests := []struct {
		description string
		checks      []check
		wantErr     bool
	}{
		{"all pass → nil", []check{pass("g", "a", ""), pass("g", "b", "")}, false},
		{"one fail → error", []check{pass("g", "a", ""), fail("g", "b", "boom")}, true},
		{"skip is not a failure → nil", []check{pass("g", "a", ""), skip("g", "b", "n/a")}, false},
		{"empty → nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			err := reportChecks(tt.checks, "")
			if (err != nil) != tt.wantErr {
				t.Errorf("reportChecks err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
