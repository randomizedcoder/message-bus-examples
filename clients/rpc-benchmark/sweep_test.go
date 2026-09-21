package main

import (
	"reflect"
	"testing"
	"time"
)

func TestParseSize(t *testing.T) {
	tests := []struct {
		description string
		in          string
		want        int
		wantErr     bool
	}{
		{description: "bare byte count", in: "100", want: 100},
		{description: "explicit B suffix", in: "100B", want: 100},
		{description: "KiB suffix", in: "1KiB", want: 1024},
		{description: "10 KiB", in: "10KiB", want: 10 * 1024},
		{description: "100 KiB", in: "100KiB", want: 100 * 1024},
		{description: "MiB suffix", in: "1MiB", want: 1024 * 1024},
		{description: "case-insensitive suffix", in: "1mib", want: 1024 * 1024},
		{description: "surrounding whitespace tolerated", in: "  2KiB ", want: 2 * 1024},
		{description: "zero is allowed (empty payload)", in: "0", want: 0},
		{description: "non-numeric is rejected", in: "big", wantErr: true},
		{description: "negative is rejected", in: "-1", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			got, err := parseSize(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseSize(%q) err = %v, wantErr = %v", tt.in, err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Errorf("parseSize(%q) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

func TestParseSizes(t *testing.T) {
	tests := []struct {
		description string
		in          string
		want        []int
		wantErr     bool
	}{
		{description: "the default ladder keyword expands to §26 sizes", in: "default", want: defaultLadder},
		{description: "default is case-insensitive", in: "DEFAULT", want: defaultLadder},
		{description: "explicit comma list in order", in: "100B,1KiB,1MiB", want: []int{100, 1024, 1024 * 1024}},
		{description: "empty entries are skipped", in: "100B,,1KiB", want: []int{100, 1024}},
		{description: "a bad entry fails the whole list", in: "1KiB,huge", wantErr: true},
		{description: "no usable sizes is an error", in: ",,", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			got, err := parseSizes(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseSizes(%q) err = %v, wantErr = %v", tt.in, err, tt.wantErr)
			}
			if err == nil && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseSizes(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestHumanSize(t *testing.T) {
	tests := []struct {
		description string
		in          int
		want        string
	}{
		{description: "sub-KiB stays in bytes", in: 100, want: "100B"},
		{description: "exact KiB", in: 1024, want: "1KiB"},
		{description: "100 KiB", in: 100 * 1024, want: "100KiB"},
		{description: "exact MiB prefers MiB over KiB", in: 1024 * 1024, want: "1MiB"},
		{description: "non-multiple falls back to bytes", in: 1500, want: "1500B"},
		{description: "zero is bytes", in: 0, want: "0B"},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := humanSize(tt.in); got != tt.want {
				t.Errorf("humanSize(%d) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestBuildWorkloadsIdempotencyKey(t *testing.T) {
	tests := []struct {
		description string
		idemKey     string
		wantKey     string // the idempotency_key every minted request should carry
	}{
		{description: "no key means each call is a distinct logical operation", idemKey: "", wantKey: ""},
		{description: "a key is stamped on every request so the run is one logical operation", idemKey: "create-order-123", wantKey: "create-order-123"},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			wls, err := buildWorkloads("1KiB", "cust", "us-west-2", time.Second, tt.idemKey)
			if err != nil {
				t.Fatalf("buildWorkloads: %v", err)
			}
			// Two independently minted requests must both carry the key (or both not),
			// and each must still get its own request_id.
			r1, r2 := wls[0].newReq(), wls[0].newReq()
			if r1.GetIdempotencyKey() != tt.wantKey || r2.GetIdempotencyKey() != tt.wantKey {
				t.Errorf("idempotency_key = %q,%q, want %q", r1.GetIdempotencyKey(), r2.GetIdempotencyKey(), tt.wantKey)
			}
			if r1.GetRequestId() == r2.GetRequestId() {
				t.Errorf("both requests share request_id %q; each attempt must be distinct", r1.GetRequestId())
			}
		})
	}
}

func TestBuildWorkloads(t *testing.T) {
	tests := []struct {
		description string
		sizes       string
		wantN       int
		wantService string
		wantFixture string // fixture of the first cell
		wantErr     bool
	}{
		{description: "empty sizes is the single representative customer.Lookup", sizes: "", wantN: 1, wantService: "customer", wantFixture: "customer"},
		{description: "the default ladder is five echo cells", sizes: "default", wantN: 5, wantService: "echo", wantFixture: "100B"},
		{description: "an explicit list keeps its order", sizes: "1KiB,1MiB", wantN: 2, wantService: "echo", wantFixture: "1KiB"},
		{description: "a bad size fails", sizes: "nope", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			wls, err := buildWorkloads(tt.sizes, "cust", "us-west-2", time.Second, "")
			if (err != nil) != tt.wantErr {
				t.Fatalf("buildWorkloads err = %v, wantErr = %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if len(wls) != tt.wantN {
				t.Fatalf("got %d workloads, want %d", len(wls), tt.wantN)
			}
			if wls[0].service != tt.wantService {
				t.Errorf("first service = %q, want %q", wls[0].service, tt.wantService)
			}
			if wls[0].fixture != tt.wantFixture {
				t.Errorf("first fixture = %q, want %q", wls[0].fixture, tt.wantFixture)
			}
			// The factory must build a valid, non-nil request.
			if req := wls[0].newReq(); req == nil || req.GetRequestId() == "" {
				t.Errorf("newReq produced an invalid request: %v", req)
			}
		})
	}
}
