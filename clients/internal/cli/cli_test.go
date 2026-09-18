package cli

import (
	"errors"
	"flag"
	"fmt"
	"testing"
	"time"
)

// closed reports whether a done-channel has been closed, without blocking.
func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestParseRate(t *testing.T) {
	tests := []struct {
		description string
		input       string
		expected    time.Duration
		wantErr     bool
	}{
		{"empty means no throttle", "", 0, false},
		{"bare zero means no throttle", "0", 0, false},
		{"whitespace is trimmed to empty", "   ", 0, false},
		{"ten per second is 100ms apart", "10/s", 100 * time.Millisecond, false},
		{"fractional rate is allowed", "0.5/s", 2 * time.Second, false},
		{"bare number is treated as per-second", "10", 100 * time.Millisecond, false},
		{"surrounding whitespace is tolerated", "  5/s  ", 200 * time.Millisecond, false},
		{"one per second is 1s apart", "1/s", time.Second, false},
		{"zero-per-second is an error (asymmetry vs bare 0)", "0/s", 0, true},
		{"negative rate is an error", "-1/s", 0, true},
		{"non-numeric rate is an error", "abc", 0, true},
		{"non-numeric with suffix is an error", "fast/s", 0, true},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			got, err := parseRate(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseRate(%q): expected error, got nil (result %v)", tc.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseRate(%q): unexpected error: %v", tc.input, err)
			}
			if got != tc.expected {
				t.Fatalf("parseRate(%q) = %v, expected %v", tc.input, got, tc.expected)
			}
		})
	}
}

func TestParseArgs(t *testing.T) {
	const (
		bin     = "testbin"
		defAddr = "127.0.0.1:9999"
		defPass = "defpass"
	)

	tests := []struct {
		description string
		args        []string
		wantErr     bool
		check       func(t *testing.T, f *Flags)
	}{
		{
			description: "defaults are applied when only a subcommand is given",
			args:        []string{"pub"},
			check: func(t *testing.T, f *Flags) {
				expectStr(t, "Cmd", f.Cmd, "pub")
				expectStr(t, "Addr", f.Addr, defAddr)
				expectStr(t, "Subject", f.Subject, "demo")
				expectStr(t, "Msg", f.Msg, "hello from "+bin)
				expectStr(t, "User", f.User, "admin")
				expectStr(t, "Pass", f.Pass, defPass)
				expectInt(t, "Count", f.Count, 0)
				if f.Timeout != 0 {
					t.Errorf("Timeout = %v, expected 0", f.Timeout)
				}
				expectBool(t, "JSON", f.JSON, false)
				expectBool(t, "JetStream", f.JetStream, false)
				expectBool(t, "Durable", f.Durable, false)
				expectStr(t, "Sentinels", f.Sentinels, "")
				if f.interval != 0 {
					t.Errorf("interval = %v, expected 0", f.interval)
				}
			},
		},
		{
			description: "sub subcommand is accepted",
			args:        []string{"sub"},
			check: func(t *testing.T, f *Flags) {
				expectStr(t, "Cmd", f.Cmd, "sub")
			},
		},
		{
			description: "all flags are parsed, and -rate becomes the interval",
			args: []string{
				"sub", "-addr", "h:1", "-subject", "s", "-msg", "m",
				"-user", "u", "-pass", "p", "-count", "5", "-rate", "10/s",
				"-timeout", "3s", "-json", "-jetstream", "-durable",
				"-sentinels", "a:1,b:2",
			},
			check: func(t *testing.T, f *Flags) {
				expectStr(t, "Cmd", f.Cmd, "sub")
				expectStr(t, "Addr", f.Addr, "h:1")
				expectStr(t, "Subject", f.Subject, "s")
				expectStr(t, "Msg", f.Msg, "m")
				expectStr(t, "User", f.User, "u")
				expectStr(t, "Pass", f.Pass, "p")
				expectInt(t, "Count", f.Count, 5)
				if f.Timeout != 3*time.Second {
					t.Errorf("Timeout = %v, expected 3s", f.Timeout)
				}
				expectBool(t, "JSON", f.JSON, true)
				expectBool(t, "JetStream", f.JetStream, true)
				expectBool(t, "Durable", f.Durable, true)
				expectStr(t, "Sentinels", f.Sentinels, "a:1,b:2")
				if f.interval != 100*time.Millisecond {
					t.Errorf("interval = %v, expected 100ms", f.interval)
				}
			},
		},
		{
			description: "missing subcommand is an error",
			args:        []string{},
			wantErr:     true,
		},
		{
			description: "unknown subcommand is an error",
			args:        []string{"frobnicate"},
			wantErr:     true,
		},
		{
			description: "a flag before the subcommand is treated as an unknown subcommand",
			args:        []string{"-count", "3", "pub"},
			wantErr:     true,
		},
		{
			description: "invalid -rate is an error",
			args:        []string{"pub", "-rate", "abc"},
			wantErr:     true,
		},
		{
			description: "unknown flag is an error",
			args:        []string{"pub", "-nope"},
			wantErr:     true,
		},
		{
			description: "non-numeric -count is an error",
			args:        []string{"pub", "-count", "lots"},
			wantErr:     true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			f, err := parseArgs(bin, defAddr, defPass, tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseArgs(%q): expected error, got nil", tc.args)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseArgs(%q): unexpected error: %v", tc.args, err)
			}
			if tc.check != nil {
				tc.check(t, f)
			}
		})
	}
}

// TestParseArgsHelp verifies -h surfaces flag.ErrHelp (so Parse can suppress
// the extra error line and just print usage).
func TestParseArgsHelp(t *testing.T) {
	_, err := parseArgs("testbin", "a", "p", []string{"pub", "-h"})
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("parseArgs with -h: expected flag.ErrHelp, got %v", err)
	}
}

func TestLimiter(t *testing.T) {
	tests := []struct {
		description string
		count       int
		hits        int
		wantDone    bool
	}{
		{"limit of 3 closes Done after 3 hits", 3, 3, true},
		{"limit of 3 stays open after 2 hits", 3, 2, false},
		{"extra hits past the limit are idempotent", 3, 5, true},
		{"limit of 1 closes on the first hit", 1, 1, true},
		{"count 0 is unlimited (Done never fires)", 0, 100, false},
		{"negative count is unlimited", -1, 100, false},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			f := &Flags{Count: tc.count}
			lim := f.NewLimiter()
			for i := 0; i < tc.hits; i++ {
				lim.Hit() // must never panic, even past the limit
			}
			if got := closed(lim.Done()); got != tc.wantDone {
				t.Fatalf("after %d hits with count=%d: Done closed = %v, expected %v",
					tc.hits, tc.count, got, tc.wantDone)
			}
		})
	}
}

func TestPubLoop(t *testing.T) {
	tests := []struct {
		description string
		count       int
		failOn      int // 1-based call index to fail on; 0 = never fail
		wantCalls   int
		wantBodies  []string
		wantErr     bool
	}{
		{
			description: "default count sends exactly one message with no sequence suffix",
			count:       0,
			wantCalls:   1,
			wantBodies:  []string{"m"},
		},
		{
			description: "count of 1 sends one message with no sequence suffix",
			count:       1,
			wantCalls:   1,
			wantBodies:  []string{"m"},
		},
		{
			description: "count of 3 sends three sequence-numbered messages",
			count:       3,
			wantCalls:   3,
			wantBodies:  []string{"m 1", "m 2", "m 3"},
		},
		{
			description: "an error from send stops the loop and propagates",
			count:       5,
			failOn:      2,
			wantCalls:   2,
			wantErr:     true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			f := &Flags{Msg: "m", Count: tc.count, Subject: "s", Addr: "a"}
			var calls int
			var bodies []string
			err := f.PubLoop(func(body string) error {
				calls++
				bodies = append(bodies, body)
				if tc.failOn != 0 && calls == tc.failOn {
					return fmt.Errorf("boom")
				}
				return nil
			})
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if calls != tc.wantCalls {
				t.Fatalf("send called %d times, expected %d", calls, tc.wantCalls)
			}
			if tc.wantBodies != nil {
				if len(bodies) != len(tc.wantBodies) {
					t.Fatalf("bodies = %q, expected %q", bodies, tc.wantBodies)
				}
				for i, b := range tc.wantBodies {
					if bodies[i] != b {
						t.Fatalf("body[%d] = %q, expected %q", i, bodies[i], b)
					}
				}
			}
		})
	}
}

// --- small assertion helpers (keep the table rows terse) -------------------

func expectStr(t *testing.T, name, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %q, expected %q", name, got, want)
	}
}

func expectInt(t *testing.T, name string, got, want int) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %d, expected %d", name, got, want)
	}
}

func expectBool(t *testing.T, name string, got, want bool) {
	t.Helper()
	if got != want {
		t.Errorf("%s = %v, expected %v", name, got, want)
	}
}
