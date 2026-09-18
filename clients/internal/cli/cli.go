// Package cli holds the tiny shared flag-parsing + pub/sub-loop helpers
// used by every message-bus client CLI (natscli, rabbitmqcli, mqttcli,
// valkeycli). Keeping the loop/rate/count/timeout/JSON logic here lets each
// client's main.go stay a thin, bus-specific adapter.
package cli

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// Flags are the options common to every client's pub/sub subcommands.
type Flags struct {
	Cmd     string // "pub" or "sub"
	Addr    string // host:port of the bus NodePort
	Subject string // subject / topic / channel / queue name
	Msg     string // message body (pub only)
	User    string // username (buses that authenticate)
	Pass    string // password (buses that authenticate)

	Count   int           // pub: messages to send (default 1); sub: exit after N (0 = unlimited)
	Timeout time.Duration // sub: exit after this long (0 = run until Ctrl-C)
	JSON    bool          // emit messages as one JSON object per line

	// Per-bus HA opt-ins (ignored by the buses they don't apply to).
	JetStream bool   // NATS: durable JetStream stream + durable consumer
	Durable   bool   // RabbitMQ: durable quorum queue (work-queue semantics)
	Sentinels string // ValKey: comma-separated Sentinel host:port list → FailoverClient

	interval time.Duration // per-message pub delay, derived from -rate
}

// Parse reads os.Args as `<bin> <pub|sub> [flags]` and returns the resolved
// Flags, exiting the process (via Usage) on any error. `defAddr` is the
// default NodePort address for this bus; `defPass` seeds -pass (typically from
// an env var). The parsing itself lives in parseArgs (pure, error-returning)
// so it can be unit-tested without touching os.Args or os.Exit.
func Parse(bin, defAddr, defPass string) *Flags {
	f, err := parseArgs(bin, defAddr, defPass, os.Args[1:])
	if err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintf(os.Stderr, "%s: %v\n", bin, err)
		}
		Usage(bin) // prints the usage line and exits 2
	}
	return f
}

// parseArgs is the pure core of Parse: given the argument slice (without the
// program name), it returns the resolved Flags or an error, never touching
// os.Args, os.Exit, or global state. args[0] is the subcommand.
func parseArgs(bin, defAddr, defPass string, args []string) (*Flags, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("missing subcommand (want pub|sub)")
	}
	cmd := args[0]
	if cmd != "pub" && cmd != "sub" {
		return nil, fmt.Errorf("unknown subcommand %q (want pub|sub)", cmd)
	}
	fs := flag.NewFlagSet(bin+" "+cmd, flag.ContinueOnError)
	fs.SetOutput(io.Discard) // errors are returned, not printed here
	f := &Flags{Cmd: cmd}
	fs.StringVar(&f.Addr, "addr", defAddr, "bus host:port (NodePort)")
	fs.StringVar(&f.Subject, "subject", "demo", "subject / topic / channel / queue")
	fs.StringVar(&f.Msg, "msg", "hello from "+bin, "message body (pub)")
	fs.StringVar(&f.User, "user", "admin", "username")
	fs.StringVar(&f.Pass, "pass", defPass, "password")
	fs.IntVar(&f.Count, "count", 0, "pub: messages to send (default 1); sub: exit after N messages (0 = unlimited)")
	rate := fs.String("rate", "", "pub throttle, e.g. 10/s or 0.5/s (default: send as fast as possible)")
	fs.DurationVar(&f.Timeout, "timeout", 0, "sub: exit after this long, e.g. 10s (0 = run until Ctrl-C)")
	fs.BoolVar(&f.JSON, "json", false, "emit each message as a JSON object per line")
	fs.BoolVar(&f.JetStream, "jetstream", false, "NATS only: durable JetStream (create stream + durable consumer)")
	fs.BoolVar(&f.Durable, "durable", false, "RabbitMQ only: durable quorum queue (work-queue semantics)")
	fs.StringVar(&f.Sentinels, "sentinels", "", "ValKey only: comma-separated Sentinel host:port list (enables primary discovery)")
	if err := fs.Parse(args[1:]); err != nil {
		return nil, err
	}
	iv, err := parseRate(*rate)
	if err != nil {
		return nil, fmt.Errorf("invalid -rate %q: %w", *rate, err)
	}
	f.interval = iv
	return f, nil
}

// parseRate converts a "-rate" value into a per-message delay. Accepted
// forms: "" or "0" → no throttle; "N/s" or "N" (messages per second, N may
// be fractional) → 1s/N. A non-positive or unparseable N is an error.
func parseRate(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "0" {
		return 0, nil
	}
	s = strings.TrimSuffix(s, "/s")
	n, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
	if err != nil {
		return 0, fmt.Errorf("want N/s (e.g. 10/s)")
	}
	if n <= 0 {
		return 0, fmt.Errorf("rate must be > 0")
	}
	return time.Duration(float64(time.Second) / n), nil
}

// Usage prints the standard usage line and exits non-zero.
func Usage(bin string) {
	fmt.Fprintf(os.Stderr,
		"usage: %s <pub|sub> [-addr host:port] [-subject name] [-msg text]\n"+
			"          [-user u] [-pass p] [-count n] [-rate N/s] [-timeout d] [-json]\n"+
			"          [-jetstream (nats)] [-durable (rabbitmq)] [-sentinels h:p,... (valkey)]\n", bin)
	os.Exit(2)
}

// PubLoop invokes send once per message — Count times (default 1),
// throttled to -rate — then prints a one-line summary. When more than one
// message is sent, a 1-based sequence number is appended to -msg so each
// payload is distinct (e.g. "hello 1", "hello 2").
func (f *Flags) PubLoop(send func(body string) error) error {
	n := f.Count
	if n <= 0 {
		n = 1
	}
	for i := 0; i < n; i++ {
		if i > 0 && f.interval > 0 {
			time.Sleep(f.interval)
		}
		body := f.Msg
		if n > 1 {
			body = fmt.Sprintf("%s %d", f.Msg, i+1)
		}
		if err := send(body); err != nil {
			return fmt.Errorf("publish %d/%d: %w", i+1, n, err)
		}
	}
	fmt.Printf("published %d message(s) to %q on %s\n", n, f.Subject, f.Addr)
	return nil
}

// Emit renders one received message, either as a human line or, with
// -json, as a compact JSON object.
func (f *Flags) Emit(subject, data string) {
	if f.JSON {
		b, _ := json.Marshal(struct {
			TS      string `json:"ts"`
			Subject string `json:"subject"`
			Data    string `json:"data"`
		}{time.Now().Format(time.RFC3339Nano), subject, data})
		fmt.Println(string(b))
	} else {
		fmt.Printf("%s  [%s] %s\n", time.Now().Format(time.RFC3339), subject, data)
	}
}

// Limiter lets a subscriber exit after -count messages. Callback-style
// clients call Hit per message and select on Done; channel-style clients
// can do the same inside their receive loop. Count <= 0 means unlimited
// (Done never fires).
type Limiter struct {
	limit int
	mu    sync.Mutex
	n     int
	done  chan struct{}
	once  sync.Once
}

// NewLimiter returns a Limiter bound to -count.
func (f *Flags) NewLimiter() *Limiter {
	return &Limiter{limit: f.Count, done: make(chan struct{})}
}

// Hit records one received message and closes Done once the limit is met.
func (l *Limiter) Hit() {
	if l.limit <= 0 {
		return
	}
	l.mu.Lock()
	l.n++
	reached := l.n >= l.limit
	l.mu.Unlock()
	if reached {
		l.once.Do(func() { close(l.done) })
	}
}

// Done fires once -count messages have been received (never, if unlimited).
func (l *Limiter) Done() <-chan struct{} { return l.done }

// Stop returns a channel that fires on SIGINT/SIGTERM, or after -timeout
// (if set). A subscriber typically selects on both this and a Limiter.
func (f *Flags) Stop() <-chan struct{} {
	ch := make(chan struct{})
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		if f.Timeout > 0 {
			t := time.NewTimer(f.Timeout)
			defer t.Stop()
			select {
			case <-sig:
			case <-t.C:
			}
		} else {
			<-sig
		}
		close(ch)
	}()
	return ch
}
