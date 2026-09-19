// Package metrics adds opt-in OpenTelemetry metrics (exported in Prometheus
// text format) to the message-bus clients, for the multi-hour soak test.
//
// A client enables it by passing -metrics-addr host:port; Setup then starts an
// HTTP server exposing /metrics there, which the in-cluster Prometheus scrapes
// over the host bridge. Every series is labelled with the fixed {bus, mode,
// role} of the process, so one Prometheus job sees every soak client.
//
// The instruments are deliberately small — enough to answer "how well did each
// client hold up": throughput (published/received), errors, reconnects,
// sequence gaps (lost messages), and request/confirm latency.
//
// When -metrics-addr is empty the clients use the no-op Recorder (Nop), so the
// default behaviour and the chaos-harness contract are unchanged.
package metrics

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel/attribute"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Recorder is the tiny surface the clients record through. Setup returns a
// live *Metrics; Nop{} is the do-nothing default when metrics are disabled.
type Recorder interface {
	IncPublished()
	IncPublishError()
	IncReceived()
	IncReconnect()
	AddGaps(n int64)
	ObserveLatency(d time.Duration)
}

// Config fixes the labels applied to every series from this process.
type Config struct {
	Addr string // host:port to serve /metrics on; empty disables metrics
	Bus  string // nats | rabbitmq | mqtt | valkey
	Mode string // core | jetstream | durable | sentinel | ...
	Role string // pub | sub
}

// Metrics is a live Recorder backed by an OTel meter + Prometheus exporter.
type Metrics struct {
	published, received, publishErrors, reconnects, gaps metric.Int64Counter
	latency                                              metric.Float64Histogram
	opt                                                  metric.MeasurementOption
}

// Setup builds the meter provider, registers the instruments, and starts the
// /metrics HTTP server on cfg.Addr. A cfg.Addr of "" returns (Nop{}, nil) —
// metrics disabled. The returned Recorder is safe for concurrent use.
func Setup(cfg Config) (Recorder, error) {
	if cfg.Addr == "" {
		return Nop{}, nil
	}

	reg := promclient.NewRegistry()
	exporter, err := otelprom.New(otelprom.WithRegisterer(reg))
	if err != nil {
		return nil, fmt.Errorf("prometheus exporter: %w", err)
	}
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter))
	meter := provider.Meter("github.com/randomizedcoder/message-bus-examples/clients")

	m := &Metrics{
		opt: metric.WithAttributes(
			attribute.String("bus", cfg.Bus),
			attribute.String("mode", cfg.Mode),
			attribute.String("role", cfg.Role),
		),
	}
	if m.published, err = meter.Int64Counter("mbclient_published",
		metric.WithDescription("messages published")); err != nil {
		return nil, err
	}
	if m.received, err = meter.Int64Counter("mbclient_received",
		metric.WithDescription("messages received")); err != nil {
		return nil, err
	}
	if m.publishErrors, err = meter.Int64Counter("mbclient_publish_errors",
		metric.WithDescription("publish attempts that returned an error")); err != nil {
		return nil, err
	}
	if m.reconnects, err = meter.Int64Counter("mbclient_reconnects",
		metric.WithDescription("client reconnects to the bus")); err != nil {
		return nil, err
	}
	if m.gaps, err = meter.Int64Counter("mbclient_receive_gaps",
		metric.WithDescription("missing messages inferred from sequence gaps")); err != nil {
		return nil, err
	}
	if m.latency, err = meter.Float64Histogram("mbclient_request_latency_seconds",
		metric.WithDescription("request/confirm round-trip latency"),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	srv := &http.Server{Addr: cfg.Addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.ListenAndServe() }()

	return m, nil
}

func (m *Metrics) IncPublished()      { m.published.Add(context.Background(), 1, m.opt) }
func (m *Metrics) IncPublishError()   { m.publishErrors.Add(context.Background(), 1, m.opt) }
func (m *Metrics) IncReceived()       { m.received.Add(context.Background(), 1, m.opt) }
func (m *Metrics) IncReconnect()      { m.reconnects.Add(context.Background(), 1, m.opt) }
func (m *Metrics) AddGaps(n int64)    { m.gaps.Add(context.Background(), n, m.opt) }
func (m *Metrics) ObserveLatency(d time.Duration) {
	m.latency.Record(context.Background(), d.Seconds(), m.opt)
}

// Nop is the disabled Recorder: every method is a no-op.
type Nop struct{}

func (Nop) IncPublished()                {}
func (Nop) IncPublishError()             {}
func (Nop) IncReceived()                 {}
func (Nop) IncReconnect()                {}
func (Nop) AddGaps(int64)                {}
func (Nop) ObserveLatency(time.Duration) {}

// RecordReceived counts one received message and folds its sequence tail (if
// any) into the gap tracker, so lost messages surface as mbclient_receive_gaps.
// It is the one call every subscriber makes per message.
func RecordReceived(rec Recorder, g *GapTracker, data string) {
	rec.IncReceived()
	if seq, ok := ParseSeq(data); ok {
		if gap := g.Observe(seq); gap > 0 {
			rec.AddGaps(gap)
		}
	}
}

// ParseSeq extracts the trailing 1-based ordinal that PubLoop appends to a
// message body ("orders 42" → 42). It returns (0, false) when the body has no
// whitespace-delimited integer tail, so callers can skip gap accounting for
// payloads that carry no sequence (e.g. a single-message publish).
func ParseSeq(body string) (int64, bool) {
	fields := strings.Fields(body)
	if len(fields) == 0 {
		return 0, false
	}
	n, err := strconv.ParseInt(fields[len(fields)-1], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// GapTracker turns a stream of observed sequence numbers into a count of
// missing messages, tolerating the realities of a soak: duplicates, reorders,
// and the sequence resetting to 1 when a supervised publisher respawns.
//
// Observe(seq) returns the number of messages inferred missing *at this step*:
//   - first sequence ever seen        → 0 (nothing to compare against)
//   - seq == last+1 (in order)         → 0
//   - seq  > last+1 (forward jump)     → seq-last-1 (that many were skipped)
//   - seq == last   (duplicate)        → 0, baseline unchanged
//   - seq  < last   (reorder / reset)  → 0, baseline reset to seq (a lower
//     sequence is treated as a new publisher run, never a negative gap)
type GapTracker struct {
	mu   sync.Mutex
	last int64
	have bool
}

// Observe records one sequence number and returns the gap it implies.
func (g *GapTracker) Observe(seq int64) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.have {
		g.have = true
		g.last = seq
		return 0
	}
	switch {
	case seq == g.last+1:
		g.last = seq
		return 0
	case seq > g.last+1:
		gap := seq - g.last - 1
		g.last = seq
		return gap
	case seq < g.last:
		// Lower than the baseline: a respawn/reset — start a fresh run here.
		g.last = seq
		return 0
	default: // seq == g.last: duplicate
		return 0
	}
}
