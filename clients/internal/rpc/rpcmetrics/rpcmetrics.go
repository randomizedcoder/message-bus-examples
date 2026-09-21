// Package rpcmetrics is the rpc_* Prometheus instrument set (§22): the common
// metrics every RPC transport records, so a request over gRPC, NATS, RabbitMQ,
// MQTT or Valkey lands on the same series and the dashboard compares them
// directly. It is the RPC-lab sibling of the proto-bench mbbench_* harness:
// built on an OpenTelemetry meter (obtained from internal/metrics.NewProvider),
// exported in Prometheus text format.
//
// The label set is deliberately bounded to {transport, codec, service, method,
// result} (§22, §36) — never request_id, trace_id or client_id, which would make
// the cardinality unbounded. A caller builds one Instruments per process and one
// Recorder per cell (a fixed transport/codec/service/method), so the per-request
// attribute set is cached, not rebuilt on the hot path.
package rpcmetrics

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Result label values (§22): the outcome dimension shared by rpc_responses_total
// and rpc_request_duration_seconds. They mirror the categories the benchmark
// driver already separates (a timeout is not a generic error, a non-OK status is
// neither).
const (
	ResultOK      = "ok"      // a Response with STATUS_OK
	ResultTimeout = "timeout" // deadline exceeded (transport error or STATUS_TIMEOUT)
	ResultError   = "error"   // a transport-level failure with no Response
	ResultNonOK   = "non_ok"  // a Response with a non-OK, non-timeout status
)

// ByteBucketsView gives the rpc_*_bytes histograms byte-scaled boundaries
// (100 B … 4 MiB) instead of the SDK's latency-oriented defaults, so the §26
// payload-size sweep (100 B, 1 KiB, 10 KiB, 100 KiB, 1 MiB) lands in meaningful
// buckets. metrics.LatencyBucketsView already covers the *_seconds histograms;
// pass this view alongside it to metrics.NewProvider so *_bytes is covered too.
var ByteBucketsView = sdkmetric.NewView(
	sdkmetric.Instrument{Name: "rpc_*_bytes", Kind: sdkmetric.InstrumentKindHistogram},
	sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
		Boundaries: []float64{
			100, 1024, 4096, 10240, 40960, 102400, 409600,
			1024 * 1024, 4 * 1024 * 1024,
		},
	}},
)

// Labels is the bounded label set applied to every rpc_* series. Codec is the
// wire encoding of the envelope payload (proto for every current transport);
// Transport names the binding (grpc, nats, mqtt-qos1, …).
type Labels struct {
	Transport string
	Codec     string
	Service   string
	Method    string
}

// Instruments is the rpc_* instrument set, built once per process from an OTel
// meter (mirrors harness.New). Recorder binds it to one Labels so the hot path
// never rebuilds the attribute set.
type Instruments struct {
	requests     metric.Int64Counter
	responses    metric.Int64Counter
	errors       metric.Int64Counter
	timeouts     metric.Int64Counter
	replays      metric.Int64Counter
	duration     metric.Float64Histogram
	requestBytes metric.Int64Histogram
	responseByte metric.Int64Histogram
}

// New registers the rpc_* instruments on meter. Errors surface the first
// registration failure so a misconfigured meter fails loudly at start, not
// silently at record time.
func New(meter metric.Meter) (*Instruments, error) {
	in := &Instruments{}
	var err error
	ctr := func(name, desc string) metric.Int64Counter {
		if err != nil {
			return nil
		}
		var c metric.Int64Counter
		c, err = meter.Int64Counter(name, metric.WithDescription(desc))
		return c
	}
	in.requests = ctr("rpc_requests_total", "RPC requests issued")
	in.responses = ctr("rpc_responses_total", "RPC responses received, by result")
	in.errors = ctr("rpc_errors_total", "RPC transport-level errors (no response)")
	in.timeouts = ctr("rpc_timeouts_total", "RPC calls that exceeded their deadline")
	in.replays = ctr("rpc_idempotent_replays_total", "responses served from the service idempotency cache (duplicate logical operations, §29)")
	if err != nil {
		return nil, err
	}
	if in.duration, err = meter.Float64Histogram("rpc_request_duration_seconds",
		metric.WithDescription("end-to-end RPC round-trip time, by result"),
		metric.WithUnit("s")); err != nil {
		return nil, err
	}
	if in.requestBytes, err = meter.Int64Histogram("rpc_request_bytes",
		metric.WithDescription("serialized request envelope size"),
		metric.WithUnit("By")); err != nil {
		return nil, err
	}
	if in.responseByte, err = meter.Int64Histogram("rpc_response_bytes",
		metric.WithDescription("serialized response envelope size"),
		metric.WithUnit("By")); err != nil {
		return nil, err
	}
	return in, nil
}

// Recorder is Instruments bound to one cell's Labels, with the MeasurementOption
// cached. A nil *Recorder is a no-op, so callers can hold one whether or not
// metrics are enabled.
type Recorder struct {
	in   *Instruments
	base metric.MeasurementOption
}

// For binds in to l, caching l's attribute set. It returns nil when in is nil so
// a disabled process carries a nil Recorder that no-ops.
func (in *Instruments) For(l Labels) *Recorder {
	if in == nil {
		return nil
	}
	return &Recorder{
		in: in,
		base: metric.WithAttributes(
			attribute.String("transport", l.Transport),
			attribute.String("codec", l.Codec),
			attribute.String("service", l.Service),
			attribute.String("method", l.Method),
		),
	}
}

// Observe records one completed call: it counts the request, its request-size
// sample, and — keyed on result — the response count/size, duration, and the
// error/timeout counter. hasResp is false for a transport error that produced no
// Response (respBytes is then ignored). replay marks an OK response that the
// service served from its idempotency cache (a duplicate logical operation, §29),
// counted separately into rpc_idempotent_replays_total. A nil Recorder does
// nothing.
func (r *Recorder) Observe(ctx context.Context, result string, d time.Duration, reqBytes int, hasResp bool, respBytes int, replay bool) {
	if r == nil {
		return
	}
	resultOpt := metric.WithAttributes(attribute.String("result", result))
	r.in.requests.Add(ctx, 1, r.base)
	r.in.requestBytes.Record(ctx, int64(reqBytes), r.base)
	r.in.duration.Record(ctx, d.Seconds(), r.base, resultOpt)
	if hasResp {
		r.in.responses.Add(ctx, 1, r.base, resultOpt)
		r.in.responseByte.Record(ctx, int64(respBytes), r.base)
	}
	if replay {
		r.in.replays.Add(ctx, 1, r.base)
	}
	switch result {
	case ResultTimeout:
		r.in.timeouts.Add(ctx, 1, r.base)
	case ResultError:
		r.in.errors.Add(ctx, 1, r.base)
	}
}
