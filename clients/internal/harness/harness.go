// Package harness holds the proto-bench measurement layer: the mbbench_*
// OpenTelemetry instruments both the driver (role=client) and the region-agent
// (role=server) record through, the per-cell label set, and a latency
// accumulator that turns a stream of RTTs into a summary.
//
// P2 implements the pieces the gRPC latency loop needs: the instrument set, the
// Cell labels, and Latencies/Summary. The richer run modes (open-loop,
// saturation, cold-start, fault), HDR histograms, and the run.json / results.md
// renderers arrive with the k8s-proto-bench host harness in P4 (design §8, §11).
package harness

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Cell is the fixed label set of one benchmark cell (design §11.4). The driver
// builds one per run; the agent builds one per request from the envelope plus
// its own fixed pool/region/gc. Never carries run_id or message_id — a run is
// selected by time range and Grafana annotations (§12.4).
type Cell struct {
	Transport string // grpc_unary, nats_request_reply, …
	Codec     string // proto | protojson | vtproto
	Fixture   string // tiny … max, sparse, dense
	Pool      string // none | messages | buffers | all
	GC        string // default | limit
	Mode      string // latency | openloop | …
	Region    string // us-west-2, …
	Role      string // client | server
	Tier      string // rpc | at_least_once | at_most_once (§2.3)
}

// opt caches the MeasurementOption for a Cell so the hot path does not rebuild
// the attribute set on every record.
type opt struct {
	base metric.MeasurementOption
}

func (c Cell) option() opt {
	return opt{base: metric.WithAttributes(
		attribute.String("transport", c.Transport),
		attribute.String("codec", c.Codec),
		attribute.String("fixture", c.Fixture),
		attribute.String("pool", c.Pool),
		attribute.String("gc", c.GC),
		attribute.String("mode", c.Mode),
		attribute.String("region", c.Region),
		attribute.String("role", c.Role),
		attribute.String("tier", c.Tier),
	)}
}

// Instruments is the mbbench_* instrument set (design §11.4). One is built per
// process from a metrics.NewProvider meter; the Cell passed to each method
// supplies the labels.
type Instruments struct {
	messages    metric.Int64Counter
	errors      metric.Int64Counter
	rtt         metric.Float64Histogram
	oneWay      metric.Float64Histogram
	serverDur   metric.Float64Histogram
	codecSecs   metric.Float64Histogram
	validateSec metric.Float64Histogram
	wireBytes   metric.Int64Counter
	inflight    metric.Int64UpDownCounter
	cellInfo    metric.Int64Gauge
}

// New registers the mbbench_* instruments on meter.
func New(meter metric.Meter) (*Instruments, error) {
	in := &Instruments{}
	var err error
	h := func(name, desc string) metric.Float64Histogram {
		if err != nil {
			return nil
		}
		var hist metric.Float64Histogram
		hist, err = meter.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit("s"))
		return hist
	}
	c := func(name, desc string) metric.Int64Counter {
		if err != nil {
			return nil
		}
		var ctr metric.Int64Counter
		ctr, err = meter.Int64Counter(name, metric.WithDescription(desc))
		return ctr
	}
	in.messages = c("mbbench_messages_total", "messages by result (sent|received|ok|error)")
	in.errors = c("mbbench_errors_total", "errors by kind (timeout|nack|decode|validate|transport|late)")
	in.wireBytes = c("mbbench_wire_bytes_total", "on-the-wire bytes by direction")
	in.rtt = h("mbbench_rtt_seconds", "driver monotonic round-trip time")
	in.oneWay = h("mbbench_one_way_seconds", "wall-clock one-way latency (clock gate permitting)")
	in.serverDur = h("mbbench_server_duration_seconds", "agent receive→send duration")
	in.codecSecs = h("mbbench_codec_seconds", "codec-only time per message")
	in.validateSec = h("mbbench_validate_seconds", "protovalidate time per message")
	if err != nil {
		return nil, err
	}
	if in.inflight, err = meter.Int64UpDownCounter("mbbench_inflight",
		metric.WithDescription("outstanding requests / open window")); err != nil {
		return nil, err
	}
	if in.cellInfo, err = meter.Int64Gauge("mbbench_cell_info",
		metric.WithDescription("the active cell (=1), for state-timeline panels and joins")); err != nil {
		return nil, err
	}
	return in, nil
}

// result label values for mbbench_messages_total.
const (
	ResultSent     = "sent"
	ResultReceived = "received"
	ResultOK       = "ok"
	ResultError    = "error"
)

// direction label values for the *_bytes / one_way instruments.
const (
	DirForward = "forward"
	DirReverse = "reverse"
)

func (in *Instruments) Message(ctx context.Context, c Cell, result string) {
	in.messages.Add(ctx, 1, c.option().base, metric.WithAttributes(attribute.String("result", result)))
}

func (in *Instruments) Error(ctx context.Context, c Cell, kind string) {
	in.errors.Add(ctx, 1, c.option().base, metric.WithAttributes(attribute.String("kind", kind)))
}

func (in *Instruments) RTT(ctx context.Context, c Cell, d time.Duration) {
	in.rtt.Record(ctx, d.Seconds(), c.option().base)
}

func (in *Instruments) OneWay(ctx context.Context, c Cell, d time.Duration, direction string) {
	in.oneWay.Record(ctx, d.Seconds(), c.option().base, metric.WithAttributes(attribute.String("direction", direction)))
}

func (in *Instruments) ServerDuration(ctx context.Context, c Cell, d time.Duration) {
	in.serverDur.Record(ctx, d.Seconds(), c.option().base)
}

func (in *Instruments) CodecTime(ctx context.Context, c Cell, d time.Duration) {
	in.codecSecs.Record(ctx, d.Seconds(), c.option().base)
}

func (in *Instruments) ValidateTime(ctx context.Context, c Cell, d time.Duration) {
	in.validateSec.Record(ctx, d.Seconds(), c.option().base)
}

func (in *Instruments) WireBytes(ctx context.Context, c Cell, n int, direction string) {
	in.wireBytes.Add(ctx, int64(n), c.option().base, metric.WithAttributes(attribute.String("direction", direction)))
}

func (in *Instruments) AddInflight(ctx context.Context, c Cell, delta int64) {
	in.inflight.Add(ctx, delta, c.option().base)
}

// SetActiveCell marks c as the running cell (=1). Call SetActiveCell with the
// same Cell and value 0 when the cell ends so state-timeline panels are clean.
func (in *Instruments) SetActiveCell(ctx context.Context, c Cell, on bool) {
	v := int64(0)
	if on {
		v = 1
	}
	in.cellInfo.Record(ctx, v, c.option().base)
}

// Summary is the reduced view of a latency cell.
type Summary struct {
	Count            int
	Errors           int
	Min, P50, P90    time.Duration
	P99, P999, Max   time.Duration
	Mean             time.Duration
	ThroughputPerSec float64
}

// Summaries are produced by HDR.Summarize (hdr.go); the exact-sort accumulator
// this package shipped in P2 was replaced by the HDR histogram in P4b (design
// §8).
