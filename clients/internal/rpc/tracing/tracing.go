// Package tracing adds opt-in OpenTelemetry distributed tracing to the RPC lab
// (§23). Trace context rides the rpc.v1 envelope's metadata map — the one carrier
// every transport marshals end-to-end (gRPC metadata, NATS/RabbitMQ headers, and
// MQTT / Valkey pub/sub, which have no native header mechanism at all) — so a
// single trace spans the whole path
//
//	client → gateway-A → broker → gateway-B → rpc-service
//
// and makes it possible to see where latency was introduced (§23). It is opt-in
// like metrics: the default mode installs a no-op tracer so the hot path is
// unchanged; pass -trace stdout on every hop to capture a trace end-to-end.
//
// There is no trace backend in the cluster yet, so the built-in exporter writes
// each finished span as one JSON line to stderr (visible in `kubectl logs`). The
// design is backend-ready: swapping the stderr exporter for an OTLP one is a
// one-line change once a Tempo/Jaeger/collector endpoint exists.
package tracing

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// tracerName is the instrumentation-scope name every RPC span is created under.
const tracerName = "github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"

// Propagator is the W3C standard context propagator (traceparent + tracestate)
// plus baggage — the "standard trace context" §23 asks transports to carry. It
// is what makes the trace continue across a hop regardless of transport.
func Propagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
}

// NewProvider installs the global propagator (always, so an already-sampled
// inbound trace propagates even through an un-traced hop) and a global
// TracerProvider for service, choosing the exporter by mode:
//
//	"" | "off" | "none"  no-op tracer: spans are free and nothing is recorded.
//	"stdout" | "stderr"  batch-export each finished span as one JSON line to
//	                     stderr, sampling every root (ParentBased(AlwaysSample)).
//
// The returned shutdown flushes the exporter; call it on process exit. mode
// "stdout" adds no external dependency (the exporter is in this package).
func NewProvider(service, mode string) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(Propagator())

	switch mode {
	case "", "off", "none":
		otel.SetTracerProvider(noop.NewTracerProvider())
		return func(context.Context) error { return nil }, nil
	case "stdout", "stderr":
		res := resource.NewSchemaless(
			attribute.String("service.name", service),
			attribute.String("service.namespace", "rpc"),
		)
		tp := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(stderrExporter{}),
			sdktrace.WithResource(res),
		)
		otel.SetTracerProvider(tp)
		return tp.Shutdown, nil
	default:
		return nil, fmt.Errorf("tracing: unknown mode %q (off|stdout)", mode)
	}
}

// Tracer returns the RPC tracer from the global provider. Callers that ran
// NewProvider with mode off get a no-op tracer, so this is always safe to call.
func Tracer() trace.Tracer { return otel.Tracer(tracerName) }

// MapCarrier adapts the envelope metadata map to the OTel TextMapCarrier
// interface so the propagator can read and write traceparent/tracestate there.
// The map is bounded to 32 pairs by protovalidate (rpc.proto), and W3C context
// adds at most two keys.
type MapCarrier map[string]string

// Get returns the value for key, or "" if absent.
func (c MapCarrier) Get(key string) string { return c[key] }

// Set stores value under key.
func (c MapCarrier) Set(key, value string) { c[key] = value }

// Keys lists the carrier's keys.
func (c MapCarrier) Keys() []string {
	ks := make([]string, 0, len(c))
	for k := range c {
		ks = append(ks, k)
	}
	return ks
}

// stderrExporter is a minimal sdktrace.SpanExporter that prints each finished
// span as one JSON line to stderr. It avoids pulling the stdouttrace module (not
// vendored) so this whole feature adds no external dependency. It is meant for
// eyeballing / `kubectl logs`, not for production ingest.
type stderrExporter struct{}

// ExportSpans writes one "span {...}" JSON line per span to stderr.
func (stderrExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	for _, s := range spans {
		sc := s.SpanContext()
		attrs := make(map[string]string, len(s.Attributes()))
		for _, kv := range s.Attributes() {
			attrs[string(kv.Key)] = kv.Value.Emit()
		}
		rec := map[string]any{
			"trace_id":    sc.TraceID().String(),
			"span_id":     sc.SpanID().String(),
			"parent_id":   s.Parent().SpanID().String(),
			"name":        s.Name(),
			"kind":        s.SpanKind().String(),
			"start":       s.StartTime().Format(time.RFC3339Nano),
			"duration_us": s.EndTime().Sub(s.StartTime()).Microseconds(),
			"status":      s.Status().Code.String(),
			"attrs":       attrs,
		}
		b, err := json.Marshal(rec)
		if err != nil {
			continue
		}
		fmt.Fprintln(os.Stderr, "span "+string(b))
	}
	return nil
}

// Shutdown is a no-op; stderr needs no flush.
func (stderrExporter) Shutdown(context.Context) error { return nil }
