package rpc

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/tracing"
)

// TracingClient decorates any Client so each Call is a client span and the
// current W3C trace context is injected into the request's metadata map (§23) —
// the carrier every transport marshals end-to-end. It is a drop-in Client and
// composes with RetryClient: wrap tracing INSIDE retry
// (NewRetryClient(NewTracingClient(base))) so each attempt is its own span and
// carries its own request_id, which is exactly how a retried outage should read
// on the trace.
type TracingClient struct {
	inner Client
}

// NewTracingClient wraps inner so its calls are traced. With tracing off (the
// default provider) the tracer is a no-op and this adds negligible overhead.
func NewTracingClient(inner Client) *TracingClient { return &TracingClient{inner: inner} }

// Call starts a client span, injects the trace context into req.Metadata (and
// mirrors the trace id onto req.TraceId, the low-cardinality display field),
// issues the call, and records the resulting Status on the span.
func (c *TracingClient) Call(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	ctx, span := tracing.Tracer().Start(ctx, spanName(req),
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(reqAttrs(req)...))
	defer span.End()

	// Carry the trace across the hop in the envelope metadata — the one carrier
	// that survives every transport (MQTT / Valkey pub/sub have no native headers).
	if req.Metadata == nil {
		req.Metadata = map[string]string{}
	}
	otel.GetTextMapPropagator().Inject(ctx, tracing.MapCarrier(req.Metadata))
	// Keep the §23 trace_id field (field 5, low cardinality) in sync for display.
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		req.TraceId = sc.TraceID().String()
	}

	resp, err := c.inner.Call(ctx, req)
	recordStatus(span, resp, err)
	return resp, err
}

// Capabilities delegates to the wrapped client.
func (c *TracingClient) Capabilities() Capabilities { return c.inner.Capabilities() }

// Close delegates to the wrapped client.
func (c *TracingClient) Close() error { return c.inner.Close() }

// TracingHandler decorates any Handler so each inbound request continues the
// caller's trace: it extracts the W3C context from the request metadata and runs
// the handler inside a server span (§23). Wrap the service's dispatch handler and
// the gateway's router with this so every hop contributes a span to one trace.
type TracingHandler struct {
	inner Handler
}

// NewTracingHandler wraps inner so its handling is traced.
func NewTracingHandler(inner Handler) *TracingHandler { return &TracingHandler{inner: inner} }

// Handle extracts the inbound trace context from req.Metadata, runs inner inside
// a server span that is a child of the caller's span, and records the Status.
func (h *TracingHandler) Handle(ctx context.Context, req *rpcv1.Request) (*rpcv1.Response, error) {
	if md := req.GetMetadata(); md != nil {
		ctx = otel.GetTextMapPropagator().Extract(ctx, tracing.MapCarrier(md))
	}
	ctx, span := tracing.Tracer().Start(ctx, spanName(req),
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(reqAttrs(req)...))
	defer span.End()

	resp, err := h.inner.Handle(ctx, req)
	recordStatus(span, resp, err)
	return resp, err
}

// spanName is the span's display name: "service.method" (bounded, no ids).
func spanName(req *rpcv1.Request) string {
	return req.GetService() + "." + req.GetMethod()
}

// reqAttrs is the bounded set of span attributes for a request. request_id and
// idempotency_key are fine on spans (unlike metrics, spans are not aggregated by
// label), and make a trace searchable.
func reqAttrs(req *rpcv1.Request) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("rpc.system", "message-bus-rpc"),
		attribute.String("rpc.service", req.GetService()),
		attribute.String("rpc.method", req.GetMethod()),
	}
	if id := req.GetRequestId(); id != "" {
		attrs = append(attrs, attribute.String("rpc.request_id", id))
	}
	if k := req.GetIdempotencyKey(); k != "" {
		attrs = append(attrs, attribute.String("rpc.idempotency_key", k))
	}
	return attrs
}

// recordStatus maps the call outcome onto the span: a transport error records
// the error and marks the span Error; otherwise a non-OK Response status marks it
// Error with the status name. The rpc status is always set as an attribute.
func recordStatus(span trace.Span, resp *rpcv1.Response, err error) {
	if err != nil {
		span.SetAttributes(attribute.String("rpc.status", StatusOf(err).String()))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return
	}
	st := resp.GetStatus()
	span.SetAttributes(attribute.String("rpc.status", st.String()))
	if st != rpcv1.Status_STATUS_OK && st != rpcv1.Status_STATUS_UNSPECIFIED {
		span.SetStatus(codes.Error, st.String())
	}
}
