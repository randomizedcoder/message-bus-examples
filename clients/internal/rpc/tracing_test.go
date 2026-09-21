package rpc

import (
	"context"
	"regexp"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	otelcodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/tracing"
)

// recordingExporter is a synchronous in-memory SpanExporter so a test can inspect
// the spans TracingClient / TracingHandler produced (there is no tracetest module
// vendored). Paired with WithSyncer, ExportSpans runs before End returns.
type recordingExporter struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (e *recordingExporter) ExportSpans(_ context.Context, s []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, s...)
	return nil
}
func (e *recordingExporter) Shutdown(context.Context) error { return nil }

func (e *recordingExporter) all() []sdktrace.ReadOnlySpan {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sdktrace.ReadOnlySpan(nil), e.spans...)
}

// withRecorder installs a real (always-sampling) TracerProvider that exports
// synchronously to the returned recorder, plus the W3C propagator, and restores
// the previous globals on cleanup.
func withRecorder(t *testing.T) *recordingExporter {
	t.Helper()
	rec := &recordingExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(rec))
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(tracing.Propagator())
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})
	return rec
}

var hex32 = regexp.MustCompile(`^[0-9a-f]{32}$`)

func spanAttr(s sdktrace.ReadOnlySpan, key string) (string, bool) {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.Emit(), true
		}
	}
	return "", false
}

func TestMapCarrier(t *testing.T) {
	c := tracing.MapCarrier{}
	if got := c.Get("absent"); got != "" {
		t.Errorf("Get on empty = %q, want \"\"", got)
	}
	c.Set("traceparent", "00-abc-def-01")
	c.Set("tracestate", "vendor=x")
	if got := c.Get("traceparent"); got != "00-abc-def-01" {
		t.Errorf("Get(traceparent) = %q", got)
	}
	keys := map[string]bool{}
	for _, k := range c.Keys() {
		keys[k] = true
	}
	if !keys["traceparent"] || !keys["tracestate"] || len(keys) != 2 {
		t.Errorf("Keys() = %v, want traceparent+tracestate", c.Keys())
	}
}

func TestTracingClientInjectsAndRecords(t *testing.T) {
	timeoutErr := Errorf(rpcv1.Status_STATUS_TIMEOUT, "deadline")

	tests := []struct {
		description string
		outcome     outcome
		wantStatus  string         // rpc.status attribute on the span
		wantCode    otelcodes.Code // span status code
	}{
		{
			description: "an OK response leaves the span status unset",
			outcome:     outcome{resp: respOK()},
			wantStatus:  rpcv1.Status_STATUS_OK.String(),
			wantCode:    otelcodes.Unset,
		},
		{
			description: "a non-OK response marks the span an error",
			outcome:     outcome{resp: status(rpcv1.Status_STATUS_NOT_FOUND)},
			wantStatus:  rpcv1.Status_STATUS_NOT_FOUND.String(),
			wantCode:    otelcodes.Error,
		},
		{
			description: "a transport error marks the span an error with the mapped status",
			outcome:     outcome{err: timeoutErr},
			wantStatus:  rpcv1.Status_STATUS_TIMEOUT.String(),
			wantCode:    otelcodes.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			rec := withRecorder(t)
			fake := &scriptedClient{outcomes: []outcome{tt.outcome}}
			tc := NewTracingClient(fake)
			req, err := NewRequest("customer", "Lookup", &rpcv1.Request{}, time.Second)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			if _, err := tc.Call(context.Background(), req); (err != nil) != (tt.outcome.err != nil) {
				t.Fatalf("Call err = %v", err)
			}

			// The request the transport saw must carry a W3C traceparent and the
			// display trace_id (§23), and request_id must be preserved.
			sent := fake.seen[0]
			if tp := sent.GetMetadata()["traceparent"]; tp == "" {
				t.Errorf("no traceparent injected into metadata: %v", sent.GetMetadata())
			}
			if !hex32.MatchString(sent.GetTraceId()) {
				t.Errorf("trace_id = %q, want 32 hex chars", sent.GetTraceId())
			}

			spans := rec.all()
			if len(spans) != 1 {
				t.Fatalf("recorded %d spans, want 1", len(spans))
			}
			s := spans[0]
			if s.Name() != "customer.Lookup" {
				t.Errorf("span name = %q, want customer.Lookup", s.Name())
			}
			if s.SpanKind() != trace.SpanKindClient {
				t.Errorf("span kind = %v, want client", s.SpanKind())
			}
			if got, _ := spanAttr(s, "rpc.status"); got != tt.wantStatus {
				t.Errorf("rpc.status = %q, want %q", got, tt.wantStatus)
			}
			if s.Status().Code != tt.wantCode {
				t.Errorf("span status code = %v, want %v", s.Status().Code, tt.wantCode)
			}
			// The span's trace id must equal the one stamped on the wire.
			if s.SpanContext().TraceID().String() != sent.GetTraceId() {
				t.Errorf("span trace id %s != wire trace_id %s", s.SpanContext().TraceID(), sent.GetTraceId())
			}
		})
	}
}

func TestTracingHandlerContinuesCallerTrace(t *testing.T) {
	rec := withRecorder(t)

	// Client leg: produce a request carrying the caller's trace context.
	fake := &scriptedClient{outcomes: []outcome{{resp: respOK()}}}
	tc := NewTracingClient(fake)
	req, err := NewRequest("customer", "Lookup", &rpcv1.Request{}, time.Second)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if _, err := tc.Call(context.Background(), req); err != nil {
		t.Fatalf("client Call: %v", err)
	}
	wire := fake.seen[0]
	clientTraceID := wire.GetTraceId()

	// Server leg: the handler must run inside a span that continues the SAME
	// trace, extracted from the envelope metadata (§23).
	var serverTraceID trace.TraceID
	var serverParent trace.SpanID
	h := NewTracingHandler(HandlerFunc(func(ctx context.Context, r *rpcv1.Request) (*rpcv1.Response, error) {
		sc := trace.SpanContextFromContext(ctx)
		serverTraceID = sc.TraceID()
		serverParent = sc.SpanID()
		return respOK(), nil
	}))
	if _, err := h.Handle(context.Background(), wire); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	if serverTraceID.String() != clientTraceID {
		t.Errorf("server trace id %s != client trace id %s (trace not propagated)", serverTraceID, clientTraceID)
	}

	// Two spans total: the client span and the server span, sharing the trace id,
	// the server span a child of the client span.
	var client, server sdktrace.ReadOnlySpan
	for _, s := range rec.all() {
		switch s.SpanKind() {
		case trace.SpanKindClient:
			client = s
		case trace.SpanKindServer:
			server = s
		}
	}
	if client == nil || server == nil {
		t.Fatalf("want one client and one server span, got %d spans", len(rec.all()))
	}
	if server.Parent().SpanID() != client.SpanContext().SpanID() {
		t.Errorf("server span parent %s != client span %s (not nested)", server.Parent().SpanID(), client.SpanContext().SpanID())
	}
	if server.SpanContext().SpanID() != serverParent {
		t.Errorf("handler ctx span id %s != recorded server span id %s", serverParent, server.SpanContext().SpanID())
	}
}
