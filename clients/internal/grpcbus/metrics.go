// grpcbus metrics: the grpcbus_* instrument set for the broker. Built on an OTel
// meter (from internal/metrics.NewProvider) and exported in Prometheus text form,
// mirroring the RPC lab's rpcmetrics. The label set is deliberately EMPTY — never
// topic, publisher_id, or subscriber_id, any of which would be an unbounded
// cardinality source on a bus with free-form topics.
package grpcbus

import (
	"context"

	"go.opentelemetry.io/otel/metric"
)

// Metrics is the broker's instrument set. A nil *Metrics is a valid no-op, so the
// Broker can hold one whether or not metrics are enabled.
type Metrics struct {
	published metric.Int64Counter       // grpcbus_messages_published_total
	delivered metric.Int64Counter       // grpcbus_messages_delivered_total
	dropped   metric.Int64Counter       // grpcbus_messages_dropped_total
	subs      metric.Int64UpDownCounter // grpcbus_active_subscribers
}

// NewMetrics registers the grpcbus_* instruments on meter.
func NewMetrics(meter metric.Meter) (*Metrics, error) {
	m := &Metrics{}
	var err error
	if m.published, err = meter.Int64Counter("grpcbus_messages_published_total",
		metric.WithDescription("messages accepted by the broker for fan-out")); err != nil {
		return nil, err
	}
	if m.delivered, err = meter.Int64Counter("grpcbus_messages_delivered_total",
		metric.WithDescription("per-subscriber message deliveries (one publish fans out to N)")); err != nil {
		return nil, err
	}
	if m.dropped, err = meter.Int64Counter("grpcbus_messages_dropped_total",
		metric.WithDescription("deliveries dropped because a subscriber's buffer was full")); err != nil {
		return nil, err
	}
	if m.subs, err = meter.Int64UpDownCounter("grpcbus_active_subscribers",
		metric.WithDescription("currently connected Subscribe streams")); err != nil {
		return nil, err
	}
	return m, nil
}

// recordPublish counts one publish plus its fan-out and drop tallies.
func (m *Metrics) recordPublish(delivered, dropped int) {
	if m == nil {
		return
	}
	ctx := context.Background()
	m.published.Add(ctx, 1)
	if delivered > 0 {
		m.delivered.Add(ctx, int64(delivered))
	}
	if dropped > 0 {
		m.dropped.Add(ctx, int64(dropped))
	}
}

// addSubscribers moves the active-subscribers gauge by delta (+1 on subscribe,
// -1 on cancel).
func (m *Metrics) addSubscribers(delta int64) {
	if m == nil {
		return
	}
	m.subs.Add(context.Background(), delta)
}
