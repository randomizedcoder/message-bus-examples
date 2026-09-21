// Package grpcbus is the in-memory core of the gRPC-native message bus: a Broker
// that fans each published Message out to every live subscriber of its topic. It
// is transport-agnostic and independently testable — the grpcbrokerd command
// wraps it in a gRPC BrokerService, but the fan-out logic here has no gRPC types.
//
// Delivery is ephemeral (design: gRPC bus PR 1): a subscriber sees only messages
// published while it is subscribed, and one slow reader never stalls a publisher.
// Each subscriber owns a bounded buffered channel; Publish does a non-blocking
// send to each and DROPS (counting the drop) for any subscriber whose buffer is
// full, so a publisher's latency never depends on the slowest consumer.
package grpcbus

import (
	"sync"

	busv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/bus/v1"
)

// DefaultSubBuffer is the per-subscriber channel depth used when NewBroker is
// given a non-positive buffer size. It absorbs a bursty publisher ahead of a
// briefly-behind subscriber before drops begin.
const DefaultSubBuffer = 256

// subscriber is one live Subscribe stream's delivery channel. The channel is
// never closed (the broker only ever sends to it): the owning stream drains it
// and calls the cancel func to deregister, after which no further sends target
// it. Not closing sidesteps the send-on-closed-channel race between Publish and
// cancel entirely.
type subscriber struct {
	topic string
	id    string
	ch    chan *busv1.Message
}

// Broker holds the topic → subscribers registry and fans messages out.
// The zero value is not usable; call NewBroker.
type Broker struct {
	mu      sync.RWMutex
	subs    map[string]map[*subscriber]struct{}
	bufSize int
	metrics *Metrics // nil = metrics disabled (all record calls no-op)
}

// NewBroker returns an empty Broker. bufSize is the per-subscriber buffer depth
// (<= 0 uses DefaultSubBuffer); metrics may be nil to disable instrumentation.
func NewBroker(bufSize int, metrics *Metrics) *Broker {
	if bufSize <= 0 {
		bufSize = DefaultSubBuffer
	}
	return &Broker{
		subs:    make(map[string]map[*subscriber]struct{}),
		bufSize: bufSize,
		metrics: metrics,
	}
}

// Subscribe registers a subscriber on topic and returns its receive channel plus
// a cancel func. The channel yields every Message published to topic while the
// subscription is live; cancel deregisters it (idempotent). The channel is
// buffered and never closed — the caller stops reading once cancel returns.
func (b *Broker) Subscribe(topic, id string) (<-chan *busv1.Message, func()) {
	s := &subscriber{topic: topic, id: id, ch: make(chan *busv1.Message, b.bufSize)}

	b.mu.Lock()
	set := b.subs[topic]
	if set == nil {
		set = make(map[*subscriber]struct{})
		b.subs[topic] = set
	}
	set[s] = struct{}{}
	b.mu.Unlock()
	b.metrics.addSubscribers(1)

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.mu.Lock()
			if set := b.subs[topic]; set != nil {
				delete(set, s)
				if len(set) == 0 {
					delete(b.subs, topic)
				}
			}
			b.mu.Unlock()
			b.metrics.addSubscribers(-1)
		})
	}
	return s.ch, cancel
}

// Publish fans msg out to every current subscriber of msg.Topic and returns the
// number it was delivered to. Delivery is non-blocking: a subscriber whose buffer
// is full has this message dropped (counted), never blocking the publisher or
// holding the registry lock during the sends.
func (b *Broker) Publish(msg *busv1.Message) int {
	b.mu.RLock()
	set := b.subs[msg.GetTopic()]
	targets := make([]*subscriber, 0, len(set))
	for s := range set {
		targets = append(targets, s)
	}
	b.mu.RUnlock()

	delivered, dropped := 0, 0
	for _, s := range targets {
		select {
		case s.ch <- msg:
			delivered++
		default:
			dropped++
		}
	}
	b.metrics.recordPublish(delivered, dropped)
	return delivered
}

// Subscribers returns the current live subscriber count for topic (test/debug
// aid; not on any hot path).
func (b *Broker) Subscribers(topic string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs[topic])
}
