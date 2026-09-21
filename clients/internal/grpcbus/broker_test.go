package grpcbus

import (
	"testing"
	"time"

	busv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/bus/v1"
)

// msg is a tiny Message constructor for the tests.
func msg(topic, body string) *busv1.Message {
	return &busv1.Message{Topic: topic, Payload: []byte(body)}
}

// recv reads one message from ch with a short deadline, reporting whether one
// arrived. It keeps the tests from hanging when a message is (correctly) not
// delivered.
func recv(t *testing.T, ch <-chan *busv1.Message) (*busv1.Message, bool) {
	t.Helper()
	select {
	case m := <-ch:
		return m, true
	case <-time.After(200 * time.Millisecond):
		return nil, false
	}
}

// TestPublishDeliveredCount covers the fan-out arithmetic: how many live
// subscribers of the published topic a message reaches. Table-driven over the
// positive (one/many same-topic), negative (other topic, no subscribers), and
// mixed (some match, some don't) cases.
func TestPublishDeliveredCount(t *testing.T) {
	type sub struct{ topic, id string }
	tests := []struct {
		description string
		subs        []sub
		publishTo   string
		expected    int // subscribers the message should be delivered to
	}{
		{
			description: "no subscribers at all → delivered to none",
			subs:        nil,
			publishTo:   "demo",
			expected:    0,
		},
		{
			description: "one subscriber on the topic → delivered to it",
			subs:        []sub{{"demo", "a"}},
			publishTo:   "demo",
			expected:    1,
		},
		{
			description: "two subscribers on the same topic → fan-out to both",
			subs:        []sub{{"demo", "a"}, {"demo", "b"}},
			publishTo:   "demo",
			expected:    2,
		},
		{
			description: "only subscriber is on a different topic → delivered to none (exact match)",
			subs:        []sub{{"other", "a"}},
			publishTo:   "demo",
			expected:    0,
		},
		{
			description: "mixed topics → only the exact-topic subscribers count",
			subs:        []sub{{"demo", "a"}, {"other", "b"}, {"demo", "c"}},
			publishTo:   "demo",
			expected:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			b := NewBroker(4, nil)
			for _, s := range tt.subs {
				_, cancel := b.Subscribe(s.topic, s.id)
				defer cancel()
			}
			got := b.Publish(msg(tt.publishTo, "hello"))
			if got != tt.expected {
				t.Fatalf("%s: delivered = %d, want %d", tt.description, got, tt.expected)
			}
		})
	}
}

// TestSubscribeReceivesFanOut verifies the payload actually lands on every
// same-topic subscriber's channel and NOT on a different-topic subscriber's.
func TestSubscribeReceivesFanOut(t *testing.T) {
	b := NewBroker(4, nil)
	chA, cancelA := b.Subscribe("demo", "a")
	defer cancelA()
	chB, cancelB := b.Subscribe("demo", "b")
	defer cancelB()
	chOther, cancelOther := b.Subscribe("other", "c")
	defer cancelOther()

	b.Publish(msg("demo", "payload-1"))

	for name, ch := range map[string]<-chan *busv1.Message{"A": chA, "B": chB} {
		m, ok := recv(t, ch)
		if !ok {
			t.Fatalf("subscriber %s: expected a message, got none", name)
		}
		if got := string(m.GetPayload()); got != "payload-1" {
			t.Fatalf("subscriber %s: payload = %q, want %q", name, got, "payload-1")
		}
	}
	if _, ok := recv(t, chOther); ok {
		t.Fatal("subscriber on other topic should not have received the demo message")
	}
}

// TestCancelDeregisters covers the corner case that a cancelled subscription no
// longer receives, and that the topic's subscriber count drops — including that
// cancel is idempotent (double-cancel must not panic or double-decrement).
func TestCancelDeregisters(t *testing.T) {
	b := NewBroker(4, nil)
	ch, cancel := b.Subscribe("demo", "a")

	if got := b.Subscribers("demo"); got != 1 {
		t.Fatalf("after subscribe: Subscribers = %d, want 1", got)
	}
	cancel()
	cancel() // idempotent — must not panic or over-decrement
	if got := b.Subscribers("demo"); got != 0 {
		t.Fatalf("after cancel: Subscribers = %d, want 0", got)
	}

	if got := b.Publish(msg("demo", "after-cancel")); got != 0 {
		t.Fatalf("publish after cancel: delivered = %d, want 0", got)
	}
	if _, ok := recv(t, ch); ok {
		t.Fatal("cancelled subscriber should not receive further messages")
	}
}

// TestSlowSubscriberDrops covers the boundary case that a full buffer drops
// without blocking the publisher: with buffer=2, the 3rd unread message is
// dropped (delivered=0 for it) yet Publish returns promptly, and the first two
// remain readable.
func TestSlowSubscriberDrops(t *testing.T) {
	b := NewBroker(2, nil)
	ch, cancel := b.Subscribe("demo", "slow")
	defer cancel()

	// Fill the buffer (2), then overflow by one — the reader never drains.
	if got := b.Publish(msg("demo", "m1")); got != 1 {
		t.Fatalf("m1 delivered = %d, want 1", got)
	}
	if got := b.Publish(msg("demo", "m2")); got != 1 {
		t.Fatalf("m2 delivered = %d, want 1", got)
	}

	done := make(chan int, 1)
	go func() { done <- b.Publish(msg("demo", "m3")) }() // must not block
	select {
	case got := <-done:
		if got != 0 {
			t.Fatalf("m3 (buffer full) delivered = %d, want 0 (dropped)", got)
		}
	case <-time.After(time.Second):
		t.Fatal("publish blocked on a full subscriber buffer (should drop, not block)")
	}

	// The two buffered messages are still delivered in order; m3 was dropped.
	for _, want := range []string{"m1", "m2"} {
		m, ok := recv(t, ch)
		if !ok {
			t.Fatalf("expected buffered %q, got none", want)
		}
		if got := string(m.GetPayload()); got != want {
			t.Fatalf("buffered payload = %q, want %q", got, want)
		}
	}
	if _, ok := recv(t, ch); ok {
		t.Fatal("m3 should have been dropped, but a third message arrived")
	}
}
