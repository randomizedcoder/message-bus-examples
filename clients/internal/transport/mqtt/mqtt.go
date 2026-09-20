// Package mqtttransport is the MQTT (Mosquitto) binding of the proto-bench
// transport interfaces (design §3.9, §7.5). MQTT is one-way only: the driver
// Publishes TelemetryBatch on `wl/<region>/telemetry` (QoS 0 and QoS 1 as
// separate profiles, `/json` suffix for ProtoJSON) and the agent's Consumer
// receives, decodes, integrity-checks and validates — there is no reply, so
// MQTT correctness is a consumer-side property (design §9.4).
//
// Buffer handling (§7.5): paho does NOT copy the payload synchronously — the
// packet is queued and written by the writer goroutine, and QoS>0 packets are
// held by the store until PUBACK — so a pooled buffer is safe to return only
// after token.Done(). Publish therefore waits for the token before freeing the
// buffer and returning; a full client write queue is the natural backpressure.
package mqtttransport

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"buf.build/go/protovalidate"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
)

// Topic is the telemetry topic for a region; ProtoJSON gets a `/json` suffix so
// the consumer can pick the codec from the topic (design §3.9).
func Topic(region string, c codec.Codec) string {
	t := "wl/" + region + "/telemetry"
	if c != nil && c.Name() == "protojson" {
		t += "/json"
	}
	return t
}

// Dial connects a paho client to addr (host:port) with a unique client id.
func Dial(addr, idPrefix string) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://" + addr).
		SetClientID(fmt.Sprintf("%s-%d", idPrefix, os.Getpid())).
		SetAutoReconnect(true).
		SetConnectRetry(true)
	c := mqtt.NewClient(opts)
	tok := c.Connect()
	if !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
		return nil, fmt.Errorf("mqtt connect %s: %w", addr, tok.Error())
	}
	return c, nil
}

// ─── Publisher (client, one-way) ─────────────────────────────────────────────

type publisher struct {
	c     mqtt.Client
	opts  transport.Options
	topic string
	qos   byte
}

// NewPublisher publishes to region's telemetry topic at the given QoS.
func NewPublisher(c mqtt.Client, opts transport.Options, region string, qos byte) transport.Publisher {
	return &publisher{c: c, opts: opts, topic: Topic(region, opts.Codec), qos: qos}
}

func (p *publisher) Publish(ctx context.Context, m proto.Message) (transport.Release, error) {
	buf, err := codec.Encode(p.opts.Codec, p.opts.Pool, m)
	if err != nil {
		return nil, err
	}
	tok := p.c.Publish(p.topic, p.qos, false, *buf)
	select {
	case <-ctx.Done():
		p.opts.Pool.Put(buf) // ctx cancelled; the client already owns/queued the packet
		return func() {}, ctx.Err()
	case <-tok.Done():
		p.opts.Pool.Put(buf) // safe now: paho is done with the payload (§7.5)
		return func() {}, tok.Error()
	}
}

func (p *publisher) Close() error { p.c.Disconnect(250); return nil }

// ─── Consumer (server, one-way) ──────────────────────────────────────────────

type msg struct {
	payload []byte
	cdc     codec.Codec
}

func (m *msg) Bytes() []byte      { return m.payload }
func (m *msg) Ack() error         { return nil } // paho acks QoS>0 at the protocol layer
func (m *msg) Codec() codec.Codec { return m.cdc }

type consumer struct {
	c      mqtt.Client
	opts   transport.Options
	region string
	qos    byte
}

// NewConsumer subscribes to region's telemetry topics (proto + json).
func NewConsumer(c mqtt.Client, opts transport.Options, region string, qos byte) transport.Consumer {
	return &consumer{c: c, opts: opts, region: region, qos: qos}
}

func (c *consumer) Consume(ctx context.Context, fn func(transport.Msg) error) error {
	filters := map[string]byte{
		"wl/" + c.region + "/telemetry":      c.qos,
		"wl/" + c.region + "/telemetry/json": c.qos,
	}
	tok := c.c.SubscribeMultiple(filters, func(_ mqtt.Client, m mqtt.Message) {
		cdc := codec.Proto
		if strings.HasSuffix(m.Topic(), "/json") {
			cdc = codec.ProtoJSON
		}
		_ = fn(&msg{payload: m.Payload(), cdc: cdc})
	})
	if !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
		return fmt.Errorf("mqtt subscribe: %w", tok.Error())
	}
	<-ctx.Done()
	c.c.Unsubscribe("wl/"+c.region+"/telemetry", "wl/"+c.region+"/telemetry/json")
	return nil
}

func (c *consumer) Close() error { c.c.Disconnect(250); return nil }

// Decode is the consumer-side helper the agent uses to turn a delivered Msg into
// a request message, receive-stamp it, and optionally validate — the one-way
// analogue of a Responder's decode half (there is no reply to send).
func Decode(m transport.Msg, newMsg func() proto.Message, responderID string, opts transport.Options) (proto.Message, error) {
	req := newMsg()
	if err := m.Codec().Unmarshal(m.Bytes(), req); err != nil {
		return nil, err
	}
	if env := envelope.Of(req); env != nil {
		envelope.StampReceive(env, responderID, m.Bytes(), opts.Integrity)
	}
	if opts.Validate {
		if err := protovalidate.Validate(req); err != nil {
			return req, err
		}
	}
	return req, nil
}
