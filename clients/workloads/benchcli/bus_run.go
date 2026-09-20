package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/corpus"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/harness"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
	mqtttransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/mqtt"
	natstransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/nats"
	rmqtransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/rabbitmq"
	valkeytransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/valkey"
)

// runNATS drives the NATS request-reply Deploy RPC (design §3.9, tier C).
func runNATS(args []string) error {
	f := newBusFlags("nats", "127.0.0.1:30422", "medium")
	if err := f.fs.Parse(args); err != nil {
		return err
	}
	br, err := f.setup("nats_request_reply", workloadsv1.Transport_TRANSPORT_NATS_REQUEST_REPLY)
	if err != nil {
		return err
	}
	req, newResp, err := busPair(br.corpus, corpus.Fixture(*f.fixture))
	if err != nil {
		return err
	}
	nc, err := natstransport.Dial("nats://" + *f.addr)
	if err != nil {
		return fmt.Errorf("nats connect %s: %w", *f.addr, err)
	}
	br.requester = natstransport.NewRequester(nc, br.opts, *f.region)
	defer br.requester.Close()
	dial := func() (transport.Requester, func(), error) {
		c, err := natstransport.Dial("nats://" + *f.addr)
		if err != nil {
			return nil, nil, err
		}
		r := natstransport.NewRequester(c, br.opts, *f.region)
		return r, func() { r.Close(); c.Close() }, nil
	}
	return runAndPrint("nats_request_reply", *f.addr, br, req, newResp, f, dial)
}

// runRabbitMQ drives the RabbitMQ RPC Deploy flow (direct reply-to).
func runRabbitMQ(args []string) error {
	f := newBusFlags("rabbitmq", "127.0.0.1:30567", "medium")
	if err := f.fs.Parse(args); err != nil {
		return err
	}
	br, err := f.setup("rabbitmq_rpc", workloadsv1.Transport_TRANSPORT_RABBITMQ_RPC)
	if err != nil {
		return err
	}
	req, newResp, err := busPair(br.corpus, corpus.Fixture(*f.fixture))
	if err != nil {
		return err
	}
	url := fmt.Sprintf("amqp://%s:%s@%s/", *f.user, *f.pass, *f.addr)
	conn, err := rmqtransport.Dial(url)
	if err != nil {
		return fmt.Errorf("rabbitmq dial (check -user/-pass): %w", err)
	}
	defer conn.Close()
	br.requester, err = rmqtransport.NewRequester(conn, br.opts, *f.region)
	if err != nil {
		return err
	}
	defer br.requester.Close()
	dial := func() (transport.Requester, func(), error) {
		c, err := rmqtransport.Dial(url)
		if err != nil {
			return nil, nil, err
		}
		r, err := rmqtransport.NewRequester(c, br.opts, *f.region)
		if err != nil {
			c.Close()
			return nil, nil, err
		}
		return r, func() { r.Close(); c.Close() }, nil
	}
	return runAndPrint("rabbitmq_rpc", *f.addr, br, req, newResp, f, dial)
}

// runValkey drives the Valkey stream RPC Deploy flow (XADD/XREADGROUP/XACK).
func runValkey(args []string) error {
	f := newBusFlags("valkey", "127.0.0.1:30637", "medium")
	if err := f.fs.Parse(args); err != nil {
		return err
	}
	br, err := f.setup("valkey_stream", workloadsv1.Transport_TRANSPORT_VALKEY_STREAM)
	if err != nil {
		return err
	}
	req, newResp, err := busPair(br.corpus, corpus.Fixture(*f.fixture))
	if err != nil {
		return err
	}
	rdb := newValkeyClient(*f.addr, *f.sentinels, *f.pass)
	defer rdb.Close()
	clientID := fmt.Sprintf("benchcli-%d", os.Getpid())
	br.requester = valkeytransport.NewRequester(rdb, br.opts, *f.region, clientID)
	defer br.requester.Close()
	dial := func() (transport.Requester, func(), error) {
		db := newValkeyClient(*f.addr, *f.sentinels, *f.pass)
		r := valkeytransport.NewRequester(db, br.opts, *f.region, clientID)
		return r, func() { r.Close(); db.Close() }, nil
	}
	return runAndPrint("valkey_stream", *f.addr, br, req, newResp, f, dial)
}

// runMQTT drives the MQTT one-way telemetry publish loop (tier A). There is no
// reply, so it reports publish latency; correctness is verified agent-side
// (design §9.4). The password/pass flags are unused (cluster MQTT is open).
func runMQTT(args []string) error {
	f := newBusFlags("mqtt", "127.0.0.1:30883", "telemetry")
	if err := f.fs.Parse(args); err != nil {
		return err
	}
	if *f.mode != modeLatency {
		return fmt.Errorf("mqtt is fire-and-forget publish; only -mode=latency is supported (got %q)", *f.mode)
	}
	br, err := f.setup("mqtt", workloadsv1.Transport_TRANSPORT_MQTT)
	if err != nil {
		return err
	}
	mc, err := mqtttransport.Dial(*f.addr, "benchcli")
	if err != nil {
		return err
	}
	pub := mqtttransport.NewPublisher(mc, br.opts, *f.region, byte(*f.qos))
	defer pub.Close()

	ctx := context.Background()
	br.inst.SetActiveCell(ctx, br.cell, true)
	defer br.inst.SetActiveCell(ctx, br.cell, false)
	sample := br.corpus.Telemetry(3)
	env := sample.GetEnvelope()
	lat := harness.NewHDR()
	start := time.Now()
	for i := 0; i < *f.n; i++ {
		if err := envelope.Fill(env, *f.runID, uint64(i), br.cenum, br.tenum, "telemetry"); err != nil {
			lat.AddErrorKind("encode")
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, *f.timeout)
		t0 := time.Now()
		rel, err := pub.Publish(pctx, sample)
		d := time.Since(t0)
		cancel()
		br.inst.Message(ctx, br.cell, harness.ResultSent)
		if err != nil {
			lat.AddErrorKind("transport")
			br.inst.Error(ctx, br.cell, "transport")
			if lat.NumErrors() <= 3 {
				fmt.Fprintf(os.Stderr, "publish %d: %v\n", i, err)
			}
			continue
		}
		rel()
		br.inst.Message(ctx, br.cell, harness.ResultReceived)
		br.inst.RTT(ctx, br.cell, d)
		lat.Record(d)
	}
	elapsed := time.Since(start)
	printBusSummary("mqtt (publish)", *f.addr, br.cell, lat.Summarize(elapsed), elapsed)
	return emitCell(f.emit(), br.cell, cellResult{hdr: lat, elapsed: elapsed, wireReq: wireLen(br.opts.Codec, sample), inflight: 1})
}

// runAndPrint runs the configured run mode over the request/reply Requester,
// prints the summary, and (when the harness passed -out/-hgrm) emits the
// per-cell record + histogram (design §8.2). dial is the fresh-connection
// factory used only by coldstart (nil for a transport that cannot re-dial).
func runAndPrint(kind, addr string, br *busRun, req proto.Message, newResp func() proto.Message, f *busFlags, dial coldDial) error {
	rc := &reqCell{
		requester: br.requester, dial: dial, reqProto: req, newResp: newResp,
		cenum: br.cenum, tenum: br.tenum, inst: br.inst, cell: br.cell,
	}
	res, err := drive(context.Background(), rc, f.driveConfig(), wireLen(br.opts.Codec, req))
	if err != nil {
		return err
	}
	printBusSummary(kind, addr, br.cell, res.hdr.Summarize(res.elapsed), res.elapsed)
	printRunExtras(res)
	return emitCell(f.emit(), br.cell, res)
}

// newValkeyClient builds a Sentinel FailoverClient when sentinels is set, else a
// direct client to addr (mirrors valkeycli, design §6).
func newValkeyClient(addr, sentinels, pass string) *redis.Client {
	if sentinels != "" {
		var addrs []string
		for _, s := range strings.Split(sentinels, ",") {
			if s = strings.TrimSpace(s); s != "" {
				addrs = append(addrs, s)
			}
		}
		return redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName: "mymaster", SentinelAddrs: addrs, Password: pass,
			MaxRetries: -1, MinRetryBackoff: 200 * time.Millisecond,
		})
	}
	return redis.NewClient(&redis.Options{Addr: addr, Password: pass, MaxRetries: -1, MinRetryBackoff: 200 * time.Millisecond})
}

func printBusSummary(kind, addr string, cell harness.Cell, s harness.Summary, elapsed time.Duration) {
	fmt.Printf("%s → %s  codec=%s fixture=%s pool=%s\n", kind, addr, cell.Codec, cell.Fixture, cell.Pool)
	fmt.Printf("  messages        %d (errors %d)\n", s.Count+s.Errors, s.Errors)
	fmt.Printf("  throughput      %.0f msg/s over %s\n", s.ThroughputPerSec, elapsed.Round(time.Millisecond))
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintf(tw, "  RTT\tmin\tp50\tp90\tp99\tp99.9\tmax\tmean\n")
	fmt.Fprintf(tw, "  \t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
		d(s.Min), d(s.P50), d(s.P90), d(s.P99), d(s.P999), d(s.Max), d(s.Mean))
	tw.Flush()
}
