package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"google.golang.org/protobuf/proto"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/harness"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
	natstransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/nats"
)

func newLogChunk() proto.Message { return &workloadsv1.LogChunk{} }

// runLogsFanout drives the NATS JetStream logs fan-out profile (design §2.2,
// §3.9). It plays the regional log source — publishing -n LogChunks on
// `wl.<region>.logs.<workload_id>` (publish→persist-ack, the headline latency) —
// while -subscribers ephemeral push consumers each attach their own consumer and
// receive every chunk. Because all subscribers and the publisher live in this
// one process, fan-out delivery latency is measured on the monotonic clock
// (publish→delivered) with no cross-host clock dependency, and the total
// deliveries (== ok × subscribers when nothing is lost) is the fan-out
// correctness signal.
//
// The chunk size is a flag (default 256 KiB) rather than the 960 KiB `max`
// fixture so every codec stays under NATS's 1 MB max_payload; only proto/vtproto
// fit the full `max` chunk (design §3.8 note).
func runLogsFanout(args []string) error {
	f := newBusFlags("logs", "127.0.0.1:30422", "max")
	subs := f.fs.Int("subscribers", 3, "fan-out subscribers (each an ephemeral push consumer receiving every chunk)")
	chunkKiB := f.fs.Int("chunk-kib", 256, "log-chunk payload size in KiB (kept under NATS's 1 MB max_payload; the max fixture is 960 KiB, which only proto/vtproto fit)")
	workloadID := f.fs.String("workload-id", "wl-fanout", "workload_id segment of the log subject")
	if err := f.fs.Parse(args); err != nil {
		return err
	}
	if *f.mode != modeLatency {
		return fmt.Errorf("logs is a fan-out publish loop; only -mode=latency is supported (got %q)", *f.mode)
	}
	if *subs < 1 {
		return fmt.Errorf("-subscribers must be >= 1 (got %d)", *subs)
	}
	if *chunkKiB < 1 {
		return fmt.Errorf("-chunk-kib must be >= 1 (got %d)", *chunkKiB)
	}
	br, err := f.setup("nats_jetstream", workloadsv1.Transport_TRANSPORT_NATS_JETSTREAM)
	if err != nil {
		return err
	}
	br.cell.Mode = "fanout" // distinguish fan-out cells from the telemetry publish loop

	nc, err := natstransport.Dial("nats://" + *f.addr)
	if err != nil {
		return fmt.Errorf("nats connect %s: %w", *f.addr, err)
	}
	defer nc.Close()
	js, err := natstransport.JetStream(nc)
	if err != nil {
		return fmt.Errorf("jetstream context: %w", err)
	}
	if err := natstransport.EnsureLogsStream(js); err != nil {
		return fmt.Errorf("ensure stream %s: %w", natstransport.LogsStreamName, err)
	}

	ctx := context.Background()
	br.inst.SetActiveCell(ctx, br.cell, true)
	defer br.inst.SetActiveCell(ctx, br.cell, false)

	// Fan-out bookkeeping: publish time by sequence (monotonic, via time.Since a
	// fixed base so it is immune to wall-clock steps), a delivered counter, and a
	// mutex-guarded HDR for the publish→delivered fan-out latency.
	base := time.Now()
	t0 := make([]atomic.Int64, *f.n)
	var delivered atomic.Int64
	fan := harness.NewHDR()
	var fanMu sync.Mutex

	onMsg := func(m transport.Msg) {
		req, derr := natstransport.Decode(m, newLogChunk, *f.region+"/subscriber", br.opts)
		br.inst.Message(ctx, br.cell, harness.ResultReceived)
		if derr != nil {
			br.inst.Error(ctx, br.cell, "validate")
			return
		}
		delivered.Add(1)
		if env := envelope.Of(req); env != nil {
			if seq := env.GetSequence(); int(seq) < len(t0) {
				if raw := t0[seq].Load(); raw > 0 {
					fanMu.Lock()
					fan.Record(time.Since(base) - time.Duration(raw-1))
					fanMu.Unlock()
				}
			}
		}
	}

	subsList := make([]*natstransport.LogSubscription, 0, *subs)
	closeSubs := func() {
		for _, s := range subsList {
			_ = s.Close()
		}
	}
	for i := 0; i < *subs; i++ {
		s, err := natstransport.SubscribeLogs(js, *f.region, *workloadID, onMsg)
		if err != nil {
			closeSubs()
			return fmt.Errorf("subscribe %d/%d: %w", i+1, *subs, err)
		}
		subsList = append(subsList, s)
	}
	defer closeSubs()

	pub := natstransport.NewLogPublisher(js, br.opts, *f.region, *workloadID)
	defer pub.Close()

	chunk := br.corpus.LogChunkBytes(*chunkKiB << 10)
	env := chunk.GetEnvelope()
	lat := harness.NewHDR()
	var ok int64
	start := time.Now()
	for i := 0; i < *f.n; i++ {
		if err := envelope.Fill(env, *f.runID, uint64(i), br.cenum, br.tenum, *f.fixture); err != nil {
			lat.AddErrorKind("encode")
			continue
		}
		t0[i].Store(int64(time.Since(base)) + 1) // +1 so 0 stays the "unset" sentinel
		pctx, cancel := context.WithTimeout(ctx, *f.timeout)
		tp := time.Now()
		rel, err := pub.Publish(pctx, chunk)
		d := time.Since(tp)
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
		ok++
		br.inst.Message(ctx, br.cell, harness.ResultOK)
		br.inst.RTT(ctx, br.cell, d)
		lat.Record(d)
	}
	elapsed := time.Since(start)

	// Wait for the fan-out to drain: every subscriber should receive every
	// published chunk. Give up after a quiet window so a lost message never hangs
	// the run (loss then shows in the deliveries line).
	expected := ok * int64(*subs)
	waitForFanout(&delivered, expected, 15*time.Second)

	printBusSummary("logs fan-out (publish→ack)", *f.addr, br.cell, lat.Summarize(elapsed), elapsed)
	printLogsFanout(*subs, *chunkKiB, expected, delivered.Load(), fan, elapsed)
	return emitCell(f.emit(), br.cell, cellResult{hdr: lat, elapsed: elapsed, wireReq: wireLen(br.opts.Codec, chunk), inflight: 1})
}

// waitForFanout blocks until delivered reaches expected, or until deliveries
// have been quiet for 3s (a lost chunk), or the hard timeout elapses.
func waitForFanout(delivered *atomic.Int64, expected int64, timeout time.Duration) {
	if expected <= 0 {
		return
	}
	deadline := time.Now().Add(timeout)
	last := delivered.Load()
	lastProgress := time.Now()
	for {
		got := delivered.Load()
		if got >= expected {
			return
		}
		if got != last {
			last = got
			lastProgress = time.Now()
		}
		if time.Since(lastProgress) > 3*time.Second || time.Now().After(deadline) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// printLogsFanout prints the fan-out tail: subscriber count, delivery total vs
// expected (the loss check), fan-out throughput, and publish→delivered latency
// percentiles.
func printLogsFanout(subs, chunkKiB int, expected, delivered int64, fan *harness.HDR, elapsed time.Duration) {
	loss := expected - delivered
	pct := 0.0
	if expected > 0 {
		pct = 100 * float64(loss) / float64(expected)
	}
	fmt.Printf("  fan-out         %d subscribers × %d KiB chunks\n", subs, chunkKiB)
	fmt.Printf("  deliveries      %d / %d expected (loss %d, %.2f%%)\n", delivered, expected, loss, pct)
	if secs := elapsed.Seconds(); secs > 0 {
		fmt.Printf("  fan-out tput    %.0f deliveries/s over %s\n", float64(delivered)/secs, elapsed.Round(time.Millisecond))
	}
	s := fan.Summarize(elapsed)
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintf(tw, "  fan-out delivery\tp50\tp90\tp99\tmax\n")
	fmt.Fprintf(tw, "  \t%s\t%s\t%s\t%s\n", d(s.P50), d(s.P90), d(s.P99), d(s.Max))
	tw.Flush()
}
