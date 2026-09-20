package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
	grpctransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/grpc"
)

// clockprobe estimates the wall-clock offset between the driver and a region
// agent with an NTP-style four-timestamp round (design §8.1). Over -probes
// requests it keeps the sample with the minimum RTT (the least-jittered one) and
// reports its offset; the uncertainty is that minimum RTT halved. The result is
// what gates one-way latency reporting: the harness only publishes one-way
// percentiles when uncertainty < 0.1 × one_way_p50.
//
//	offset      = ((t2 - t1) + (t3 - t4)) / 2
//	rtt         = (t4 - t1) - (t3 - t2)
//	uncertainty = rtt_min / 2
//
// t1 = client send, t2 = server receive, t3 = server send, t4 = client receive.
// t4 is read from the wall clock (not the monotonic clock) because it is
// compared against the server's wall-clock timestamps across machines.

// clockResult is the per-region clock estimate; it is also the JSON line the
// k8s-proto-bench harness folds into run.json's "clock" block (design §8.5).
type clockResult struct {
	Region        string `json:"region"`
	Probes        int    `json:"probes"`
	OK            int    `json:"ok"`
	OffsetNs      int64  `json:"offset_ns"`
	UncertaintyNs int64  `json:"uncertainty_ns"`
	RTTMinNs      int64  `json:"rtt_min_ns"`
	ClockSource   string `json:"clock_source"`
	ChronySynced  bool   `json:"chrony_synced"`
}

func runClockProbe(args []string) error {
	fs := flag.NewFlagSet("clockprobe", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:30710", "region-agent gRPC address (host:port)")
	region := fs.String("region", "us-west-2", "region label")
	n := fs.Int("probes", 200, "number of probes; the minimum-RTT sample wins (design §8.1)")
	codecName := fs.String("codec", "proto", "proto|protojson|vtproto")
	timeout := fs.Duration("timeout", 5*time.Second, "per-probe timeout")
	asJSON := fs.Bool("json", false, "emit the result as one JSON line (for the harness)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *n <= 0 {
		return fmt.Errorf("-probes must be > 0")
	}
	cdc, err := codec.ByName(*codecName)
	if err != nil {
		return err
	}
	opts := transport.Options{Codec: cdc, Pool: bufferPoolAll(), Region: *region}
	cc, err := grpctransport.Dial(*addr, opts, "none")
	if err != nil {
		return fmt.Errorf("dial %s: %w", *addr, err)
	}
	defer cc.Close()

	res, err := clockProbe(context.Background(), workloadsv1.NewBenchServiceClient(cc), *region, *n, *codecName, *timeout)
	if err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(res)
	}
	printClockResult(res)
	return nil
}

// clockProbe runs the probe loop and returns the winning sample's estimate.
func clockProbe(ctx context.Context, client workloadsv1.BenchServiceClient, region string, n int, codecName string, timeout time.Duration) (clockResult, error) {
	cen, err := codecEnum(codecName)
	if err != nil {
		return clockResult{}, err
	}
	res := clockResult{Region: region, Probes: n, RTTMinNs: math.MaxInt64}
	env := &workloadsv1.Envelope{}
	var errs int
	for i := 0; i < n; i++ {
		if err := envelope.Fill(env, "clockprobe", uint64(i), cen, workloadsv1.Transport_TRANSPORT_GRPC_UNARY, "tiny"); err != nil {
			return clockResult{}, err
		}
		t1 := env.GetClientSendTime() // Fill just set this; reuse it as t1 so the two readings cannot skew
		req := &workloadsv1.ClockProbeRequest{Envelope: env, T1: t1}
		pctx, cancel := context.WithTimeout(ctx, timeout)
		resp, err := client.ClockProbe(pctx, req)
		t4 := time.Now()
		cancel()
		if err != nil {
			errs++
			if errs <= 3 {
				fmt.Fprintf(os.Stderr, "probe %d: %v\n", i, err)
			}
			continue
		}
		offset, rtt, ok := probeSample(t1, resp.GetT2(), resp.GetT3(), t4)
		if !ok {
			errs++
			continue
		}
		res.OK++
		res.ClockSource = resp.GetClockSource()
		res.ChronySynced = resp.GetChronySynced()
		if rtt < res.RTTMinNs {
			res.RTTMinNs = rtt
			res.OffsetNs = offset
		}
	}
	if res.OK == 0 {
		return clockResult{}, fmt.Errorf("clockprobe %s: no successful probe out of %d", region, n)
	}
	res.UncertaintyNs = res.RTTMinNs / 2
	return res, nil
}

// probeSample computes the NTP offset and RTT for one round in nanoseconds. It
// returns ok=false when either server timestamp is missing. Values are never
// clamped — a negative offset is a real reading (design §8.1).
func probeSample(t1, t2, t3 *timestamppb.Timestamp, t4 time.Time) (offsetNs, rttNs int64, ok bool) {
	if t1 == nil || t2 == nil || t3 == nil {
		return 0, 0, false
	}
	t1ns := t1.AsTime().UnixNano()
	t2ns := t2.AsTime().UnixNano()
	t3ns := t3.AsTime().UnixNano()
	t4ns := t4.UnixNano()
	offsetNs = ((t2ns - t1ns) + (t3ns - t4ns)) / 2
	rttNs = (t4ns - t1ns) - (t3ns - t2ns)
	return offsetNs, rttNs, true
}

func printClockResult(r clockResult) {
	fmt.Printf("clockprobe %s: %d/%d probes ok\n", r.Region, r.OK, r.Probes)
	fmt.Printf("  offset       %s\n", time.Duration(r.OffsetNs))
	fmt.Printf("  uncertainty  %s (rtt_min %s)\n", time.Duration(r.UncertaintyNs), time.Duration(r.RTTMinNs))
	fmt.Printf("  clock_source %s (chrony_synced=%v)\n", r.ClockSource, r.ChronySynced)
}
