// Command benchcli is the host-side driver for the proto-bench benchmark.
//
// P1 implements the `codec` subcommand: an in-process codec × fixture × pool
// loop (no transport) that exercises exactly the same codec + pool code the
// transports use, plus the schema demos (--sizes, --validate-demo,
// --antipattern). Transport subcommands (grpc/nats/rabbitmq/valkey/mqtt/
// clockprobe/report) arrive in later phases.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"text/tabwriter"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"

	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/corpus"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/pool"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "codec":
		if err := runCodec(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "benchcli codec:", err)
			os.Exit(1)
		}
	case "grpc":
		if err := runGRPC(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "benchcli grpc:", err)
			os.Exit(1)
		}
	case "nats", "jetstream", "rabbitmq", "quorum", "valkey", "stream", "mqtt", "logs":
		run := map[string]func([]string) error{
			"nats": runNATS, "jetstream": runJetStream, "rabbitmq": runRabbitMQ,
			"quorum": runQuorum, "valkey": runValkey, "stream": runStream, "mqtt": runMQTT,
			"logs": runLogsFanout,
		}[os.Args[1]]
		if err := run(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "benchcli %s: %v\n", os.Args[1], err)
			os.Exit(1)
		}
	case "correctness":
		if err := runCorrectness(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "benchcli correctness:", err)
			os.Exit(1)
		}
	case "clockprobe":
		if err := runClockProbe(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "benchcli clockprobe:", err)
			os.Exit(1)
		}
	case "report":
		if err := runReport(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "benchcli report:", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: benchcli <codec|grpc|nats|jetstream|rabbitmq|quorum|valkey|stream|mqtt|logs|correctness|clockprobe|report> [flags]")
	fmt.Fprintln(os.Stderr, "  codec        in-process codec/pool loop and schema demos (--sizes, --validate-demo, --antipattern)")
	fmt.Fprintln(os.Stderr, "  grpc|nats|…  transport run-mode loop against a region-agent (-mode latency|windowed|openloop|saturation|coldstart; §8.2)")
	fmt.Fprintln(os.Stderr, "  jetstream    NATS JetStream durable telemetry publish→ack loop (tier B, one-way; -mode latency)")
	fmt.Fprintln(os.Stderr, "  quorum       RabbitMQ quorum-queue durable telemetry publish→confirm loop (tier B, one-way; -mode latency)")
	fmt.Fprintln(os.Stderr, "  stream       Valkey stream durable telemetry XADD loop (tier B, one-way; -mode latency)")
	fmt.Fprintln(os.Stderr, "  logs         NATS JetStream logs fan-out: publish→ack while N ephemeral push subscribers each receive every chunk (§3.9; -mode latency)")
	fmt.Fprintln(os.Stderr, "  correctness  assert codec round-trip + validation (+ live transport with -transport); the §9.4 gate")
	fmt.Fprintln(os.Stderr, "  clockprobe   NTP-style clock-offset estimate against a region-agent (-json for the harness)")
	fmt.Fprintln(os.Stderr, "  report       render run.json → results.tsv + results.md (-run <run.json>)")
}

func runCodec(args []string) error {
	fs := flag.NewFlagSet("codec", flag.ExitOnError)
	codecName := fs.String("codec", "proto", "proto|protojson|vtproto")
	fixtureName := fs.String("fixture", "medium", "fixture name or 'all'")
	poolName := fs.String("pool", "all", "none|messages|buffers|all")
	n := fs.Int("n", 100000, "iterations for the in-process loop")
	seed := fs.Uint64("seed", 42, "corpus seed")
	sizes := fs.Bool("sizes", false, "print fixture × codec wire sizes and ProtoJSON special forms")
	validateDemo := fs.Bool("validate-demo", false, "show a valid message passing and each mutation's constraint id")
	antipattern := fs.Bool("antipattern", false, "show encoding/json on a generated struct (wrong) vs ProtoJSON")
	if err := fs.Parse(args); err != nil {
		return err
	}

	c := corpus.New(*seed)
	switch {
	case *sizes:
		return printSizes(c)
	case *validateDemo:
		return printValidateDemo(c)
	case *antipattern:
		return printAntipattern(c)
	}

	cdc, err := codec.ByName(*codecName)
	if err != nil {
		return err
	}
	mode, err := pool.ParseMode(*poolName)
	if err != nil {
		return err
	}
	return runLoop(c, cdc, mode, corpus.Fixture(*fixtureName), *n)
}

func runLoop(c *corpus.Corpus, cdc codec.Codec, mode pool.Mode, fixture corpus.Fixture, n int) error {
	msg, err := c.Message(fixture)
	if err != nil {
		return err
	}
	bp := mode.NewBufferPool()

	// Warm the pool, then measure alloc deltas exactly (design §7.6).
	if b, err := codec.Encode(cdc, bp, msg); err == nil {
		bp.Put(b)
	}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	// With message pooling on, reuse one decode target and reset it each
	// iteration (proto.Reset for the official codecs; ResetVT for vtproto,
	// which keeps nested slices for UnmarshalVT to reuse — design §7.1).
	var target proto.Message
	if mode.MessagesEnabled() {
		target = msg.ProtoReflect().New().Interface()
	}
	start := time.Now()
	for i := 0; i < n; i++ {
		buf, err := codec.Encode(cdc, bp, msg)
		if err != nil {
			return fmt.Errorf("encode: %w", err)
		}
		if mode.MessagesEnabled() {
			resetMessage(target)
		} else {
			target = msg.ProtoReflect().New().Interface()
		}
		if err := cdc.Unmarshal(*buf, target); err != nil {
			return fmt.Errorf("decode: %w", err)
		}
		bp.Put(buf)
	}
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)

	wire := len(mustMarshal(cdc, msg))
	mallocs := after.Mallocs - before.Mallocs
	fmt.Printf("codec=%s fixture=%s pool=%s n=%d\n", cdc.Name(), fixture, mode, n)
	fmt.Printf("  wire_bytes      %d\n", wire)
	fmt.Printf("  ns/op (rt)      %d\n", elapsed.Nanoseconds()/int64(n))
	fmt.Printf("  allocs/op       %.2f\n", float64(mallocs)/float64(n))
	if b, ok := bp.(*pool.Buffers); ok {
		s := b.Stats()
		fmt.Printf("  pool hits/miss/drop  %d / %d / %d\n", s.Hits, s.Misses, s.Drops)
	}
	return nil
}

func printSizes(c *corpus.Corpus) error {
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "fixture\tproto\tprotojson\tvtproto")
	for _, f := range corpus.AllFixtures {
		msg, err := c.Message(f)
		if err != nil {
			return err
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\n", f,
			len(mustMarshal(codec.Proto, msg)),
			len(mustMarshal(codec.ProtoJSON, msg)),
			len(mustMarshal(codec.VT, msg)))
	}
	tw.Flush()

	// ProtoJSON special forms on a representative message.
	fmt.Println("\nProtoJSON special forms (dense DeployRequest):")
	dense := c.Deploy(corpus.Dense)
	fmt.Println(string(mustMarshal(codec.ProtoJSON, dense)))
	return nil
}

func printValidateDemo(c *corpus.Corpus) error {
	dense := c.Deploy(corpus.Dense)
	if err := protovalidate.Validate(dense); err != nil {
		return fmt.Errorf("dense fixture unexpectedly failed validation: %w", err)
	}
	fmt.Println("valid: dense DeployRequest passes protovalidate")
	fmt.Println("\nmutations (each violates exactly one rule):")
	for _, ic := range c.InvalidCases() {
		err := protovalidate.Validate(ic.Message)
		fmt.Printf("  %-32s -> %s\n", ic.Description, compact(err))
	}
	return nil
}

func printAntipattern(c *corpus.Corpus) error {
	// encoding/json on a generated struct is WRONG (oneof wrappers, int64 as
	// number, Timestamp as a struct). This is the only place encoding/json
	// touches generated types; internal/codec never imports it (design rule 3).
	req := c.Deploy(corpus.Small)
	bad, err := json.Marshal(req)
	if err != nil {
		return err
	}
	fmt.Println("encoding/json (WRONG):")
	fmt.Println(string(bad))
	fmt.Println("\nprotojson (correct):")
	fmt.Println(string(mustMarshal(codec.ProtoJSON, req)))
	return nil
}

// resetMessage clears a decode target for reuse. ResetVT (vtproto) keeps the
// nested slices/sub-messages so UnmarshalVT reuses them; proto.Reset drops them
// (the official-library ceiling, design §7.1).
func resetMessage(m proto.Message) {
	if v, ok := m.(interface{ ResetVT() }); ok {
		v.ResetVT()
		return
	}
	proto.Reset(m)
}

func mustMarshal(c codec.Codec, m proto.Message) []byte {
	b, err := c.MarshalAppend(nil, m)
	if err != nil {
		panic(err)
	}
	return b
}

// compact renders a (possibly multi-line) validation error on one line so the
// field path and constraint id are visible in the demo output.
func compact(err error) string {
	if err == nil {
		return "OK (unexpected)"
	}
	out := make([]byte, 0, len(err.Error()))
	prevSpace := false
	for _, r := range err.Error() {
		if r == '\n' || r == '\t' {
			r = ' '
		}
		if r == ' ' {
			if prevSpace {
				continue
			}
			prevSpace = true
		} else {
			prevSpace = false
		}
		out = append(out, string(r)...)
	}
	return string(out)
}
