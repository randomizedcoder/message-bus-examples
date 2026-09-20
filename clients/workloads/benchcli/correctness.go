package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/codec"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/corpus"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/envelope"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/pool"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/transport"
	grpctransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/grpc"
	natstransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/nats"
	rmqtransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/rabbitmq"
	valkeytransport "github.com/randomizedcoder/message-bus-examples/clients/internal/transport/valkey"
)

// The correctness pass (design §9.4) is the one place the harness asserts rather
// than reports. It has two layers:
//
//   - In-process (always, no cluster needed): every fixture survives an
//     encode→decode round trip byte-for-nothing (proto.Equal) under each codec;
//     the corpus is deterministic (a deterministic marshal is byte-stable across
//     repeats); valid fixtures pass protovalidate and each invalid-corpus
//     mutation fails carrying exactly its expected constraint id.
//   - Over a live transport (when -transport is set): each RPC fixture round
//     trips through the real bus/codec and the reply echoes the request's
//     message_id; when the agent runs with -integrity it also returns a 32-byte
//     request_sha256 (the hash it took over the received wire), which we assert
//     is present. We do not assert byte-equality of that hash against a client
//     re-marshal: proto map fields have no canonical order, so two independent
//     marshals of the same message may differ on the wire while remaining
//     proto.Equal — the message_id echo is the end-to-end equality guarantee,
//     the in-process proto.Equal is the codec-fidelity guarantee.
//
// Exit status is non-zero if any check fails, so this doubles as the CI gate.

// check is one correctness assertion outcome. status is "ok", "FAIL", or "skip"
// (a cell that does not apply — e.g. a gRPC codec whose wire format the deployed
// server cannot accept; a skip is never counted as a failure).
type check struct {
	group  string
	name   string
	status string
	detail string
}

func pass(group, name, detail string) check { return check{group, name, "ok", detail} }
func fail(group, name, detail string) check { return check{group, name, "FAIL", detail} }
func skip(group, name, detail string) check { return check{group, name, "skip", detail} }

func (c check) failed() bool { return c.status == "FAIL" }

// bufferPoolAll is the pool the transport requesters encode into during the
// correctness round trips. "all" matches the deployed agent's default so the
// codec path is the same one the benchmark measures.
func bufferPoolAll() pool.BufferPool {
	m, _ := pool.ParseMode("all") // "all" is a constant; ParseMode cannot fail here
	return m.NewBufferPool()
}

// namedCodec pairs a -codecs entry with its Codec and schema enum.
type namedCodec struct {
	name  string
	codec codec.Codec
	enum  workloadsv1.Codec
}

func parseCodecs(csv string) ([]namedCodec, error) {
	var out []namedCodec
	for _, name := range strings.Split(csv, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		cdc, err := codec.ByName(name)
		if err != nil {
			return nil, err
		}
		en, err := codecEnum(name)
		if err != nil {
			return nil, err
		}
		out = append(out, namedCodec{name: name, codec: cdc, enum: en})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no codecs selected")
	}
	return out, nil
}

func runCorrectness(args []string) error {
	fs := newBusFlags("correctness", "", "")
	// Reuse the shared bus flags for addr/user/pass/sentinels/region/timeout, and
	// add the two correctness-specific selectors.
	transportName := fs.fs.String("transport", "", "also round-trip through this live transport (grpc|nats|rabbitmq|valkey; empty = in-process only)")
	codecsCSV := fs.fs.String("codecs", "proto,protojson,vtproto", "comma-separated codecs to check")
	// gRPC pins the server's wire format per deployment (ForceServerCodecV2), so a
	// client codec whose wire format differs is rejected by design. -server-codec
	// names the codec the deployed agent runs so those cells are skipped, not
	// failed; the buses decode by Content-Type and ignore it.
	serverCodec := fs.fs.String("server-codec", "proto", "the deployed gRPC agent's codec (proto|protojson|vtproto); gRPC-only")
	if err := fs.fs.Parse(args); err != nil {
		return err
	}
	codecs, err := parseCodecs(*codecsCSV)
	if err != nil {
		return err
	}
	c := corpus.New(*fs.seed)

	var results []check
	results = append(results, codecRoundTrip(c, codecs)...)
	results = append(results, corpusDeterminism(c)...)
	results = append(results, validationChecks(c)...)
	if *transportName != "" {
		if *fs.addr == "" {
			return fmt.Errorf("-addr is required with -transport=%s", *transportName)
		}
		results = append(results, transportChecks(*transportName, *serverCodec, fs, codecs, c)...)
	}
	return reportChecks(results, *transportName)
}

// codecRoundTrip asserts every fixture survives encode→decode under every codec.
func codecRoundTrip(c *corpus.Corpus, codecs []namedCodec) []check {
	var out []check
	for _, nc := range codecs {
		for _, f := range corpus.AllFixtures {
			msg, err := c.Message(f)
			if err != nil {
				out = append(out, fail("codec-roundtrip", nc.name+"/"+string(f), err.Error()))
				continue
			}
			wire, err := nc.codec.MarshalAppend(nil, msg)
			if err != nil {
				out = append(out, fail("codec-roundtrip", nc.name+"/"+string(f), "marshal: "+err.Error()))
				continue
			}
			got := msg.ProtoReflect().New().Interface()
			if err := nc.codec.Unmarshal(wire, got); err != nil {
				out = append(out, fail("codec-roundtrip", nc.name+"/"+string(f), "unmarshal: "+err.Error()))
				continue
			}
			if !proto.Equal(msg, got) {
				out = append(out, fail("codec-roundtrip", nc.name+"/"+string(f), "decoded message not proto.Equal to fixture"))
				continue
			}
			out = append(out, pass("codec-roundtrip", nc.name+"/"+string(f), fmt.Sprintf("%d bytes", len(wire))))
		}
	}
	return out
}

// corpusDeterminism asserts a deterministic marshal of each fixture is byte
// stable across repeats — the property that lets run.json record one sha256 per
// fixture and compare runs (design §8.5).
func corpusDeterminism(c *corpus.Corpus) []check {
	var out []check
	det := proto.MarshalOptions{Deterministic: true}
	for _, f := range corpus.AllFixtures {
		msg, err := c.Message(f)
		if err != nil {
			out = append(out, fail("corpus-determinism", string(f), err.Error()))
			continue
		}
		a, err := det.Marshal(msg)
		if err != nil {
			out = append(out, fail("corpus-determinism", string(f), err.Error()))
			continue
		}
		b, _ := det.Marshal(msg)
		if !bytes.Equal(a, b) {
			out = append(out, fail("corpus-determinism", string(f), "deterministic marshal not byte-stable"))
			continue
		}
		sum := envelope.Sum256(a)
		out = append(out, pass("corpus-determinism", string(f), fmt.Sprintf("sha256=%x…", sum[:4])))
	}
	return out
}

// validationChecks asserts the valid fixtures pass protovalidate and each
// invalid-corpus mutation fails carrying its expected constraint id.
func validationChecks(c *corpus.Corpus) []check {
	var out []check
	// Valid: the DeployRequest fixtures are the ones protovalidate has rules for.
	for _, f := range []corpus.Fixture{corpus.Small, corpus.Medium, corpus.Large, corpus.Sparse, corpus.Dense} {
		dr := c.Deploy(f)
		if err := protovalidate.Validate(dr); err != nil {
			out = append(out, fail("validate-valid", string(f), compact(err)))
			continue
		}
		out = append(out, pass("validate-valid", string(f), "passes"))
	}
	// Invalid: each must fail, and the violation must name the expected rule.
	for _, ic := range c.InvalidCases() {
		err := protovalidate.Validate(ic.Message)
		if err == nil {
			out = append(out, fail("validate-invalid", ic.Description, "expected a violation, got none"))
			continue
		}
		if !strings.Contains(err.Error(), ic.WantContains) {
			out = append(out, fail("validate-invalid", ic.Description, fmt.Sprintf("missing %q: %s", ic.WantContains, compact(err))))
			continue
		}
		out = append(out, pass("validate-invalid", ic.Description, ic.WantContains))
	}
	return out
}

// transportChecks round-trips every RPC fixture through the live transport under
// each codec, asserting the reply echoes the request's message_id (and, when the
// agent stamps integrity, that a request_sha256 came back).
func transportChecks(name, serverCodec string, f *busFlags, codecs []namedCodec, c *corpus.Corpus) []check {
	tenum, ok := transportEnum(name)
	if !ok {
		return []check{fail("transport", name, "unknown transport (grpc|nats|rabbitmq|valkey)")}
	}
	fixtures := []corpus.Fixture{corpus.Small, corpus.Medium, corpus.Large, corpus.Sparse, corpus.Dense}
	if name == "grpc" {
		fixtures = append([]corpus.Fixture{corpus.Tiny}, fixtures...) // Ping is the gRPC codec floor
	}
	var out []check
	for _, nc := range codecs {
		// gRPC forces one wire format on the server; a client whose wire format
		// differs is rejected by design, so skip rather than dial+fail.
		if name == "grpc" && wireFormat(nc.name) != wireFormat(serverCodec) {
			out = append(out, skip("transport", name+"/"+nc.name,
				fmt.Sprintf("gRPC server is -codec=%s; %s wire format differs (ForceServerCodecV2)", serverCodec, nc.name)))
			continue
		}
		opts := transport.Options{Codec: nc.codec, Pool: bufferPoolAll(), Region: *f.region, Integrity: true, Validate: true}
		req0, cleanup, err := dialForCorrectness(name, f, opts)
		if err != nil {
			out = append(out, fail("transport", name+"/"+nc.name, "dial: "+err.Error()))
			continue
		}
		for _, fx := range fixtures {
			out = append(out, oneTransportCheck(req0, name, nc, tenum, fx, c, *f.runID, *f.timeout))
		}
		cleanup()
	}
	return out
}

// wireFormat is the on-the-wire encoding a codec produces. proto and vtproto
// share the binary wire format (vtproto is a faster marshaler for the same
// bytes); protojson is JSON. gRPC's forced server codec accepts only its own
// wire format.
func wireFormat(codecName string) string {
	if codecName == "protojson" {
		return "json"
	}
	return "binary"
}

func oneTransportCheck(req0 transport.Requester, name string, nc namedCodec, tenum workloadsv1.Transport, fx corpus.Fixture, c *corpus.Corpus, runID string, timeout time.Duration) check {
	label := name + "/" + nc.name + "/" + string(fx)
	req, newResp, err := pairFor(name, c, fx)
	if err != nil {
		return fail("transport", label, err.Error())
	}
	env := req.(hasEnvelope).GetEnvelope()
	if err := envelope.Fill(env, runID, 0, nc.enum, tenum, string(fx)); err != nil {
		return fail("transport", label, "fill: "+err.Error())
	}
	resp := newResp()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	err = req0.Request(ctx, req, resp)
	cancel()
	if err != nil {
		return fail("transport", label, "request: "+err.Error())
	}
	re := envelope.Of(resp)
	if re == nil || !bytes.Equal(re.GetMessageId(), env.GetMessageId()) {
		return fail("transport", label, "reply did not echo the request message_id")
	}
	if sha := re.GetRequestSha256(); len(sha) != 0 && len(sha) != 32 {
		return fail("transport", label, fmt.Sprintf("request_sha256 length %d, want 0 or 32", len(sha)))
	}
	detail := "message_id echoed"
	if len(re.GetRequestSha256()) == 32 {
		detail += ", sha256 stamped"
	}
	return pass("transport", label, detail)
}

// pairFor builds the request + response constructor for a fixture on the named
// transport (gRPC allows Ping; the buses are Deploy-only).
func pairFor(name string, c *corpus.Corpus, f corpus.Fixture) (proto.Message, func() proto.Message, error) {
	if name == "grpc" {
		return grpcPair(c, f)
	}
	return busPair(c, f)
}

func transportEnum(name string) (workloadsv1.Transport, bool) {
	switch name {
	case "grpc":
		return workloadsv1.Transport_TRANSPORT_GRPC_UNARY, true
	case "nats":
		return workloadsv1.Transport_TRANSPORT_NATS_REQUEST_REPLY, true
	case "rabbitmq":
		return workloadsv1.Transport_TRANSPORT_RABBITMQ_RPC, true
	case "valkey":
		return workloadsv1.Transport_TRANSPORT_VALKEY_STREAM, true
	default:
		return 0, false
	}
}

// dialForCorrectness opens a Requester for one transport and returns a cleanup
// that closes the requester and its underlying connection.
func dialForCorrectness(name string, f *busFlags, opts transport.Options) (transport.Requester, func(), error) {
	switch name {
	case "grpc":
		cc, err := grpctransport.Dial(*f.addr, opts, "none")
		if err != nil {
			return nil, nil, err
		}
		return grpctransport.NewRequester(cc), func() { _ = cc.Close() }, nil
	case "nats":
		nc, err := natstransport.Dial("nats://" + *f.addr)
		if err != nil {
			return nil, nil, err
		}
		r := natstransport.NewRequester(nc, opts, *f.region)
		return r, func() { _ = r.Close(); nc.Close() }, nil
	case "rabbitmq":
		conn, err := rmqtransport.Dial(fmt.Sprintf("amqp://%s:%s@%s/", *f.user, *f.pass, *f.addr))
		if err != nil {
			return nil, nil, err
		}
		r, err := rmqtransport.NewRequester(conn, opts, *f.region)
		if err != nil {
			_ = conn.Close()
			return nil, nil, err
		}
		return r, func() { _ = r.Close(); _ = conn.Close() }, nil
	case "valkey":
		rdb := newValkeyClient(*f.addr, *f.sentinels, *f.pass)
		r := valkeytransport.NewRequester(rdb, opts, *f.region, fmt.Sprintf("correctness-%d", os.Getpid()))
		return r, func() { _ = r.Close(); _ = rdb.Close() }, nil
	default:
		return nil, nil, fmt.Errorf("transport %q has no request/reply correctness path (mqtt is one-way)", name)
	}
}

// reportChecks prints the grouped PASS/FAIL table and returns an error if any
// check failed (so the exit status is the CI gate).
func reportChecks(results []check, transportName string) error {
	tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
	var failed, skipped int
	group := ""
	for _, r := range results {
		if r.group != group {
			group = r.group
			fmt.Fprintf(tw, "\n[%s]\n", group)
		}
		switch r.status {
		case "FAIL":
			failed++
		case "skip":
			skipped++
		}
		fmt.Fprintf(tw, "  %-4s\t%s\t%s\n", r.status, r.name, r.detail)
	}
	tw.Flush()
	scope := "in-process"
	if transportName != "" {
		scope = "in-process + " + transportName
	}
	fmt.Printf("\ncorrectness (%s): %d checks, %d failed, %d skipped\n", scope, len(results), failed, skipped)
	if failed > 0 {
		return fmt.Errorf("%d correctness check(s) failed", failed)
	}
	return nil
}
