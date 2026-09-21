// Command rpc-client is the caller side of the RPC lab (§17): it builds a routed
// rpc.v1 envelope for a demo method, sends it through a GatewayService endpoint
// (rpc-gateway, or rpc-service directly — both speak Call), and prints the
// decoded Response plus the §20 timeline. It is the reference for the uniform
// rpc.Call(ctx, request) API from the client's point of view.
//
// Only the "customer.Lookup" method is wired here (its payload is
// benchmark.v1.CustomerLookupRequest); it is enough to exercise unary Call and,
// with -idempotency-key + -count>1, the idempotency cache (§29): the first call
// executes, retries return the cached Response tagged idempotent-replay.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	benchmarkv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/benchmark/v1"
	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/grpcx"
)

func main() {
	addr := flag.String("addr", "localhost:9430", "GatewayService endpoint (gateway or service)")
	service := flag.String("service", "customer", "service to route to")
	method := flag.String("method", "Lookup", "method to invoke")
	customerID := flag.String("customer-id", "11111111-1111-1111-1111-111111111111", "customer id (uuid) for the Lookup payload")
	region := flag.String("region", "us-west-2", "region for the Lookup payload")
	timeout := flag.Duration("timeout", 5*time.Second, "per-call timeout (envelope + ctx deadline)")
	idemKey := flag.String("idempotency-key", "", "idempotency key; with -count>1 proves retry dedup (§29)")
	count := flag.Int("count", 1, "number of identical calls to issue")
	asJSON := flag.Bool("json", false, "emit one JSON object per call instead of the text summary")
	flag.Parse()

	if *service != "customer" || *method != "Lookup" {
		log.Fatalf("rpc-client: only customer.Lookup is wired in this phase, got %s.%s", *service, *method)
	}
	if *count < 1 {
		log.Fatalf("rpc-client: -count must be >= 1, got %d", *count)
	}

	client, err := grpcx.Dial(*addr)
	if err != nil {
		log.Fatalf("rpc-client: dial %s: %v", *addr, err)
	}
	defer client.Close()

	payload := &benchmarkv1.CustomerLookupRequest{
		CustomerId: *customerID,
		Region:     *region,
	}

	exit := 0
	for i := 1; i <= *count; i++ {
		if err := call(client, *service, *method, payload, *idemKey, *timeout, *asJSON, i); err != nil {
			exit = 1
		}
	}
	os.Exit(exit)
}

// call issues one Call and reports it. It returns an error when the call could
// not be made or came back non-OK, so a non-OK response drives a non-zero exit.
func call(client rpc.Client, service, method string, payload *benchmarkv1.CustomerLookupRequest, idemKey string, timeout time.Duration, asJSON bool, seq int) error {
	req, err := rpc.NewRequest(service, method, payload, timeout)
	if err != nil {
		log.Printf("rpc-client: build request: %v", err)
		return err
	}
	req.IdempotencyKey = idemKey

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	start := time.Now()
	resp, err := client.Call(ctx, req)
	elapsed := time.Since(start)
	if err != nil {
		log.Printf("rpc-client: call #%d: transport error: %v", seq, err)
		return err
	}

	report := newReport(seq, req, resp, elapsed)
	if asJSON {
		b, _ := json.Marshal(report)
		fmt.Println(string(b))
	} else {
		report.print()
	}
	if resp.GetStatus() != rpcv1.Status_STATUS_OK {
		return fmt.Errorf("status %s: %s", resp.GetStatus(), resp.GetErrorMessage())
	}
	return nil
}

// report is the flattened view of one Response for -json output and printing.
type report struct {
	Seq          int           `json:"seq"`
	RequestID    string        `json:"request_id"`
	Status       string        `json:"status"`
	ErrorMessage string        `json:"error_message,omitempty"`
	Replay       bool          `json:"idempotent_replay"`
	RoundTripMS  float64       `json:"round_trip_ms"`
	Customer     *customerView `json:"customer,omitempty"`
}

type customerView struct {
	CustomerID string   `json:"customer_id"`
	Name       string   `json:"name"`
	Email      string   `json:"email"`
	AccountIDs []string `json:"account_ids,omitempty"`
}

func newReport(seq int, req *rpcv1.Request, resp *rpcv1.Response, elapsed time.Duration) *report {
	r := &report{
		Seq:          seq,
		RequestID:    req.GetRequestId(),
		Status:       resp.GetStatus().String(),
		ErrorMessage: resp.GetErrorMessage(),
		Replay:       resp.GetMetadata()["idempotent-replay"] == "true",
		RoundTripMS:  float64(elapsed.Microseconds()) / 1000.0,
	}
	if resp.GetStatus() == rpcv1.Status_STATUS_OK && resp.GetPayload() != nil {
		out := &benchmarkv1.CustomerLookupResponse{}
		if err := rpc.UnpackInto(resp.GetPayload(), out); err == nil {
			r.Customer = &customerView{
				CustomerID: out.GetCustomerId(),
				Name:       out.GetName(),
				Email:      out.GetEmail(),
				AccountIDs: out.GetAccountIds(),
			}
		} else {
			// Fall back to a raw protojson dump so an unexpected payload type is
			// still visible rather than silently dropped.
			r.ErrorMessage = fmt.Sprintf("decode payload: %v (%s)", err, protojson.Format(resp))
		}
	}
	return r
}

func (r *report) print() {
	replay := ""
	if r.Replay {
		replay = " [idempotent-replay]"
	}
	fmt.Printf("#%d %s%s  rtt=%.3fms  request_id=%s\n", r.Seq, r.Status, replay, r.RoundTripMS, r.RequestID)
	if r.Customer != nil {
		fmt.Printf("    customer_id=%s name=%q email=%s accounts=%v\n",
			r.Customer.CustomerID, r.Customer.Name, r.Customer.Email, r.Customer.AccountIDs)
	}
	if r.ErrorMessage != "" {
		fmt.Printf("    error: %s\n", r.ErrorMessage)
	}
}
