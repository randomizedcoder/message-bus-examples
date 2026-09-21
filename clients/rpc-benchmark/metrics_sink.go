package main

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	rpcv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/rpc/v1"
	"github.com/randomizedcoder/message-bus-examples/clients/internal/rpc/rpcmetrics"
)

// metricsSink records one cell's rpc_* Prometheus metrics (§22). A nil
// *metricsSink is a no-op, so the bench loops hold one whether or not
// -metrics-addr was set. reqBytes is the cell's constant serialized request
// size; the response size is measured per call. In open-loop mode the rtt passed
// here is coordinated-omission-corrected (the same value the HDR sees), so the
// Prometheus duration matches the reported distribution; the .hgrm files remain
// the source of exact percentiles.
type metricsSink struct {
	rec      *rpcmetrics.Recorder
	reqBytes int
}

func (s *metricsSink) record(result string, resp *rpcv1.Response, rtt time.Duration) {
	if s == nil {
		return
	}
	respBytes := 0
	if resp != nil {
		respBytes = proto.Size(resp)
	}
	s.rec.Observe(context.Background(), result, rtt, s.reqBytes, resp != nil, respBytes)
}
