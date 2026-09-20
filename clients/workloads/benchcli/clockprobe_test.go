package main

import (
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ts builds a Timestamp at the given nanoseconds-since-epoch.
func ts(ns int64) *timestamppb.Timestamp { return timestamppb.New(time.Unix(0, ns)) }

func TestProbeSample(t *testing.T) {
	tests := []struct {
		description string
		t1, t2, t3  *timestamppb.Timestamp
		t4          time.Time
		wantOffset  int64
		wantRTT     int64
		wantOK      bool
	}{
		{
			description: "perfect sync, symmetric path — offset 0",
			t1:          ts(0), t2: ts(100), t3: ts(110), t4: time.Unix(0, 210),
			wantOffset: 0, wantRTT: 200, wantOK: true,
		},
		{
			description: "server clock ahead by 50 — positive offset",
			t1:          ts(0), t2: ts(150), t3: ts(160), t4: time.Unix(0, 210),
			wantOffset: 50, wantRTT: 200, wantOK: true,
		},
		{
			description: "server clock behind by 50 — negative offset, never clamped",
			t1:          ts(0), t2: ts(50), t3: ts(60), t4: time.Unix(0, 210),
			wantOffset: -50, wantRTT: 200, wantOK: true,
		},
		{
			description: "zero server processing time — rtt is the whole round trip",
			t1:          ts(1000), t2: ts(1100), t3: ts(1100), t4: time.Unix(0, 1200),
			wantOffset: 0, wantRTT: 200, wantOK: true,
		},
		{
			description: "missing t2 (server receive) — not usable",
			t1:          ts(0), t2: nil, t3: ts(110), t4: time.Unix(0, 210),
			wantOK: false,
		},
		{
			description: "missing t3 (server send) — not usable",
			t1:          ts(0), t2: ts(100), t3: nil, t4: time.Unix(0, 210),
			wantOK: false,
		},
		{
			description: "missing t1 (client send) — not usable",
			t1:          nil, t2: ts(100), t3: ts(110), t4: time.Unix(0, 210),
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			offset, rtt, ok := probeSample(tt.t1, tt.t2, tt.t3, tt.t4)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if offset != tt.wantOffset {
				t.Errorf("offset = %d, want %d", offset, tt.wantOffset)
			}
			if rtt != tt.wantRTT {
				t.Errorf("rtt = %d, want %d", rtt, tt.wantRTT)
			}
		})
	}
}
