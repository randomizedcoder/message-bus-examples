package runrecord

import (
	"math"
	"testing"
)

func TestMedian(t *testing.T) {
	tests := []struct {
		description string
		in          []float64
		expected    float64
	}{
		{"empty is zero", nil, 0},
		{"single value", []float64{7}, 7},
		{"odd count picks the middle", []float64{3, 1, 2}, 2},
		{"even count averages the two middles", []float64{1, 2, 3, 4}, 2.5},
		{"unsorted input is handled", []float64{9, 1, 5, 3, 7}, 5},
		{"negatives (one-way can be negative)", []float64{-4, -2, 0}, -2},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := median(tt.in); got != tt.expected {
				t.Errorf("median(%v) = %v, want %v", tt.in, got, tt.expected)
			}
		})
	}
}

func TestMAD(t *testing.T) {
	tests := []struct {
		description string
		in          []float64
		expected    float64
	}{
		{"empty is zero", nil, 0},
		{"identical values have zero deviation", []float64{5, 5, 5}, 0},
		{"symmetric spread about the median", []float64{1, 2, 3, 4, 5}, 1}, // devs 2,1,0,1,2 -> median 1
		{"single outlier", []float64{10, 10, 10, 40}, 0},                   // med 10; devs 0,0,0,30 -> median 0
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			if got := mad(tt.in, median(tt.in)); got != tt.expected {
				t.Errorf("mad(%v) = %v, want %v", tt.in, got, tt.expected)
			}
		})
	}
}

// cellAt builds a minimal cell for id with the given rtt_p99 and throughput so
// the stability logic can be exercised without a full Summary.
func cellAt(id string, repeat int, p99, tput float64) Cell {
	return Cell{ID: id, Repeat: repeat, Summary: Summary{
		Tier: "rpc", Transport: "grpc_unary", Codec: "proto", Fixture: "medium",
		Pool: "all", GC: "default", Mode: "latency",
		RTTP99US: p99, ThroughputMsgS: tput,
	}}
}

func TestAggregate(t *testing.T) {
	tests := []struct {
		description  string
		cells        []Cell
		wantGroups   int
		wantP99Med   float64
		wantUnstable bool
		wantStableBy string
	}{
		{
			description: "single group, three repeats, stable p99",
			cells: []Cell{
				cellAt("g", 0, 100, 5000), cellAt("g", 1, 104, 5000), cellAt("g", 2, 102, 5000),
			},
			wantGroups: 1, wantP99Med: 102, wantUnstable: false, wantStableBy: "rtt_p99_us",
		},
		{
			description: "wide p99 spread flags unstable (MAD > 20% of median)",
			cells: []Cell{
				cellAt("g", 0, 100, 5000), cellAt("g", 1, 200, 5000), cellAt("g", 2, 300, 5000),
			},
			// med 200, devs 100/0/100 -> MAD 100 = 50% > 20%
			wantGroups: 1, wantP99Med: 200, wantUnstable: true, wantStableBy: "rtt_p99_us",
		},
		{
			description:  "no rtt (open-loop) falls back to throughput for stability",
			cells:        []Cell{cellAt("g", 0, 0, 5000), cellAt("g", 1, 0, 5100)},
			wantGroups:   1,
			wantP99Med:   0,
			wantUnstable: false,
			wantStableBy: "throughput_msg_s",
		},
		{
			description: "two distinct ids yield two groups in first-seen order",
			cells: []Cell{
				cellAt("b", 0, 50, 0), cellAt("a", 0, 60, 0), cellAt("b", 1, 52, 0),
			},
			wantGroups: 2, wantP99Med: 51, wantUnstable: false, wantStableBy: "rtt_p99_us",
		},
	}
	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			got := Aggregate(tt.cells)
			if len(got) != tt.wantGroups {
				t.Fatalf("groups = %d, want %d", len(got), tt.wantGroups)
			}
			first := got[0]
			if math.Abs(first.Med["rtt_p99_us"]-tt.wantP99Med) > 1e-9 {
				t.Errorf("group[0] p99 median = %v, want %v", first.Med["rtt_p99_us"], tt.wantP99Med)
			}
			if first.Unstable != tt.wantUnstable {
				t.Errorf("group[0] unstable = %v, want %v (MAD %v, med %v)",
					first.Unstable, tt.wantUnstable, first.MAD[first.StableBy], first.Med[first.StableBy])
			}
			if first.StableBy != tt.wantStableBy {
				t.Errorf("group[0] stableBy = %q, want %q", first.StableBy, tt.wantStableBy)
			}
			if first.Repeats != len(groupsFor(tt.cells, first.ID)) {
				t.Errorf("group[0] repeats = %d, want %d", first.Repeats, len(groupsFor(tt.cells, first.ID)))
			}
		})
	}
}

// TestAggregatePreservesFirstSeenOrder pins the group ordering to first
// appearance so results.md is deterministic regardless of map iteration.
func TestAggregatePreservesFirstSeenOrder(t *testing.T) {
	cells := []Cell{cellAt("z", 0, 1, 0), cellAt("m", 0, 1, 0), cellAt("a", 0, 1, 0), cellAt("z", 1, 1, 0)}
	got := Aggregate(cells)
	want := []string{"z", "m", "a"}
	for i, a := range got {
		if a.ID != want[i] {
			t.Errorf("group[%d] = %q, want %q", i, a.ID, want[i])
		}
	}
}

func groupsFor(cells []Cell, id string) []Cell {
	var out []Cell
	for _, c := range cells {
		if c.ID == id {
			out = append(out, c)
		}
	}
	return out
}
