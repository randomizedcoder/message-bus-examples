package runrecord

import (
	"math"
	"sort"
)

// Column is one numeric summary field: its results.tsv/results.md header, the
// display precision, and an accessor. Defining the set once keeps the flat TSV
// dump, the median/MAD aggregation, and the Markdown tables in sync.
type Column struct {
	Name string
	Dec  int // decimal places when displayed
	Get  func(Summary) float64
}

// NumericColumns is the ordered numeric column set (results.tsv order). Identity
// fields (tier/transport/…) and booleans are rendered separately; everything
// here is a float that aggregates by median across repeats.
func NumericColumns() []Column {
	return []Column{
		{"msgs", 0, func(s Summary) float64 { return float64(s.Msgs) }},
		{"errors", 0, func(s Summary) float64 { return float64(s.TotalErrors()) }},
		{"throughput_msg_s", 0, func(s Summary) float64 { return s.ThroughputMsgS }},
		{"throughput_mib_s", 1, func(s Summary) float64 { return s.ThroughputMiBS }},
		{"rtt_p50_us", 0, func(s Summary) float64 { return s.RTTP50US }},
		{"rtt_p90_us", 0, func(s Summary) float64 { return s.RTTP90US }},
		{"rtt_p99_us", 0, func(s Summary) float64 { return s.RTTP99US }},
		{"rtt_p999_us", 0, func(s Summary) float64 { return s.RTTP999US }},
		{"rtt_max_us", 0, func(s Summary) float64 { return s.RTTMaxUS }},
		{"rtt_co_corrected_p99_us", 0, func(s Summary) float64 { return s.RTTCoCorrectedP99US }},
		{"one_way_fwd_p50_us", 0, func(s Summary) float64 { return s.OneWayFwdP50US }},
		{"one_way_fwd_p99_us", 0, func(s Summary) float64 { return s.OneWayFwdP99US }},
		{"one_way_rev_p50_us", 0, func(s Summary) float64 { return s.OneWayRevP50US }},
		{"one_way_rev_p99_us", 0, func(s Summary) float64 { return s.OneWayRevP99US }},
		{"clock_uncertainty_us", 1, func(s Summary) float64 { return s.ClockUncertaintyUS }},
		{"server_duration_p50_us", 0, func(s Summary) float64 { return s.ServerDurationP50US }},
		{"server_duration_p99_us", 0, func(s Summary) float64 { return s.ServerDurationP99US }},
		{"wire_bytes_req", 0, func(s Summary) float64 { return float64(s.WireBytesReq) }},
		{"wire_bytes_resp", 0, func(s Summary) float64 { return float64(s.WireBytesResp) }},
		{"driver_alloc_bytes_per_msg", 0, func(s Summary) float64 { return s.DriverAllocBytesPerMsg }},
		{"driver_allocs_per_msg", 2, func(s Summary) float64 { return s.DriverAllocsPerMsg }},
		{"driver_gc_cycles_per_s", 2, func(s Summary) float64 { return s.DriverGCCyclesPerS }},
		{"driver_gc_pause_p99_us", 1, func(s Summary) float64 { return s.DriverGCPauseP99US }},
		{"driver_gc_cpu_fraction", 4, func(s Summary) float64 { return s.DriverGCCPUFraction }},
		{"agent_alloc_bytes_per_msg", 0, func(s Summary) float64 { return s.AgentAllocBytesPerMsg }},
		{"agent_allocs_per_msg", 2, func(s Summary) float64 { return s.AgentAllocsPerMsg }},
		{"agent_gc_cycles_per_s", 2, func(s Summary) float64 { return s.AgentGCCyclesPerS }},
		{"agent_gc_pause_p99_us", 1, func(s Summary) float64 { return s.AgentGCPauseP99US }},
		{"agent_gc_cpu_fraction", 4, func(s Summary) float64 { return s.AgentGCCPUFraction }},
		{"driver_cpu_us_per_msg", 2, func(s Summary) float64 { return s.DriverCPUUsPerMsg }},
		{"agent_cpu_us_per_msg", 2, func(s Summary) float64 { return s.AgentCPUUsPerMsg }},
		{"broker_cpu_us_per_msg", 2, func(s Summary) float64 { return s.BrokerCPUUsPerMsg }},
		{"broker_node_cpu_busy_pct", 1, func(s Summary) float64 { return s.BrokerNodeCPUBusyPct }},
		{"pool_hit_ratio", 3, func(s Summary) float64 { return s.PoolHitRatio }},
		{"missing", 0, func(s Summary) float64 { return float64(s.Missing) }},
		{"duplicate", 0, func(s Summary) float64 { return float64(s.Duplicate) }},
		{"reordered", 0, func(s Summary) float64 { return float64(s.Reordered) }},
		{"redelivered", 0, func(s Summary) float64 { return float64(s.Redelivered) }},
		{"late_sends", 0, func(s Summary) float64 { return float64(s.LateSends) }},
	}
}

// UnstableThreshold is the MAD/median ratio above which a cell is flagged
// "unstable" in results.md (design §8.3: MAD > 20% of the median).
const UnstableThreshold = 0.20

// Aggregated is one cell reduced across its repeats: the identity, the number of
// repeats folded in, the per-column median and MAD, and the stability verdict.
type Aggregated struct {
	ID       string
	Repeats  int
	Ident    Summary            // identity fields (+ inflight/rate) from the first repeat
	Med      map[string]float64 // column name -> median across repeats
	MAD      map[string]float64 // column name -> median absolute deviation
	Unstable bool
	StableBy string // the column the stability verdict was computed on
	HGRM     []string
}

// Aggregate groups a run's cells by cell id (repeats collapse together, first-
// seen order preserved) and reduces each group to medians and MADs over
// NumericColumns. A group is flagged unstable when the MAD of its headline
// latency (rtt_p99_us, else throughput_msg_s for open-loop cells) exceeds
// UnstableThreshold × its median.
func Aggregate(cells []Cell) []Aggregated {
	order := []string{}
	groups := map[string][]Cell{}
	for _, c := range cells {
		if _, ok := groups[c.ID]; !ok {
			order = append(order, c.ID)
		}
		groups[c.ID] = append(groups[c.ID], c)
	}
	cols := NumericColumns()
	out := make([]Aggregated, 0, len(order))
	for _, id := range order {
		grp := groups[id]
		a := Aggregated{
			ID:      id,
			Repeats: len(grp),
			Ident:   grp[0].Summary,
			Med:     map[string]float64{},
			MAD:     map[string]float64{},
		}
		for _, c := range grp {
			if c.HGRM != "" {
				a.HGRM = append(a.HGRM, c.HGRM)
			}
		}
		for _, col := range cols {
			vals := make([]float64, len(grp))
			for i, c := range grp {
				vals[i] = col.Get(c.Summary)
			}
			med := median(vals)
			a.Med[col.Name] = med
			a.MAD[col.Name] = mad(vals, med)
		}
		a.StableBy = "rtt_p99_us"
		if a.Med["rtt_p99_us"] <= 0 {
			a.StableBy = "throughput_msg_s"
		}
		if m := a.Med[a.StableBy]; m > 0 {
			a.Unstable = a.MAD[a.StableBy]/m > UnstableThreshold
		}
		out = append(out, a)
	}
	return out
}

// median returns the median of vs (average of the two middle values for an even
// count). An empty slice yields 0.
func median(vs []float64) float64 {
	if len(vs) == 0 {
		return 0
	}
	c := append([]float64(nil), vs...)
	sort.Float64s(c)
	n := len(c)
	if n%2 == 1 {
		return c[n/2]
	}
	return (c[n/2-1] + c[n/2]) / 2
}

// mad returns the median absolute deviation of vs about med.
func mad(vs []float64, med float64) float64 {
	if len(vs) == 0 {
		return 0
	}
	dev := make([]float64, len(vs))
	for i, v := range vs {
		dev[i] = math.Abs(v - med)
	}
	return median(dev)
}
