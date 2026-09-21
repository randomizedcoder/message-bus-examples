// Package runrecord is the on-disk shape of a proto-bench run and the flat
// column set the report renderer emits.
//
// A run is one invocation of the k8s-proto-bench host harness: a matrix of
// cells (transport × codec × fixture × pool × gc × mode), each executed
// `repeats` times. The harness assembles a Run, benchcli run modes fill the
// per-cell Summary (P4b-2), and `benchcli report` reduces repeats to a
// median ± MAD table (aggregate.go) and renders results.tsv / results.md
// (design §8.4, §8.5). The schema is deliberately flat and JSON-tagged so the
// shell harness can also assemble it with jq.
package runrecord

import (
	"encoding/json"
	"fmt"
	"os"
)

// Run is the run.json record (design §8.5).
type Run struct {
	RunID        string                       `json:"run_id"`
	StartedAt    string                       `json:"started_at"`
	FinishedAt   string                       `json:"finished_at"`
	Git          Git                          `json:"git"`
	Versions     map[string]string            `json:"versions,omitempty"`
	Host         Host                         `json:"host"`
	CodecOptions map[string]map[string]any    `json:"codec_options,omitempty"`
	GRPC         map[string]any               `json:"grpc,omitempty"`
	GCProfiles   map[string]map[string]string `json:"gc_profiles,omitempty"`
	Clock        map[string]Clock             `json:"clock,omitempty"`
	Corpus       Corpus                       `json:"corpus"`
	Matrix       Matrix                       `json:"matrix"`
	Cells        []Cell                       `json:"cells"`
}

// Git records the tree the run was built from.
type Git struct {
	Rev   string `json:"rev"`
	Dirty bool   `json:"dirty"`
}

// Host records the driver machine and the CPU sets used for pinning.
type Host struct {
	CPU          string            `json:"cpu,omitempty"`
	DriverCPUSet string            `json:"driver_cpuset,omitempty"`
	VMVCPUs      map[string]string `json:"vm_vcpus,omitempty"`
	Kernel       string            `json:"kernel,omitempty"`
}

// Clock is the per-region clock-probe result folded into the run
// (`benchcli clockprobe -json`, design §8.1).
type Clock struct {
	OffsetNs      int64  `json:"offset_ns"`
	UncertaintyNs int64  `json:"uncertainty_ns"`
	Source        string `json:"source"`
	Synced        bool   `json:"synced"`
}

// Corpus records the fixture seed and per-fixture wire sizes / sha256.
type Corpus struct {
	Seed     uint64                  `json:"seed"`
	Fixtures map[string]FixtureSizes `json:"fixtures,omitempty"`
}

// FixtureSizes is the wire footprint of one fixture under each codec.
type FixtureSizes struct {
	ProtoBytes     int    `json:"proto_bytes"`
	ProtoJSONBytes int    `json:"protojson_bytes"`
	VTProtoBytes   int    `json:"vtproto_bytes,omitempty"`
	SHA256         string `json:"sha256,omitempty"`
}

// Matrix records how the cell order was produced so a run is reproducible.
type Matrix struct {
	OrderSeed int64    `json:"order_seed"`
	Repeats   int      `json:"repeats"`
	Cells     []string `json:"cells,omitempty"` // cell ids, in shuffled order
}

// Cell is one executed (cell-id, repeat) pair.
type Cell struct {
	ID       string              `json:"id"`
	Repeat   int                 `json:"repeat"`
	Summary  Summary             `json:"summary"`
	Memstats map[string]Memstats `json:"memstats,omitempty"` // "driver" / "agent"
	HGRM     string              `json:"hgrm,omitempty"`
}

// Memstats is the exact runtime.MemStats delta over a cell (design §8.3).
type Memstats struct {
	Mallocs      uint64 `json:"Mallocs"`
	TotalAlloc   uint64 `json:"TotalAlloc"`
	NumGC        uint32 `json:"NumGC"`
	PauseTotalNs uint64 `json:"PauseTotalNs"`
}

// Summary is the per-cell reported column set (design §8.4). Identity fields
// name the cell; the rest are measurements. Zero values render as "-" / 0 and
// are treated as "not measured" by the renderer, so a partially-filled cell is
// still valid.
type Summary struct {
	// Identity.
	Tier      string  `json:"tier"`
	Transport string  `json:"transport"`
	Codec     string  `json:"codec"`
	Fixture   string  `json:"fixture"`
	Pool      string  `json:"pool"`
	GC        string  `json:"gc"`
	Mode      string  `json:"mode"`
	Inflight  int     `json:"inflight,omitempty"`
	Rate      float64 `json:"rate,omitempty"`

	// Counters.
	Msgs           int64            `json:"msgs"`
	Errors         map[string]int64 `json:"errors,omitempty"`
	ThroughputMsgS float64          `json:"throughput_msg_s"`
	ThroughputMiBS float64          `json:"throughput_mib_s"`

	// Latency (driver monotonic RTT), microseconds.
	RTTP50US            float64 `json:"rtt_p50_us"`
	RTTP90US            float64 `json:"rtt_p90_us"`
	RTTP95US            float64 `json:"rtt_p95_us,omitempty"`
	RTTP99US            float64 `json:"rtt_p99_us"`
	RTTP999US           float64 `json:"rtt_p999_us"`
	RTTMaxUS            float64 `json:"rtt_max_us"`
	RTTCoCorrectedP99US float64 `json:"rtt_co_corrected_p99_us,omitempty"`

	// One-way (published only when the clock gate permits, design §8.1).
	OneWayFwdP50US     float64 `json:"one_way_fwd_p50_us,omitempty"`
	OneWayFwdP99US     float64 `json:"one_way_fwd_p99_us,omitempty"`
	OneWayRevP50US     float64 `json:"one_way_rev_p50_us,omitempty"`
	OneWayRevP99US     float64 `json:"one_way_rev_p99_us,omitempty"`
	ClockUncertaintyUS float64 `json:"clock_uncertainty_us,omitempty"`
	OneWayPublished    bool    `json:"one_way_published"`

	// Agent-side service time.
	ServerDurationP50US float64 `json:"server_duration_p50_us,omitempty"`
	ServerDurationP99US float64 `json:"server_duration_p99_us,omitempty"`

	// Wire footprint per message.
	WireBytesReq  int64 `json:"wire_bytes_req,omitempty"`
	WireBytesResp int64 `json:"wire_bytes_resp,omitempty"`

	// Allocation / GC, driver and agent (runtime.MemStats deltas + go_* metrics).
	DriverAllocBytesPerMsg float64 `json:"driver_alloc_bytes_per_msg,omitempty"`
	DriverAllocsPerMsg     float64 `json:"driver_allocs_per_msg,omitempty"`
	DriverGCCyclesPerS     float64 `json:"driver_gc_cycles_per_s,omitempty"`
	DriverGCPauseP99US     float64 `json:"driver_gc_pause_p99_us,omitempty"`
	DriverGCCPUFraction    float64 `json:"driver_gc_cpu_fraction,omitempty"`
	AgentAllocBytesPerMsg  float64 `json:"agent_alloc_bytes_per_msg,omitempty"`
	AgentAllocsPerMsg      float64 `json:"agent_allocs_per_msg,omitempty"`
	AgentGCCyclesPerS      float64 `json:"agent_gc_cycles_per_s,omitempty"`
	AgentGCPauseP99US      float64 `json:"agent_gc_pause_p99_us,omitempty"`
	AgentGCCPUFraction     float64 `json:"agent_gc_cpu_fraction,omitempty"`

	// CPU per message.
	DriverCPUUsPerMsg float64 `json:"driver_cpu_us_per_msg,omitempty"`
	AgentCPUUsPerMsg  float64 `json:"agent_cpu_us_per_msg,omitempty"`
	BrokerCPUUsPerMsg float64 `json:"broker_cpu_us_per_msg,omitempty"`

	// Broker node.
	BrokerNodeCPUBusyPct float64 `json:"broker_node_cpu_busy_pct,omitempty"`
	BrokerThrottled      bool    `json:"broker_throttled,omitempty"`

	// Pooling.
	PoolHitRatio float64 `json:"pool_hit_ratio,omitempty"`

	// Integrity counters (correctness / fault modes, design §8.4).
	Missing     int64 `json:"missing,omitempty"`
	Duplicate   int64 `json:"duplicate,omitempty"`
	Reordered   int64 `json:"reordered,omitempty"`
	Redelivered int64 `json:"redelivered,omitempty"`
	LateSends   int64 `json:"late_sends,omitempty"`

	// Links.
	Grafana string `json:"grafana,omitempty"`
}

// TotalErrors sums the per-kind error counts.
func (s Summary) TotalErrors() int64 {
	var n int64
	for _, v := range s.Errors {
		n += v
	}
	return n
}

// Load reads and decodes a run.json file.
func Load(path string) (*Run, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r Run
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &r, nil
}

// Save writes r to path as indented JSON.
func Save(path string, r *Run) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}
