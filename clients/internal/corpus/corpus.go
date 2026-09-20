// Package corpus builds the deterministic payload fixtures the benchmark
// encodes (design §3.8). Every fixture is derived from a fixed seed via
// math/rand/v2 PCG and a per-fixture salt, so a given (seed, fixture) always
// yields the same message regardless of call order, and fixtures are built
// once outside every timed loop. All valid fixtures pass protovalidate.
package corpus

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
)

// Fixture names the payload shapes (design §3.8).
type Fixture string

const (
	Tiny   Fixture = "tiny"   // PingRequest, envelope only
	Small  Fixture = "small"  // DeployRequest{delete}
	Medium Fixture = "medium" // DeployRequest{create}, 1 container
	Large  Fixture = "large"  // DeployRequest{create}, 8 containers
	Max    Fixture = "max"    // LogChunk with ~960 KiB data
	Sparse Fixture = "sparse" // medium with every optional/map unset
	Dense  Fixture = "dense"  // medium with every optional/map populated
)

// AllFixtures is the canonical order used by reports and the -fixture flag.
var AllFixtures = []Fixture{Tiny, Small, Medium, Large, Max, Sparse, Dense}

var baseTime = time.Unix(1_700_000_000, 0).UTC()

var regions = []string{"us-west-2", "us-east-1", "eu-west-1", "ap-south-1"}

// Corpus builds fixtures for one seed.
type Corpus struct {
	seed uint64
}

// New returns a corpus keyed on seed.
func New(seed uint64) *Corpus { return &Corpus{seed: seed} }

// randFor returns a deterministic RNG for a fixture salt, so builders are pure
// per (seed, salt) and independent of call order.
func (c *Corpus) randFor(salt uint64) *rand.Rand { return rand.New(rand.NewPCG(c.seed, salt)) }

// Message returns the primary payload for a fixture (what the codec loop
// encodes): tiny→PingRequest, max→LogChunk, the rest→DeployRequest.
func (c *Corpus) Message(f Fixture) (proto.Message, error) {
	switch f {
	case Tiny:
		return c.Ping(), nil
	case Small, Medium, Large, Sparse, Dense:
		return c.Deploy(f), nil
	case Max:
		return c.LogChunk(Max), nil
	default:
		return nil, fmt.Errorf("corpus: unknown fixture %q", f)
	}
}

// Ping is the tiny fixture: an envelope-only round-trip floor.
func (c *Corpus) Ping() *workloadsv1.PingRequest {
	return &workloadsv1.PingRequest{Envelope: c.envelope(Tiny, 1)}
}

// Deploy builds a DeployRequest for the control-plane fixtures.
func (c *Corpus) Deploy(f Fixture) *workloadsv1.DeployRequest {
	r := c.randFor(uint64(len(f)) ^ hash(string(f)))
	req := &workloadsv1.DeployRequest{
		Envelope:       c.envelope(f, 2),
		Ref:            c.workloadRef(r),
		IdempotencyKey: randUUID(r),
		RequestedAt:    timestamppb.New(baseTime.Add(time.Second)),
	}
	if f == Small {
		req.Op = &workloadsv1.DeployRequest_Delete{Delete: &workloadsv1.DeleteWorkload{
			Force:       false,
			GracePeriod: durationpb.New(30 * time.Second),
		}}
		return req
	}
	req.Op = &workloadsv1.DeployRequest_Create{Create: c.spec(f, r)}
	return req
}

// LogChunk builds the max fixture: a single ~960 KiB log chunk (the payload
// where ProtoJSON base64 expansion is visible at scale).
func (c *Corpus) LogChunk(f Fixture) *workloadsv1.LogChunk {
	return c.logChunk(f, hash("logchunk:"+string(f)), 960<<10)
}

// LogChunkBytes builds a LogChunk carrying exactly n bytes of synthetic log data
// (the fan-out payload, design §3.9). The logs fan-out driver sweeps n so even
// ProtoJSON's base64 expansion (×1.33) stays under NATS's 1 MB max_payload — the
// canonical `max` fixture is 960 KiB, which only the binary codecs fit.
func (c *Corpus) LogChunkBytes(n int) *workloadsv1.LogChunk {
	return c.logChunk(Max, hash(fmt.Sprintf("logchunk-bytes:%d", n)), n)
}

// logChunk is the shared LogChunk builder (data first, then the RNG-derived pod
// name, so the byte stream is independent of struct-field order).
func (c *Corpus) logChunk(f Fixture, salt uint64, size int) *workloadsv1.LogChunk {
	r := c.randFor(salt)
	data := randBytes(r, size)
	return &workloadsv1.LogChunk{
		Envelope:  c.envelope(f, 3),
		PodName:   "pod-" + shortHex(r),
		Container: "app",
		Stream:    workloadsv1.LogStream_LOG_STREAM_STDOUT,
		FirstTs:   timestamppb.New(baseTime),
		LastTs:    timestamppb.New(baseTime.Add(time.Second)),
		LineCount: 4096,
		Data:      data,
		Truncated: false,
	}
}

// Telemetry builds a TelemetrySample with n container states (design §3.8:
// 1 / 4 / 16 used for the one-way transports).
func (c *Corpus) Telemetry(nContainers int) *workloadsv1.TelemetrySample {
	r := c.randFor(hash(fmt.Sprintf("telemetry:%d", nContainers)))
	cs := make([]*workloadsv1.ContainerState, nContainers)
	for i := range cs {
		cs[i] = &workloadsv1.ContainerState{
			Name:         fmt.Sprintf("app-%d", i),
			Phase:        workloadsv1.ContainerPhase_CONTAINER_PHASE_RUNNING,
			RestartCount: int32(r.IntN(3)),
			StartedAt:    timestamppb.New(baseTime),
		}
	}
	return &workloadsv1.TelemetrySample{
		Envelope:   c.envelope(Medium, 4),
		Ref:        c.workloadRef(r),
		Cluster:    c.clusterRef(r),
		PodName:    "pod-" + shortHex(r),
		SampledAt:  timestamppb.New(baseTime),
		Containers: cs,
		Resources: &workloadsv1.ResourceSample{
			CpuCores:              1.5,
			MemoryWorkingSetBytes: uint64(r.Uint32()),
			RxBytes:               uint64(r.Uint32()),
			TxBytes:               uint64(r.Uint32()),
			FsUsedBytes:           uint64(r.Uint32()),
		},
		CustomGauges: map[string]float64{"queue_depth": float64(r.IntN(100)), "p99_ms": 12.5},
	}
}

// Usage builds a UsageRecord with n meter readings (design §3.8: 1 / 5).
func (c *Corpus) Usage(nReadings int) *workloadsv1.UsageRecord {
	r := c.randFor(hash(fmt.Sprintf("usage:%d", nReadings)))
	meters := []workloadsv1.Meter{
		workloadsv1.Meter_METER_CPU_CORE_SECONDS,
		workloadsv1.Meter_METER_MEMORY_GIB_SECONDS,
		workloadsv1.Meter_METER_EGRESS_BYTES,
		workloadsv1.Meter_METER_STORAGE_GIB_HOURS,
		workloadsv1.Meter_METER_REQUESTS,
	}
	if nReadings > len(meters) {
		nReadings = len(meters)
	}
	readings := make([]*workloadsv1.MeterReading, nReadings)
	for i := 0; i < nReadings; i++ {
		readings[i] = &workloadsv1.MeterReading{
			Meter: meters[i],
			Value: &workloadsv1.MeterReading_Quantity{Quantity: float64(r.IntN(1000)) + 0.5},
			Unit:  "unit",
		}
	}
	return &workloadsv1.UsageRecord{
		Envelope:    c.envelope(Medium, 5),
		Ref:         c.workloadRef(r),
		Cluster:     c.clusterRef(r),
		RecordId:    randUUID(r),
		WindowStart: timestamppb.New(baseTime),
		WindowEnd:   timestamppb.New(baseTime.Add(30 * time.Minute)),
		Readings:    readings,
	}
}

// Sizes returns the binary-proto wire size of each fixture's primary message.
func (c *Corpus) Sizes() (map[Fixture]int, error) {
	out := make(map[Fixture]int, len(AllFixtures))
	for _, f := range AllFixtures {
		m, err := c.Message(f)
		if err != nil {
			return nil, err
		}
		out[f] = proto.Size(m)
	}
	return out, nil
}

// ---- builders --------------------------------------------------------------

func (c *Corpus) envelope(f Fixture, seq uint64) *workloadsv1.Envelope {
	r := c.randFor(hash("env:" + string(f)))
	var mid [16]byte
	binary.LittleEndian.PutUint64(mid[0:], r.Uint64())
	binary.LittleEndian.PutUint64(mid[8:], r.Uint64())
	return &workloadsv1.Envelope{
		RunId:          "proto-bench",
		MessageId:      mid[:],
		Sequence:       seq,
		ClientSendTime: timestamppb.New(baseTime),
		Codec:          workloadsv1.Codec_CODEC_PROTO,
		Transport:      workloadsv1.Transport_TRANSPORT_GRPC_UNARY,
		Fixture:        string(f),
	}
}

func (c *Corpus) workloadRef(r *rand.Rand) *workloadsv1.WorkloadRef {
	return &workloadsv1.WorkloadRef{
		Tenant:     &workloadsv1.Tenant{CustomerId: randUUID(r), ProjectId: randUUID(r)},
		WorkloadId: randUUID(r),
	}
}

func (c *Corpus) clusterRef(r *rand.Rand) *workloadsv1.ClusterRef {
	region := regions[r.IntN(len(regions))]
	return &workloadsv1.ClusterRef{Region: region, ClusterId: "cluster-" + shortHex(r)}
}

func (c *Corpus) resources() *workloadsv1.Resources {
	return &workloadsv1.Resources{
		Requests: &workloadsv1.ResourceQuantity{CpuMillis: 100, MemoryBytes: 128 << 20},
		Limits:   &workloadsv1.ResourceQuantity{CpuMillis: 200, MemoryBytes: 256 << 20},
	}
}

func (c *Corpus) container(i, nEnv, nPorts int, withOptional bool, r *rand.Rand) *workloadsv1.ContainerSpec {
	env := make([]*workloadsv1.EnvVar, nEnv)
	for j := range env {
		env[j] = &workloadsv1.EnvVar{
			Name:  fmt.Sprintf("VAR_%d_%d", i, j),
			Value: &workloadsv1.EnvVar_Literal{Literal: "v" + shortHex(r)},
		}
	}
	ports := make([]*workloadsv1.Port, nPorts)
	for j := range ports {
		ports[j] = &workloadsv1.Port{
			Name:          fmt.Sprintf("p%d-%d", i, j),
			ContainerPort: uint32(8000 + i*10 + j),
			Protocol:      workloadsv1.Protocol_PROTOCOL_TCP,
		}
	}
	cs := &workloadsv1.ContainerSpec{
		Name:      fmt.Sprintf("app-%d", i),
		Image:     &workloadsv1.ImageSource{Repository: "registry.example/app", Ref: &workloadsv1.ImageSource_Tag{Tag: "v1.2.3"}},
		Env:       env,
		Ports:     ports,
		Resources: c.resources(),
	}
	if withOptional {
		wd := "/srv/app"
		cs.WorkingDir = &wd
	}
	return cs
}

func (c *Corpus) spec(f Fixture, r *rand.Rand) *workloadsv1.WorkloadSpec {
	spec := &workloadsv1.WorkloadSpec{
		Kind:          workloadsv1.WorkloadKind_WORKLOAD_KIND_SERVICE,
		Replicas:      3,
		RestartPolicy: workloadsv1.RestartPolicy_RESTART_POLICY_ALWAYS,
		Placement: &workloadsv1.Placement{Policy: &workloadsv1.Placement_Cluster{
			Cluster: c.clusterRef(r),
		}},
	}
	switch f {
	case Large:
		for i := 0; i < 8; i++ {
			nEnv := 4
			if i == 0 {
				nEnv = 32
			}
			spec.Containers = append(spec.Containers, c.container(i, nEnv, 2, false, r))
		}
		spec.Labels = strMap("label", 16)
		spec.Annotations = strMap("annotation", 16)
	case Sparse:
		// every optional / map unset: one minimal container, no labels.
		spec.Containers = []*workloadsv1.ContainerSpec{c.container(0, 0, 0, false, r)}
	case Dense:
		// every optional / map populated. schedule is only valid on CRON, so
		// dense is a CRON workload to keep schedule_iff_cron satisfied.
		spec.Kind = workloadsv1.WorkloadKind_WORKLOAD_KIND_CRON
		spec.Containers = []*workloadsv1.ContainerSpec{c.container(0, 4, 2, true, r)}
		spec.Labels = strMap("label", 4)
		spec.Annotations = strMap("annotation", 4)
		spec.TtlAfterFinished = durationpb.New(24 * time.Hour)
		sched := "*/5 * * * *"
		spec.Schedule = &sched
	default: // Medium
		spec.Containers = []*workloadsv1.ContainerSpec{c.container(0, 4, 2, false, r)}
		spec.Labels = strMap("label", 4)
	}
	return spec
}

// ---- deterministic helpers -------------------------------------------------

func randUUID(r *rand.Rand) string {
	var b [16]byte
	binary.LittleEndian.PutUint64(b[0:], r.Uint64())
	binary.LittleEndian.PutUint64(b[8:], r.Uint64())
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	u, _ := uuid.FromBytes(b[:])
	return u.String()
}

func randBytes(r *rand.Rand, n int) []byte {
	b := make([]byte, n)
	for i := 0; i+8 <= n; i += 8 {
		binary.LittleEndian.PutUint64(b[i:], r.Uint64())
	}
	for i := n &^ 7; i < n; i++ {
		b[i] = byte(r.Uint64())
	}
	return b
}

func shortHex(r *rand.Rand) string { return fmt.Sprintf("%08x", r.Uint32()) }

func strMap(prefix string, n int) map[string]string {
	m := make(map[string]string, n)
	for i := 0; i < n; i++ {
		m[fmt.Sprintf("%s-%d", prefix, i)] = fmt.Sprintf("value-%d", i)
	}
	return m
}

// hash is a small deterministic string→uint64 (FNV-1a) for per-fixture salts.
func hash(s string) uint64 {
	const off = 1469598103934665603
	const prime = 1099511628211
	h := uint64(off)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	return h
}
