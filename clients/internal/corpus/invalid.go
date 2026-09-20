package corpus

import (
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	workloadsv1 "github.com/randomizedcoder/message-bus-examples/clients/gen/go/workloads/v1"
)

// InvalidCase is a message that violates exactly one protovalidate rule, with
// the constraint id the failure should carry (design demo step 3 / §9.4).
type InvalidCase struct {
	Description  string
	Message      proto.Message
	WantContains string // substring expected in the violation (constraint id)
}

// validBase returns a valid DeployRequest{create} to mutate per case.
func (c *Corpus) validBase() *workloadsv1.DeployRequest {
	r := c.randFor(hash("invalid-base"))
	return &workloadsv1.DeployRequest{
		Envelope:       c.envelope(Medium, 2),
		Ref:            c.workloadRef(r),
		IdempotencyKey: randUUID(r),
		RequestedAt:    timestamppb.New(baseTime),
		Op:             &workloadsv1.DeployRequest_Create{Create: c.spec(Medium, r)},
	}
}

// InvalidCases returns one message per validation rule the demo showcases; each
// is valid except for the single named violation.
func (c *Corpus) InvalidCases() []InvalidCase {
	// 1. limits < requests
	limitsLtRequests := proto.Clone(c.validBase()).(*workloadsv1.DeployRequest)
	limitsLtRequests.GetCreate().Containers[0].Resources = &workloadsv1.Resources{
		Requests: &workloadsv1.ResourceQuantity{CpuMillis: 200, MemoryBytes: 256 << 20},
		Limits:   &workloadsv1.ResourceQuantity{CpuMillis: 100, MemoryBytes: 128 << 20},
	}

	// 2. duplicate container names
	dupNames := proto.Clone(c.validBase()).(*workloadsv1.DeployRequest)
	dup := proto.Clone(dupNames.GetCreate().Containers[0]).(*workloadsv1.ContainerSpec)
	dupNames.GetCreate().Containers = append(dupNames.GetCreate().Containers, dup)

	// 3. CRON without schedule
	cronNoSchedule := proto.Clone(c.validBase()).(*workloadsv1.DeployRequest)
	cronNoSchedule.GetCreate().Kind = workloadsv1.WorkloadKind_WORKLOAD_KIND_CRON
	cronNoSchedule.GetCreate().Schedule = nil

	// 4. update_mask with an unknown path
	r := c.randFor(hash("invalid-update"))
	badMask := &workloadsv1.DeployRequest{
		Envelope:       c.envelope(Medium, 2),
		Ref:            c.workloadRef(r),
		IdempotencyKey: randUUID(r),
		RequestedAt:    timestamppb.New(baseTime),
		Op: &workloadsv1.DeployRequest_Update{Update: &workloadsv1.UpdateWorkload{
			Spec:       c.spec(Medium, r),
			UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"bogus_field"}},
		}},
	}

	// 5. request_sha256 of length 31
	badSha := proto.Clone(c.validBase()).(*workloadsv1.DeployRequest)
	badSha.Envelope.RequestSha256 = make([]byte, 31)

	// 6. max_rtt of 6s (> 5s limit)
	badRtt := proto.Clone(c.validBase()).(*workloadsv1.DeployRequest)
	badRtt.GetCreate().Placement = &workloadsv1.Placement{Policy: &workloadsv1.Placement_LatencyTarget{
		LatencyTarget: &workloadsv1.LatencyTarget{FromRegion: "us-west-2", MaxRtt: durationpb.New(6 * time.Second)},
	}}

	return []InvalidCase{
		{"limits < requests", limitsLtRequests, "resources.limits_ge_requests"},
		{"duplicate container names", dupNames, "workload_spec.container_names_unique"},
		{"CRON without schedule", cronNoSchedule, "workload_spec.schedule_iff_cron"},
		{"update_mask with an unknown path", badMask, "update_workload.mask_paths"},
		{"request_sha256 of length 31", badSha, "envelope.sha256_len"},
		{"max_rtt of 6s", badRtt, "max_rtt"},
	}
}
