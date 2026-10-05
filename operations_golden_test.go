// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/bcl"
	jc "github.com/cratis/chronicle.go/contracts/jobs"
	oc "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/jobs"
	"github.com/cratis/chronicle.go/observation"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

var operationJobID = uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")

// Hand-derived from .NET Guid byte order, not calculated by the SDK converter.
func operationGuid() *bcl.Guid { return &bcl.Guid{Lo: 0x6677445500112233, Hi: 0xffeeddccbbaa9988} }
func operationJob() *jc.JobSummaryResponse {
	return &jc.JobSummaryResponse{Id: operationGuid(), Details: "Replay orders", Type: "ReplayObserver", Status: jc.JobStatus_JOB_STATUS_Running, Created: &jc.SerializableDateTimeOffset{Value: "2026-01-02T03:04:05.1234567+02:00"}, StatusChanges: []*jc.JobStatusChanged{{Status: jc.JobStatus_JOB_STATUS_Running, Occurred: &jc.SerializableDateTimeOffset{Value: "2026-01-02T03:04:06+02:00"}, ExceptionMessages: []string{"diagnostic"}, ExceptionStackTrace: "job-stack"}}, Progress: &jc.JobProgress{TotalSteps: 4, SuccessfulSteps: 1, FailedSteps: 1, StoppedSteps: 1, Message: "working"}}
}
func operationObserver() *oc.ObserverInformation {
	return &oc.ObserverInformation{Id: "orders", EventSequenceId: "source-sequence", Type: oc.ObserverType_Reactor, Owner: oc.ObserverOwner_Client, EventTypes: []*oc.EventType{{Id: "ordered", Generation: 2}, {Id: "changed", Generation: 1}}, NextEventSequenceNumber: 10, LastHandledEventSequenceNumber: 8, RunningState: oc.ObserverRunningState_Quarantined, IsSubscribed: true, IsReplayable: true, TailEventSequenceNumber: 12, HandledEventCount: 4}
}
func operationFailure() *oc.FailedPartition {
	return &oc.FailedPartition{Id: operationGuid(), ObserverId: "orders", Partition: "customer/1", Attempts: []*oc.FailedPartitionAttempt{{Occurred: &oc.SerializableDateTimeOffset{Value: "2026-01-02T03:04:05.1234567+02:00"}, SequenceNumber: 8, Messages: []string{"failed", "because"}, StackTrace: "failure-stack", Kind: oc.FailureKind_Handling}}, IsResolved: true, IsQuarantined: true}
}

// These are full hand-derived C# request/result fixtures, NOT .NET-emitted captures.
// Authority: Chronicle 2e31b0dfb Observation/{Observers,FailedPartitions},
// Reactors/Reactors, Jobs/{Jobs,Job,JobsConverters}; pinned protobuf supplies the
// additional explicit quarantine controls and explicit sequence selection.
func TestOperationsMatchHandDerivedCSharpGolden(t *testing.T) {
	ctx := testContext(t)
	tests := []struct {
		name              string
		request, response proto.Message
		run               func(*observation.Service, *jobs.Service) error
	}{
		{"GetObservers", &oc.AllObserversRequest{EventStore: "store", Namespace: "tenant"}, &oc.IEnumerable_ObserverInformation{Items: []*oc.ObserverInformation{operationObserver()}}, func(o *observation.Service, _ *jobs.Service) error {
			v, err := o.List(ctx)
			if err == nil {
				assertObserver(t, v[0], false)
				if len(v) != 1 {
					t.Fatal(v)
				}
			}
			return err
		}},
		{"GetObserverInformation", &oc.GetObserverInformationRequest{EventStore: "store", Namespace: "tenant", ObserverId: "orders", EventSequenceId: "source-sequence"}, operationObserver(), func(o *observation.Service, _ *jobs.Service) error {
			v, err := o.Get(ctx, "orders", "source-sequence")
			if err == nil {
				assertObserver(t, *v, true)
			}
			return err
		}},
		{"GetFailedPartitions", &oc.GetFailedPartitionsRequest{EventStore: "store", Namespace: "tenant", ObserverId: "orders"}, &oc.IEnumerable_FailedPartition{Items: []*oc.FailedPartition{operationFailure()}}, func(o *observation.Service, _ *jobs.Service) error {
			v, err := o.FailedPartitions(ctx, "orders")
			if err == nil {
				assertFailure(t, v[0])
			}
			return err
		}},
		{"RemoveObserver", &oc.RemoveObserver{EventStore: "store", Namespace: "tenant", ObserverId: "orders", EventSequenceId: "source-sequence"}, &oc.RemoveObserverResponse{Outcome: oc.ObserverRemovalOutcome_ObserverSubscribed, BlockingNamespace: "other-tenant"}, func(o *observation.Service, _ *jobs.Service) error {
			v, err := o.Remove(ctx, "orders")
			if v.Outcome != observation.ObserverSubscribed || v.BlockingNamespace != "other-tenant" {
				t.Fatal(v)
			}
			return err
		}},
		{"Replay", &oc.Replay{EventStore: "store", Namespace: "tenant", ObserverId: "orders", EventSequenceId: "source-sequence"}, &oc.ReplayResponse{JobId: operationJobID.String()}, func(o *observation.Service, _ *jobs.Service) error {
			v, err := o.Replay(ctx, "orders", "source-sequence")
			if v.ID() != operationJobID {
				t.Fatal(v.ID())
			}
			return err
		}},
		{"ReplayPartition", &oc.ReplayPartition{EventStore: "store", Namespace: "tenant", ObserverId: "orders", EventSequenceId: "source-sequence", Partition: "customer/1"}, &emptypb.Empty{}, func(o *observation.Service, _ *jobs.Service) error {
			return o.ReplayPartition(ctx, "orders", "source-sequence", "customer/1")
		}},
		{"RetryPartition", &oc.RetryPartition{EventStore: "store", Namespace: "tenant", ObserverId: "orders", EventSequenceId: "source-sequence", Partition: "customer/1"}, &oc.RetryPartitionResponse{Outcome: oc.PartitionRecoveryOutcome_PartitionQuarantined}, func(o *observation.Service, _ *jobs.Service) error {
			v, err := o.RetryPartition(ctx, "orders", "source-sequence", "customer/1")
			if v != observation.PartitionQuarantined {
				t.Fatal(v)
			}
			return err
		}},
		{"ClearPartitionQuarantine", &oc.ClearPartitionQuarantine{EventStore: "store", Namespace: "tenant", ObserverId: "orders", EventSequenceId: "source-sequence", Partition: "customer/1", RetryImmediately: true}, &oc.ClearPartitionQuarantineResponse{Outcome: oc.ClearPartitionQuarantineOutcome_Cleared, RetryOutcome: oc.PartitionRecoveryOutcome_ObserverQuarantined}, func(o *observation.Service, _ *jobs.Service) error {
			v, err := o.ClearPartitionQuarantine(ctx, "orders", "source-sequence", "customer/1", true)
			if v != (observation.QuarantineResult{Outcome: observation.QuarantineCleared, RetryRequested: true, RetryOutcome: observation.ObserverQuarantined}) {
				t.Fatal(v)
			}
			return err
		}},
		{"ClearObserverQuarantine", &oc.ClearObserverQuarantine{EventStore: "store", Namespace: "tenant", ObserverId: "orders", EventSequenceId: "source-sequence"}, &emptypb.Empty{}, func(o *observation.Service, _ *jobs.Service) error {
			return o.ClearObserverQuarantine(ctx, "orders", "source-sequence")
		}},
		{"ClearFailedPartitions", &oc.ClearFailedPartitions{EventStore: "store", Namespace: "tenant", ObserverId: "orders", EventSequenceId: "source-sequence"}, &emptypb.Empty{}, func(o *observation.Service, _ *jobs.Service) error {
			return o.ClearFailedPartitions(ctx, "orders", "source-sequence")
		}},
		{"AllJobs", &jc.AllJobsRequest{EventStore: "store", Namespace: "tenant"}, &jc.QueryResult_IEnumerable_JobSummaryResponse{IsAuthorized: true, Data: []*jc.JobSummaryResponse{operationJob()}}, func(_ *observation.Service, j *jobs.Service) error {
			v, err := j.List(ctx)
			if err == nil {
				assertJob(t, v[0])
			}
			return err
		}},
		{"StopJob", &jc.StopJobRequest{EventStore: "store", Namespace: "tenant", JobId: operationGuid()}, &jc.CommandResult{IsAuthorized: true}, func(_ *observation.Service, j *jobs.Service) error { return j.Stop(ctx, operationJobID) }},
		{"ResumeJob", &jc.ResumeJobRequest{EventStore: "store", Namespace: "tenant", JobId: operationGuid()}, &jc.CommandResult{IsAuthorized: true}, func(_ *observation.Service, j *jobs.Service) error { return j.Resume(ctx, operationJobID) }},
		{"DeleteJob", &jc.DeleteJobRequest{EventStore: "store", Namespace: "tenant", JobId: operationGuid()}, &jc.CommandResult{IsAuthorized: true}, func(_ *observation.Service, j *jobs.Service) error { return j.Delete(ctx, operationJobID) }},
		{"GetJobSteps", &jc.GetJobStepsRequest{EventStore: "store", Namespace: "tenant", JobId: operationGuid()}, &jc.QueryResult_IEnumerable_JobStepSummaryResponse{IsAuthorized: true, Data: []*jc.JobStepSummaryResponse{{Id: &bcl.Guid{Lo: 7, Hi: 8}, Type: "ReplayPartition", Name: "customer/1", Status: jc.JobStepStatus_CompletedWithFailure, Progress: &jc.JobStepProgress{Percentage: 75, Message: "step progress"}, StatusChanges: []*jc.JobStepStatusChanged{{Status: jc.JobStepStatus_JOB_STEP_STATUS_Failed, Occurred: &jc.SerializableDateTimeOffset{Value: "2026-01-02T03:04:07+02:00"}, ExceptionMessages: []string{"step failure"}, ExceptionStackTrace: "step-stack"}}}}}, func(_ *observation.Service, j *jobs.Service) error {
			v, err := j.Steps(ctx, operationJobID)
			if err == nil {
				assertStep(t, v[0])
			}
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			conn := operationsConnection(t, func(_ context.Context, method string, request proto.Message) (proto.Message, error) {
				if tc.name == "RemoveObserver" && methodName(method) == "GetObservers" {
					return &oc.IEnumerable_ObserverInformation{Items: []*oc.ObserverInformation{operationObserver()}}, nil
				}
				calls++
				if methodName(method) != tc.name {
					t.Errorf("method %s", method)
				}
				equalOperation(t, request, tc.request)
				return tc.response, nil
			})
			o, j := operationServices(t, conn)
			if err := tc.run(o, j); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatalf("calls=%d", calls)
			}
		})
	}
}
func assertObserver(t *testing.T, v observation.Information, subscriptionKnown bool) {
	t.Helper()
	if v.ID() != "orders" || v.Sequence() != "source-sequence" || v.Type() != observation.Reactor || v.Owner() != observation.ClientOwner || v.RunningState() != observation.Quarantined || v.Next() != 10 || v.LastHandled() != 8 || v.Tail() != 12 || v.HandledEventCount() != 4 || v.SubscriptionKnown() != subscriptionKnown || v.IsSubscribed() != subscriptionKnown || !v.IsReplayable() || !reflect.DeepEqual(v.EventTypes(), []events.TypeRef{{ID: "ordered", Generation: 2}, {ID: "changed", Generation: 1}}) {
		t.Fatalf("observer fields: %#v", v)
	}
	refs := v.EventTypes()
	refs[0].ID = "changed-by-caller"
	if v.EventTypes()[0].ID != "ordered" {
		t.Fatal("mutable types")
	}
}
func assertFailure(t *testing.T, v observation.FailedPartition) {
	t.Helper()
	a := v.Attempts()[0]
	if v.ID() != operationJobID || v.Observer() != "orders" || v.Partition() != "customer/1" || !v.IsResolved() || !v.IsQuarantined() || a.Occurred().Format(time.RFC3339Nano) != "2026-01-02T03:04:05.1234567+02:00" || a.Position() != 8 || a.Kind() != observation.HandlingFailure || a.StackTrace() != "failure-stack" || !reflect.DeepEqual(a.Messages(), []string{"failed", "because"}) {
		t.Fatal("failure fields lost")
	}
	a.Messages()[0] = "changed"
	v.Attempts()[0] = observation.FailedAttempt{}
	if v.Attempts()[0].Messages()[0] != "failed" {
		t.Fatal("mutable failure")
	}
}
func assertJob(t *testing.T, v jobs.Job) {
	t.Helper()
	if v.ID() != operationJobID || v.Handle().ID() != operationJobID || v.Type() != "ReplayObserver" || v.Details() != "Replay orders" || v.Status() != jobs.Running || v.Created().Format(time.RFC3339Nano) != "2026-01-02T03:04:05.1234567+02:00" || v.Progress() != (jobs.Progress{TotalSteps: 4, SuccessfulSteps: 1, FailedSteps: 1, StoppedSteps: 1, Message: "working"}) {
		t.Fatal("job fields lost")
	}
	c := v.StatusChanges()[0]
	if c.Status() != jobs.Running || c.Occurred().Format(time.RFC3339Nano) != "2026-01-02T03:04:06+02:00" || c.ExceptionStackTrace() != "job-stack" || !reflect.DeepEqual(c.ExceptionMessages(), []string{"diagnostic"}) {
		t.Fatal("job changes lost")
	}
	c.ExceptionMessages()[0] = "changed"
	v.StatusChanges()[0] = jobs.StatusChange{}
	if v.StatusChanges()[0].ExceptionMessages()[0] != "diagnostic" {
		t.Fatal("mutable job")
	}
}
func assertStep(t *testing.T, v jobs.Step) {
	t.Helper()
	if v.ID() != uuid.MustParse("00000007-0000-0000-0800-000000000000") || v.Type() != "ReplayPartition" || v.Name() != "customer/1" || v.Status() != jobs.StepCompletedWithFailure || v.Progress() != (jobs.StepProgress{Percentage: 75, Message: "step progress"}) {
		t.Fatal("step fields lost", v.ID())
	}
	c := v.StatusChanges()[0]
	if c.Status() != jobs.StepFailed || c.Occurred().Format(time.RFC3339Nano) != "2026-01-02T03:04:07+02:00" || c.ExceptionStackTrace() != "step-stack" || !reflect.DeepEqual(c.ExceptionMessages(), []string{"step failure"}) {
		t.Fatal("step changes lost")
	}
	c.ExceptionMessages()[0] = "changed"
	if v.StatusChanges()[0].ExceptionMessages()[0] != "step failure" {
		t.Fatal("mutable step")
	}
}
