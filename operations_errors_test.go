// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	jc "github.com/cratis/chronicle.go/contracts/jobs"
	oc "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/jobs"
	"github.com/cratis/chronicle.go/observation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestAdministrationLostRepliesNeverRetry(t *testing.T) {
	mutations := []struct {
		name string
		call func(context.Context, *observation.Service, *jobs.Service) error
	}{
		{"remove", func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			_, err := o.Remove(ctx, "orders")
			return err
		}},
		{"replay", func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			_, err := o.Replay(ctx, "orders", "source-sequence")
			return err
		}},
		{"replay partition", func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			return o.ReplayPartition(ctx, "orders", "source-sequence", "partition")
		}},
		{"retry", func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			_, err := o.RetryPartition(ctx, "orders", "source-sequence", "partition")
			return err
		}},
		{"clear failures", func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			return o.ClearFailedPartitions(ctx, "orders", "source-sequence")
		}},
		{"clear observer", func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			return o.ClearObserverQuarantine(ctx, "orders", "source-sequence")
		}},
		{"clear partition", func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			_, err := o.ClearPartitionQuarantine(ctx, "orders", "source-sequence", "partition", true)
			return err
		}},
		{"stop", func(ctx context.Context, _ *observation.Service, j *jobs.Service) error {
			return j.Stop(ctx, operationJobID)
		}},
		{"resume", func(ctx context.Context, _ *observation.Service, j *jobs.Service) error {
			return j.Resume(ctx, operationJobID)
		}},
		{"delete", func(ctx context.Context, _ *observation.Service, j *jobs.Service) error {
			return j.Delete(ctx, operationJobID)
		}},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			o, j := operationServices(t, operationsConnection(t, func(_ context.Context, method string, _ proto.Message) (proto.Message, error) {
				if tc.name == "remove" && methodName(method) == "GetObservers" {
					return &oc.IEnumerable_ObserverInformation{Items: []*oc.ObserverInformation{operationObserver()}}, nil
				}
				calls.Add(1)
				return nil, status.Error(codes.Unavailable, "sensitive-operation-marker")
			}))
			err := tc.call(testContext(t), o, j)
			var unknown *jobs.OutcomeUnknownError
			if !errors.As(err, &unknown) || status.Code(err) != codes.Unavailable || calls.Load() != 1 {
				t.Fatal(err, calls.Load())
			}
			if strings.Contains(err.Error(), "sensitive-operation-marker") || !strings.Contains(unknown.Unwrap().Error(), "sensitive-operation-marker") {
				t.Fatal("error formatting leaked or lost deliberate cause access", err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			err = tc.call(ctx, o, j)
			if !errors.Is(err, context.Canceled) || errors.As(err, &unknown) || calls.Load() != 1 {
				t.Fatal("pre-cancel dispatched", err, calls.Load())
			}
		})
	}
}

func TestAdministrationRejectsMalformedResponses(t *testing.T) {
	c, err := observation.NewCompletion("store", "tenant", "source-sequence", []events.TypeRef{{ID: "ordered", Generation: 1}}, []events.SequenceNumber{0})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		reply    proto.Message
		call     func(context.Context, *observation.Service, *jobs.Service) error
		mutation bool
	}{
		{"observer missing ID", &oc.ObserverInformation{}, func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			_, e := o.Get(ctx, "orders", "source-sequence")
			return e
		}, false},
		{"observer different target", &oc.ObserverInformation{Id: "foreign", EventSequenceId: "source-sequence"}, func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			_, e := o.Get(ctx, "orders", "source-sequence")
			return e
		}, false},
		{"failed attempt invalid time", &oc.IEnumerable_FailedPartition{Items: []*oc.FailedPartition{{Id: operationGuid(), ObserverId: "orders", Attempts: []*oc.FailedPartitionAttempt{{Occurred: &oc.SerializableDateTimeOffset{Value: "invalid"}}}}}}, func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			_, e := o.FailedPartitions(ctx, "orders")
			return e
		}, false},
		{"invalid replay job", &oc.ReplayResponse{JobId: "not-a-uuid"}, func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			_, e := o.Replay(ctx, "orders", "source-sequence")
			return e
		}, true},
		{"unknown removal", &oc.RemoveObserverResponse{Outcome: 99}, func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			_, e := o.Remove(ctx, "orders")
			return e
		}, true},
		{"unknown recovery", &oc.RetryPartitionResponse{Outcome: 99}, func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			_, e := o.RetryPartition(ctx, "orders", "source-sequence", "partition")
			return e
		}, true},
		{"unknown quarantine", &oc.ClearPartitionQuarantineResponse{RetryOutcome: 99}, func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			_, e := o.ClearPartitionQuarantine(ctx, "orders", "source-sequence", "partition", true)
			return e
		}, true},
		{"contradictory completion", &oc.WaitForObserverCompletionResponse{IsSuccess: true, TimedOut: true}, func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			_, e := o.WaitForCompletion(ctx, c, 0)
			return e
		}, false},
		{"empty completion", &oc.WaitForObserverCompletionResponse{}, func(ctx context.Context, o *observation.Service, _ *jobs.Service) error {
			_, e := o.WaitForCompletion(ctx, c, 0)
			return e
		}, false},
		{"job missing fields", &jc.QueryResult_IEnumerable_JobSummaryResponse{IsAuthorized: true, Data: []*jc.JobSummaryResponse{{Id: operationGuid()}}}, func(ctx context.Context, _ *observation.Service, j *jobs.Service) error {
			_, e := j.List(ctx)
			return e
		}, false},
		{"duplicate job", &jc.QueryResult_IEnumerable_JobSummaryResponse{IsAuthorized: true, Data: []*jc.JobSummaryResponse{operationJob(), operationJob()}}, func(ctx context.Context, _ *observation.Service, j *jobs.Service) error {
			_, e := j.Get(ctx, operationJobID)
			return e
		}, false},
		{"step missing progress", &jc.QueryResult_IEnumerable_JobStepSummaryResponse{IsAuthorized: true, Data: []*jc.JobStepSummaryResponse{{Id: operationGuid(), Type: "Replay"}}}, func(ctx context.Context, _ *observation.Service, j *jobs.Service) error {
			_, e := j.Steps(ctx, operationJobID)
			return e
		}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			o, j := operationServices(t, operationsConnection(t, func(_ context.Context, method string, _ proto.Message) (proto.Message, error) {
				if tc.name == "unknown removal" && methodName(method) == "GetObservers" {
					return &oc.IEnumerable_ObserverInformation{Items: []*oc.ObserverInformation{operationObserver()}}, nil
				}
				return tc.reply, nil
			}))
			err := tc.call(testContext(t), o, j)
			var unknown *jobs.OutcomeUnknownError
			if !errors.Is(err, chronicle.ErrProtocol) || errors.As(err, &unknown) != tc.mutation {
				t.Fatal(err)
			}
		})
	}
}

func TestAdministrationPreservesRefusalsAbsenceAndTargetIsolation(t *testing.T) {
	for i := range 4 {
		t.Run(fmt.Sprint("retry-", i), func(t *testing.T) {
			o, _ := operationServices(t, operationsConnection(t, func(context.Context, string, proto.Message) (proto.Message, error) {
				return &oc.RetryPartitionResponse{Outcome: oc.PartitionRecoveryOutcome(i)}, nil
			}))
			v, err := o.RetryPartition(testContext(t), "orders", "source-sequence", "partition")
			if err != nil || v != observation.RecoveryOutcome(i) {
				t.Fatal(v, err)
			}
		})
	}
	for i := range 4 {
		t.Run(fmt.Sprint("remove-", i), func(t *testing.T) {
			o, _ := operationServices(t, operationsConnection(t, func(_ context.Context, method string, _ proto.Message) (proto.Message, error) {
				if methodName(method) == "GetObservers" {
					return &oc.IEnumerable_ObserverInformation{Items: []*oc.ObserverInformation{operationObserver()}}, nil
				}
				return &oc.RemoveObserverResponse{Outcome: oc.ObserverRemovalOutcome(i)}, nil
			}))
			v, err := o.Remove(testContext(t), "orders")
			if err != nil || v.Outcome != observation.RemovalOutcome(i) {
				t.Fatal(v, err)
			}
		})
	}
	for i := range 3 {
		for _, retry := range []bool{false, true} {
			t.Run(fmt.Sprintf("quarantine-%d-%t", i, retry), func(t *testing.T) {
				o, _ := operationServices(t, operationsConnection(t, func(_ context.Context, _ string, request proto.Message) (proto.Message, error) {
					if request.(*oc.ClearPartitionQuarantine).RetryImmediately != retry {
						t.Error("retry changed")
					}
					return &oc.ClearPartitionQuarantineResponse{Outcome: oc.ClearPartitionQuarantineOutcome(i)}, nil
				}))
				v, err := o.ClearPartitionQuarantine(testContext(t), "orders", "source-sequence", "partition", retry)
				if err != nil || v.Outcome != observation.QuarantineOutcome(i) || v.RetryRequested != retry {
					t.Fatal(v, err)
				}
			})
		}
	}
	for _, tc := range []struct {
		name    string
		reply   *jc.CommandResult
		unknown bool
	}{{"authorization", &jc.CommandResult{AuthorizationFailureReason: "denied"}, false}, {"validation", &jc.CommandResult{IsAuthorized: true, ValidationResults: []*jc.ValidationResult{{Severity: jc.ValidationResultSeverity_Error, Message: "invalid", Members: []string{"JobId"}}}}, false}, {"exception", &jc.CommandResult{IsAuthorized: true, ExceptionMessages: []string{"partial effect"}}, true}} {
		t.Run(tc.name, func(t *testing.T) {
			_, j := operationServices(t, operationsConnection(t, func(context.Context, string, proto.Message) (proto.Message, error) { return tc.reply, nil }))
			err := j.Stop(testContext(t), operationJobID)
			var envelope *chronicle.EnvelopeError
			var unknown *jobs.OutcomeUnknownError
			if !errors.As(err, &envelope) || errors.As(err, &unknown) != tc.unknown {
				t.Fatal(err)
			}
		})
	}
	conn := operationsConnection(t, func(_ context.Context, method string, request proto.Message) (proto.Message, error) {
		switch methodName(method) {
		case "GetObserverInformation":
			return nil, status.Error(codes.NotFound, "absent")
		case "Replay":
			return &oc.ReplayResponse{JobId: operationJobID.String()}, nil
		case "AllJobs":
			equalOperation(t, request, &jc.AllJobsRequest{EventStore: "second-store", Namespace: "other"})
			return &jc.QueryResult_IEnumerable_JobSummaryResponse{IsAuthorized: true}, nil
		case "GetJobSteps":
			equalOperation(t, request, &jc.GetJobStepsRequest{EventStore: "second-store", Namespace: "other", JobId: operationGuid()})
			return &jc.QueryResult_IEnumerable_JobStepSummaryResponse{IsAuthorized: true}, nil
		}
		return nil, status.Error(codes.Unimplemented, method)
	})
	o, err := observation.New("second-store", "other", conn)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := o.Get(testContext(t), "orders", "source-sequence"); v != nil || err != nil {
		t.Fatal(v, err)
	}
	handle, err := o.Replay(testContext(t), "orders", "source-sequence")
	if err != nil {
		t.Fatal(err)
	}
	if v, err := handle.Get(testContext(t)); v != nil || err != nil {
		t.Fatal(v, err)
	}
	if v, err := handle.Steps(testContext(t)); len(v) != 0 || err != nil {
		t.Fatal(v, err)
	}
}
