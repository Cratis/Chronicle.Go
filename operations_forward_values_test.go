// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	jc "github.com/cratis/chronicle.go/contracts/jobs"
	oc "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/jobs"
	"github.com/cratis/chronicle.go/observation"
	"google.golang.org/protobuf/proto"
)

func TestAdministrationPreservesForwardDiagnosticValues(t *testing.T) {
	ctx := testContext(t)
	observer := operationObserver()
	observer.Type, observer.Owner, observer.RunningState = 91, 92, 93
	failure := operationFailure()
	failure.Attempts[0].Kind = 94
	job := operationJob()
	job.Status, job.StatusChanges[0].Status = 95, 96
	step := &jc.JobStepSummaryResponse{Id: operationGuid(), Type: "FutureStep", Status: 97, Progress: &jc.JobStepProgress{}, StatusChanges: []*jc.JobStepStatusChanged{{Status: 98, Occurred: job.Created}}}
	o, j := operationServices(t, operationsConnection(t, func(_ context.Context, method string, _ proto.Message) (proto.Message, error) {
		switch methodName(method) {
		case "GetObservers":
			return &oc.IEnumerable_ObserverInformation{Items: []*oc.ObserverInformation{observer, operationObserver()}}, nil
		case "GetObserverInformation":
			return observer, nil
		case "GetFailedPartitions":
			return &oc.IEnumerable_FailedPartition{Items: []*oc.FailedPartition{failure}}, nil
		case "WaitForCompletion":
			return &oc.WaitForObserverCompletionResponse{FailedPartitions: []*oc.FailedPartition{failure}}, nil
		case "AllJobs":
			return &jc.QueryResult_IEnumerable_JobSummaryResponse{IsAuthorized: true, Data: []*jc.JobSummaryResponse{job}}, nil
		case "GetJobSteps":
			return &jc.QueryResult_IEnumerable_JobStepSummaryResponse{IsAuthorized: true, Data: []*jc.JobStepSummaryResponse{step}}, nil
		}
		panic("unexpected operation")
	}))
	all, err := o.List(ctx)
	if err != nil || len(all) != 2 || all[1].Type() != observation.Reactor {
		t.Fatal(all, err)
	}
	got, err := o.Get(ctx, "orders", "source-sequence")
	if err != nil || got == nil {
		t.Fatal(got, err)
	}
	for _, v := range []observation.Information{all[0], *got} {
		if v.Type() != 91 || v.Owner() != 92 || v.RunningState() != 93 {
			t.Fatal("observer diagnostics lost", v)
		}
	}
	failed, err := o.FailedPartitions(ctx, "orders")
	if err != nil || len(failed) != 1 || failed[0].Attempts()[0].Kind() != 94 {
		t.Fatal(failed, err)
	}
	completion, err := observation.NewCompletion("store", "tenant", "source-sequence", []events.TypeRef{{ID: "ordered", Generation: 1}}, []events.SequenceNumber{0})
	if err != nil {
		t.Fatal(err)
	}
	waited, err := o.WaitForCompletion(ctx, completion, 0)
	if err != nil || waited.IsSuccess() || waited.FailedPartitions()[0].Attempts()[0].Kind() != 94 {
		t.Fatal(waited, err)
	}
	listed, err := j.List(ctx)
	if err != nil || len(listed) != 1 || listed[0].Status() != 95 || listed[0].StatusChanges()[0].Status() != 96 {
		t.Fatal(listed, err)
	}
	gotJob, err := j.Get(ctx, operationJobID)
	if err != nil || gotJob == nil || gotJob.Status() != 95 || gotJob.Status().Terminal() {
		t.Fatal(gotJob, err)
	}
	steps, err := gotJob.Steps(ctx)
	if err != nil || len(steps) != 1 || steps[0].Status() != 97 || steps[0].StatusChanges()[0].Status() != 98 {
		t.Fatal(steps, err)
	}
}

func TestUnknownJobStatusIsNotCompletionOrTerminalEvidence(t *testing.T) {
	for _, mode := range []string{"completion", "terminal", "no jobs", "no selected jobs", "predicate"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				_, j := operationServices(t, inMemoryOperations{call: func(context.Context, string, proto.Message) (proto.Message, error) {
					calls++
					job := operationJob()
					job.Status = 95
					job.Progress.IsCompleted = true // Progress cannot turn an unknown status into success.
					return &jc.QueryResult_IEnumerable_JobSummaryResponse{IsAuthorized: true, Data: []*jc.JobSummaryResponse{job}}, nil
				}})
				var err error
				switch mode {
				case "completion":
					_, err = j.WaitForCompletion(t.Context(), operationJobID, time.Second)
				case "terminal":
					_, err = j.WaitForTerminalOrAbsent(t.Context(), operationJobID, time.Second)
				case "no jobs":
					err = j.WaitForNoJobs(t.Context(), time.Second)
				case "no selected jobs":
					err = j.WaitForNoJobs(t.Context(), time.Second, jobs.Status(95))
				case "predicate":
					var got *jobs.Job
					got, err = j.WaitFor(t.Context(), operationJobID, time.Second, func(v jobs.Job) bool { return v.Status() == 95 })
					if err != nil || got == nil || got.Status() != 95 {
						t.Fatal(got, err)
					}
					return
				}
				if !errors.Is(err, context.DeadlineExceeded) || calls < 2 {
					t.Fatal("unknown status ended wait", err, calls)
				}
			})
		})
	}
}
