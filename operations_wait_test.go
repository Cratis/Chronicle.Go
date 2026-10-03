// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	jc "github.com/cratis/chronicle.go/contracts/jobs"
	oc "github.com/cratis/chronicle.go/contracts/observation"
	sc "github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/jobs"
	"github.com/cratis/chronicle.go/observation"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestJobWaitRequiresEvidenceAndPreservesDisappearance(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		statuses               []jc.JobStatus
		absent, seen, terminal bool
	}{
		{name: "initial absence", absent: true},
		{name: "disappeared", statuses: []jc.JobStatus{jc.JobStatus_JOB_STATUS_Running}, absent: true, seen: true},
		{name: "completed", statuses: []jc.JobStatus{jc.JobStatus_JOB_STATUS_Running, jc.JobStatus_JOB_STATUS_CompletedSuccessfully}},
		{name: "failed", statuses: []jc.JobStatus{jc.JobStatus_JOB_STATUS_Failed}, terminal: true},
		{name: "completed with failures", statuses: []jc.JobStatus{jc.JobStatus_CompletedWithFailures}, terminal: true},
		{name: "stopped", statuses: []jc.JobStatus{jc.JobStatus_JOB_STATUS_Stopped}, terminal: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			conn := operationsConnection(t, func(_ context.Context, _ string, request proto.Message) (proto.Message, error) {
				equalOperation(t, request, &jc.AllJobsRequest{EventStore: "store", Namespace: "tenant"})
				r := &jc.QueryResult_IEnumerable_JobSummaryResponse{IsAuthorized: true}
				if calls < len(tc.statuses) {
					job := operationJob()
					job.Status = tc.statuses[calls]
					r.Data = []*jc.JobSummaryResponse{job}
				}
				calls++
				return r, nil
			})
			_, j := operationServices(t, conn)
			v, err := j.WaitForCompletion(testContext(t), operationJobID, 0)
			if tc.absent {
				var absent *jobs.AbsentError
				if !errors.As(err, &absent) || absent.Seen != tc.seen || absent.ID != operationJobID || v != nil {
					t.Fatalf("absence = %v %v", v, err)
				}
			} else if tc.terminal {
				var terminal *jobs.TerminalError
				if !errors.As(err, &terminal) || terminal.Job.ID() != operationJobID || v == nil {
					t.Fatal(v, err)
				}
			} else if err != nil || v == nil || v.Status() != jobs.CompletedSuccessfully {
				t.Fatal(v, err)
			}
		})
	}
}

func TestJobWaitHelpersHonorBudgetsAndCallerCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		calls := 0
		present := true
		_, j := operationServices(t, inMemoryOperations{call: func(_ context.Context, _ string, _ proto.Message) (proto.Message, error) {
			calls++
			r := &jc.QueryResult_IEnumerable_JobSummaryResponse{IsAuthorized: true}
			if present {
				r.Data = []*jc.JobSummaryResponse{operationJob()}
			}
			return r, nil
		}})
		start := time.Now()
		_, err := j.WaitForCompletion(ctx, operationJobID, 0)
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != 5*time.Second || calls < 2 {
			t.Fatal(err, time.Since(start), calls)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		before := calls
		_, err = j.WaitForCompletion(cancelled, operationJobID, jobs.InfiniteTimeout)
		if !errors.Is(err, context.Canceled) || calls != before {
			t.Fatal(err, calls, before)
		}
		deadline, stop := context.WithTimeout(ctx, 17*time.Millisecond)
		defer stop()
		_, err = j.WaitForCompletion(deadline, operationJobID, jobs.InfiniteTimeout)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		present = false
		if v, err := j.WaitForTerminalOrAbsent(ctx, operationJobID, 0); err != nil || v != nil {
			t.Fatal(v, err)
		}
		if err := j.WaitForDeletion(ctx, operationJobID, 0); err != nil {
			t.Fatal(err)
		}
		if err := j.WaitForNoJobs(ctx, 0); err != nil {
			t.Fatal(err)
		}
		if v, err := j.TryFindJobsOfType(ctx, "Replay", time.Millisecond); err != nil || len(v) != 0 {
			t.Fatal(v, err)
		}
		present = true
		if v, err := j.WaitForJobs(ctx, "Replay", 0); err != nil || len(v) != 1 {
			t.Fatal(v, err)
		}
		if err := j.WaitForNoJobs(ctx, time.Millisecond, jobs.Running); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		if err := j.WaitForNoJobs(ctx, 0, jobs.Failed); err != nil {
			t.Fatal(err)
		}
		_, broken := operationServices(t, inMemoryOperations{call: func(context.Context, string, proto.Message) (proto.Message, error) {
			return nil, status.Error(codes.DeadlineExceeded, "server timeout")
		}})
		if _, err := broken.TryFindJobsOfType(ctx, "Replay", 0); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("server error hidden", err)
		}
	})
}

type OperationChanged struct{ Number int }
type OperationRegisteredV2 struct{ Name string }

func TestCompletionUsesActualBatchTailsAndOriginalCoordinates(t *testing.T) {
	ctx := testContext(t)
	conn := operationsConnection(t, func(_ context.Context, method string, request proto.Message) (proto.Message, error) {
		switch methodName(method) {
		case "AppendMany":
			return &sc.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sc.AppendManyResponse{IsSuccess: true, SequenceNumbers: []uint64{3, 5, 8}}}, nil
		case "WaitForCompletion":
			// Hand-derived C# AppendResultWaitForCompletionExtensions 2e31b0dfb:
			// A1/B/A2 => A's tail 8, B's tail 5, first 3; not B=8.
			equalOperation(t, request, &oc.WaitForObserverCompletionRequest{EventStore: "store", Namespace: "tenant", EventSequenceId: "original", TailEventSequenceNumber: 8, FirstEventSequenceNumber: 3, HasFirstEventSequenceNumber: true, TimeoutMilliseconds: 5000, EventTypeTails: []*oc.AppendedEventTypeTail{{EventType: &oc.EventType{Id: "a", Generation: 2}, SequenceNumber: 8}, {EventType: &oc.EventType{Id: "b", Generation: 1}, SequenceNumber: 5}}})
			return &oc.WaitForObserverCompletionResponse{IsSuccess: true}, nil
		case "TailSequenceNumber":
			equalOperation(t, request, &sc.TailSequenceNumberRequest{EventStore: "store", Namespace: "tenant", EventSequenceId: "original", EventTypeIds: "a,b"})
			return &sc.QueryResult_EventSequenceTailResponse{IsAuthorized: true, Data: &sc.EventSequenceTailResponse{SequenceNumber: uint64(events.Unavailable)}}, nil
		default:
			t.Errorf("unexpected RPC %s", method)
			return nil, status.Error(codes.Unimplemented, method)
		}
	})
	a2, err := events.Define[OperationRegisteredV2](events.WithID("a"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	a, err := events.DefineGeneration[CustomerRegistered](a2, 1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := events.Define[OperationChanged](events.WithID("b"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(a.Descriptor(), b.Descriptor(), a2.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := eventsequences.New("store", "tenant", "original", catalog, conn)
	if err != nil {
		t.Fatal(err)
	}
	result, err := sequence.AppendManyWithMetadata(ctx, "source", []any{CustomerRegistered{}, OperationChanged{2}, OperationRegisteredV2{}}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	if err != nil {
		t.Fatal(err)
	}
	c, err := result.Completion()
	if err != nil {
		t.Fatal(err)
	}
	target := c.Target()
	*target.First = 90
	target.EventTypeTails["b"] = 90
	o, _ := operationServices(t, conn)
	waited, err := o.WaitForCompletion(ctx, c, 0)
	if err != nil || !waited.IsSuccess() || waited.Trivial() {
		t.Fatal(waited, err)
	}
	foreign, err := observation.New("store", "different-namespace", conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.WaitForCompletion(ctx, c, 0); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	otherStore, err := observation.New("different-store", "tenant", conn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := otherStore.WaitForCompletion(ctx, c, 0); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	if tail, present, err := sequence.TailForObserver(ctx, []events.TypeRef{{ID: "a", Generation: 1}, {ID: "b", Generation: 1}}); err != nil || present || tail != 0 {
		t.Fatal(tail, present, err)
	}
}

func TestCompletionBudgetsAbsenceCancellationAndDiagnostics(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, err := observation.NewCompletion("store", "tenant", "original", []events.TypeRef{{ID: "a", Generation: 2}, {ID: "a", Generation: 3}, {ID: "b", Generation: 1}}, []events.SequenceNumber{0, 8, 5})
		if err != nil {
			t.Fatal(err)
		}
		var absent *observation.Service
		if r, err := absent.WaitForCompletion(t.Context(), observation.Completion{}, 0); err != nil || !r.IsSuccess() || !r.Trivial() {
			t.Fatal(r, err)
		}
		var cannot *observation.CannotWaitError
		if _, err := absent.WaitForCompletion(t.Context(), c, 0); !errors.As(err, &cannot) {
			t.Fatal(err)
		}
		calls := 0
		o, _ := operationServices(t, inMemoryOperations{call: func(ctx context.Context, _ string, request proto.Message) (proto.Message, error) {
			calls++
			r := request.(*oc.WaitForObserverCompletionRequest)
			if r.EventTypeTails[0].EventType.Generation != 3 || r.FirstEventSequenceNumber != 0 || !r.HasFirstEventSequenceNumber {
				t.Fatal(r)
			}
			deadline, ok := ctx.Deadline()
			if calls == 1 {
				if !ok || time.Until(deadline) != 5200*time.Millisecond || r.TimeoutMilliseconds != 5000 {
					t.Fatal(deadline, r)
				}
			}
			if calls == 2 {
				if ok || r.TimeoutMilliseconds != 0 {
					t.Fatal("infinite acquired deadline", r)
				}
				return &oc.WaitForObserverCompletionResponse{TimedOut: true, OutstandingObservers: []string{"slow"}, FailedPartitions: []*oc.FailedPartition{operationFailure()}}, nil
			}
			<-ctx.Done()
			return nil, status.FromContextError(ctx.Err()).Err()
		}})
		start := time.Now()
		r, err := o.WaitForCompletion(t.Context(), c, 0)
		if err != nil || r.IsSuccess() || !r.TimedOut() || time.Since(start) != 5200*time.Millisecond {
			t.Fatal(r, err, time.Since(start))
		}
		r, err = o.WaitForCompletion(t.Context(), c, observation.InfiniteTimeout)
		if err != nil || r.IsSuccess() || !r.TimedOut() || len(r.OutstandingObservers()) != 1 {
			t.Fatal(r, err)
		}
		assertFailure(t, r.FailedPartitions()[0])
		r.OutstandingObservers()[0] = "mutated"
		if r.OutstandingObservers()[0] != "slow" {
			t.Fatal("mutable diagnostics")
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if _, err = o.WaitForCompletion(ctx, c, 0); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		ctx, cancel = context.WithCancel(t.Context())
		cancel()
		if _, err = o.WaitForCompletion(ctx, c, 0); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}
