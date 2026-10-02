// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package transactions_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/transactions"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConcurrentStageEnrollsEachCallContiguously(t *testing.T) {
	const participants = 32
	ctx, sequence, calls := fixture(t, func(_ context.Context, input any) (any, error) {
		request := input.(*sequences.AppendManyForEventSourcesRequest)
		if len(request.Events) != participants*2 {
			t.Errorf("events=%d", len(request.Events))
		}
		seen := make(map[string]bool)
		for i := 0; i < len(request.Events); i += 2 {
			source := request.Events[i].EventSourceId
			if seen[source] || request.Events[i+1].EventSourceId != source {
				t.Error("split or duplicate stage", source)
			}
			seen[source] = true
		}
		return success(len(request.Events)), nil
	})
	unit, owner := begin(t, ctx, sequence)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range participants {
		wg.Go(func() {
			<-start
			source := strconv.Itoa(i)
			entries := []eventsequences.Entry{{Source: events.SourceID(source), Event: changed{Value: "first"}}, {Source: events.SourceID(source), Event: changed{Value: "second"}}}
			if err := unit.Stage(ctx, entries, scope(source)); err != nil {
				t.Error(err)
			}
			_ = unit.GetEvents()
			_, _ = unit.Result()
		})
	}
	close(start)
	wg.Wait()
	if result, err := owner.Commit(ctx); err != nil || result.Err() != nil || calls.Load() != 1 {
		t.Fatal(result, err, calls.Load())
	}
}

func TestCommitFreezesParticipantsAndCancellationIsUnknown(t *testing.T) {
	entered := make(chan struct{})
	exited := make(chan struct{})
	ctx, sequence, calls := fixture(t, func(ctx context.Context, _ any) (any, error) {
		defer close(exited)
		close(entered)
		<-ctx.Done()
		return nil, status.Error(codes.Canceled, "acknowledgement lost")
	})
	unit, owner := begin(t, ctx, sequence)
	stage(t, ctx, unit, "A", "pending")
	commitCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	completed := make(chan error, 1)
	go func() { _, err := owner.Commit(commitCtx); completed <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if unit.State() != transactions.Completing || unit.IsCompleted() {
		t.Fatal(unit.State())
	}
	if err := unit.Stage(ctx, nil); !errors.Is(err, transactions.ErrCompleting) {
		t.Fatal(err)
	}
	if _, err := owner.Commit(ctx); !errors.Is(err, transactions.ErrCompleting) {
		t.Fatal(err)
	}
	if err := owner.Rollback(); !errors.Is(err, transactions.ErrCompleting) {
		t.Fatal(err)
	}
	cancel()
	var err error
	select {
	case err = <-completed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-exited:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	var unknown *eventsequences.OutcomeUnknownError
	if !errors.As(err, &unknown) || !errors.Is(err, context.Canceled) || unit.State() != transactions.OutcomeUnknown {
		t.Fatal(err, unit.State())
	}
	if err := owner.Rollback(); err != nil {
		t.Fatal(err)
	}
	if unit.State() != transactions.OutcomeUnknown || calls.Load() != 1 {
		t.Fatal("rollback changed unknown disposition")
	}
}

func TestTailFailureRejectsBeforeAppendAndDoesNotResolveDuringStage(t *testing.T) {
	ctx, sequence, calls := fixture(t, func(_ context.Context, input any) (any, error) {
		if _, ok := input.(*sequences.TailSequenceNumberRequest); !ok {
			t.Errorf("unexpected RPC %T", input)
		}
		return nil, status.Error(codes.Unavailable, "tail unavailable")
	})
	unit, owner := begin(t, ctx, sequence)
	if err := unit.Stage(ctx, []eventsequences.Entry{{Source: "A", Event: changed{}}}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatal("Stage resolved history")
	}
	result, err := owner.Commit(ctx)
	if err == nil || unit.State() != transactions.Rejected || result.Disposition != eventsequences.Rejected || calls.Load() != 1 {
		t.Fatal(result, err, unit.State())
	}
}
