// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reducers_test

import (
	"context"
	"errors"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/observation"
	"github.com/cratis/chronicle.go/reducers"
)

type BatchLedger struct{ calls *int }

func (l *BatchLedger) Reduce(_ context.Context, batch *observation.EventBatch, current *Total, bc observation.BatchContext) (*Total, error) {
	*l.calls++
	total := Total{}
	if current != nil {
		total = *current
	}
	for _, item := range batch.All() {
		switch e := item.Event.(type) {
		case Changed:
			total.ID, total.Amount = string(item.Context.SourceID), total.Amount+e.Amount
			if e.Amount < 0 {
				_ = batch.FailEvent(item.Context, errors.New("negative amount"))
			}
			if e.Amount == 0 && bc.CanDefer {
				_ = batch.Defer(item.Context.SourceID, time.Minute)
			}
		case Removed:
			return nil, nil
		}
	}
	return &total, nil
}

type TwoBatchFolds struct{}

func (TwoBatchFolds) A(*observation.EventBatch, *Total) *Total { return nil }
func (TwoBatchFolds) B(*observation.EventBatch, *Total) *Total { return nil }

type MixedFolds struct{}

func (MixedFolds) Batch(*observation.EventBatch, *Total) *Total { return nil }
func (MixedFolds) Each(Removed, *Total) *Total                  { return nil }

type WrongBatchCurrent struct{}

func (WrongBatchCurrent) Batch(*observation.EventBatch, *Other) *Total { return nil }

type FoldWithBatchContext struct{}

func (FoldWithBatchContext) Each(Changed, *Total, observation.BatchContext) *Total { return nil }

var observesLedger = reducers.WithBatchMethod("Reduce", observation.Observes[Changed](), observation.Observes[Removed]())

func batchPlan(t *testing.T, calls *int, options ...reducers.Option) *reducers.Plan {
	t.Helper()
	c, m, models := catalog(t)
	d, err := reducers.Define[*BatchLedger](m, func() *BatchLedger { return &BatchLedger{calls} }, append([]reducers.Option{observesLedger}, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	p, err := reducers.Compile(d, c, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestBatchReducerFoldsAPartitionInOneCall(t *testing.T) {
	calls := 0
	p := batchPlan(t, &calls)
	if !p.HasBatchMethod() || len(p.EventTypes()) != 2 {
		t.Fatal("batch method not compiled")
	}
	result := p.Reduce(t.Context(), []reducers.Event{event("Changed", 4, &Changed{2}), event("Changed", 7, Changed{3})}, &Total{Amount: 10})
	if result.Err != nil || calls != 1 || result.State.(*Total).Amount != 15 || result.LastSuccessful != 7 {
		t.Fatalf("result = %+v, calls = %d", result, calls)
	}
	removed := p.Reduce(t.Context(), []reducers.Event{event("Removed", 8, Removed{})}, &Total{})
	if removed.Err != nil || removed.State != nil {
		t.Fatalf("nil result must delete: %+v", removed)
	}
	empty := p.Reduce(t.Context(), nil, &Total{Amount: 1})
	if calls != 2 || empty.State.(*Total).Amount != 1 {
		t.Fatal("an empty partition must keep its state without calling the batch method")
	}
}

func TestBatchReducerFailAndDeferWriteNoState(t *testing.T) {
	calls := 0
	p := batchPlan(t, &calls)
	failed := p.Reduce(t.Context(), []reducers.Event{event("Changed", 1, Changed{2}), event("Changed", 2, Changed{-1})}, nil)
	if failed.Err == nil || failed.State != nil || failed.LastSuccessful != events.Unavailable {
		t.Fatalf("failed = %+v", failed)
	}
	paged := observation.BatchContext{CanDefer: true, Origin: observation.OriginLive}
	deferred := p.Reduce(t.Context(), []reducers.Event{event("Changed", 3, Changed{0})}, nil, reducers.WithBatchContext(paged))
	var deferral *observation.DeferredError
	if !errors.As(deferred.Err, &deferral) || deferral.RetryAfter != time.Minute || deferred.State != nil {
		t.Fatalf("deferred = %+v", deferred)
	}
	local := p.Reduce(t.Context(), []reducers.Event{event("Changed", 3, Changed{0})}, nil)
	if local.Err != nil {
		t.Fatalf("a local fold cannot defer, so the reducer saw CanDefer false: %v", local.Err)
	}
}

func TestBatchReducerFingerprintDiffersFromPerEventFolds(t *testing.T) {
	calls := 0
	batch := batchPlan(t, &calls)
	c, m, models := catalog(t)
	d, err := reducers.Define[*Ledger](m, nil)
	if err != nil {
		t.Fatal(err)
	}
	perEvent, err := reducers.Compile(d, c, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Fingerprint() == perEvent.Fingerprint() {
		t.Fatal("fingerprint ignores the batch fold")
	}
	if batchPlan(t, &calls, reducers.WithMaxConcurrency(3)).MaxConcurrency() != 3 || batch.MaxConcurrency() < 1 {
		t.Fatal("concurrency")
	}
	if _, err := reducers.Define[*Ledger](m, nil, reducers.WithMaxConcurrency(0)); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatal("zero concurrency accepted")
	}
}

func TestInvalidReducerBatchRegistrationsFail(t *testing.T) {
	c, m, models := catalog(t)
	for _, tc := range []struct {
		name string
		d    func() (reducers.Declaration, error)
		want error
	}{
		{"two batch methods", func() (reducers.Declaration, error) {
			return reducers.Define[TwoBatchFolds](m, nil, reducers.WithBatchMethod("A", observation.Observes[Changed]()), reducers.WithBatchMethod("B", observation.Observes[Removed]()))
		}, observation.ErrReducerHasMoreThanOneBatchMethod},
		{"batch and per-event folds", func() (reducers.Declaration, error) {
			return reducers.Define[MixedFolds](m, nil, reducers.WithBatchMethod("Batch", observation.Observes[Changed]()))
		}, observation.ErrReducerMixesBatchAndPerEventMethods},
		{"batch method observing nothing", func() (reducers.Declaration, error) { return reducers.Define[MixedFolds](m, nil) }, observation.ErrBatchMethodObservesNothing},
		{"observes on a per-event fold", func() (reducers.Declaration, error) {
			return reducers.Define[*Ledger](m, nil, reducers.WithBatchMethod("Change", observation.Observes[Changed]()))
		}, observation.ErrObservesOnlyOnBatchMethods},
		{"wrong current model", func() (reducers.Declaration, error) {
			return reducers.Define[WrongBatchCurrent](m, nil, reducers.WithBatchMethod("Batch", observation.Observes[Changed]()))
		}, observation.ErrInvalidBatchMethodSignature},
		{"per-event fold taking BatchContext", func() (reducers.Declaration, error) { return reducers.Define[FoldWithBatchContext](m, nil) }, observation.ErrBatchContextOnlyForBatchMethods},
		{"unknown event type", func() (reducers.Declaration, error) {
			return reducers.Define[*BatchLedger](m, nil, reducers.WithBatchMethod("Reduce", observation.Observes[Unknown]()))
		}, observation.ErrBatchMethodObservesUnknownEventType},
		{"batch callback and per-event callback", func() (reducers.Declaration, error) {
			return reducers.DefineHandlers(m, "mixed", []reducers.Handler{
				reducers.OnBatch(func(context.Context, *observation.EventBatch, *Total, observation.BatchContext) (*Total, error) {
					return nil, nil
				}, observation.Observes[Changed]()),
				reducers.On(func(context.Context, Removed, *Total, events.Context) (*Total, error) { return nil, nil }),
			})
		}, observation.ErrReducerMixesBatchAndPerEventMethods},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := tc.d()
			if err != nil {
				t.Fatal(err)
			}
			_, err = reducers.Compile(d, c, models, nil)
			if !errors.Is(err, tc.want) || !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestBatchReducerCallback(t *testing.T) {
	c, m, models := catalog(t)
	d, err := reducers.DefineHandlers(m, "callback", []reducers.Handler{reducers.OnBatch(func(_ context.Context, batch *observation.EventBatch, current *Total, _ observation.BatchContext) (*Total, error) {
		return &Total{Amount: batch.Len()}, nil
	}, observation.Observes[Changed]())})
	if err != nil {
		t.Fatal(err)
	}
	p, err := reducers.Compile(d, c, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := p.Reduce(t.Context(), []reducers.Event{event("Changed", 1, Changed{1}), event("Changed", 2, Changed{1})}, nil)
	if result.Err != nil || result.State.(*Total).Amount != 2 {
		t.Fatalf("result = %+v", result)
	}
}
