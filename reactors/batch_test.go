// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/observation"
	"github.com/cratis/chronicle.go/reactors"
)

type Indexer struct{ seen *[]observation.BatchContext }

func (i *Indexer) Index(_ context.Context, batch *observation.EventBatch, bc observation.BatchContext) error {
	*i.seen = append(*i.seen, bc)
	for _, item := range batch.All() {
		if _, ok := item.Event.(Ordered); !ok {
			return errors.New("batch events are values of the observed type")
		}
	}
	return nil
}
func (i *Indexer) Mail(Other) {}

type BatchOnly struct{}

func (BatchOnly) Index(*observation.EventBatch) error { return nil }

type BatchAndPerEvent struct{}

func (BatchAndPerEvent) Index(*observation.EventBatch) error { return nil }
func (BatchAndPerEvent) Each(Ordered)                        {}

type TwoBatches struct{}

func (TwoBatches) First(*observation.EventBatch)  {}
func (TwoBatches) Second(*observation.EventBatch) {}

type PerEventWithBatchContext struct{}

func (PerEventWithBatchContext) Each(Ordered, observation.BatchContext) {}

type ReturnsEvents struct{}

func (ReturnsEvents) Index(*observation.EventBatch) Other { return Other{} }

type VariadicBatch struct{}

func (VariadicBatch) Index(*observation.EventBatch, ...observation.BatchContext) {}

type Unregistered struct{}

func TestBatchMethodsAreDiscoveredBesidePerEventMethods(t *testing.T) {
	var seen []observation.BatchContext
	d, err := reactors.Define[*Indexer](func() *Indexer { return &Indexer{&seen} }, reactors.WithBatchMethod("Index", observation.Observes[Ordered]()))
	if err != nil {
		t.Fatal(err)
	}
	p := compile(t, d)
	methods := p.BatchMethods()
	if len(methods) != 1 || methods[0].Name() != "Index" {
		t.Fatalf("batch methods = %v", methods)
	}
	if _, ok := p.BatchMethodFor("Ordered"); !ok {
		t.Fatal("Ordered does not reach the batch method")
	}
	if _, ok := p.BatchMethodFor("Other"); ok {
		t.Fatal("per-event type reaches the batch method")
	}
	if len(p.EventTypes()) != 2 {
		t.Fatalf("subscriptions = %v", p.EventTypes())
	}
	lease, err := p.Activate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lease.Close(t.Context()) }()
	batch := observation.NewEventBatch([]observation.EventAndContext{{Event: Ordered{1}, Context: events.Context{SourceID: "a", SequenceNumber: 1}}}, true)
	if err := lease.InvokeBatch(t.Context(), methods[0], batch, observation.BatchContext{Origin: observation.OriginLive}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0].Origin != observation.OriginLive {
		t.Fatalf("batch context = %+v", seen)
	}
}

func TestBatchCallbacksCompileWithoutAnArtifact(t *testing.T) {
	calls := 0
	d, err := reactors.DefineHandlers("callbacks", []reactors.Handler{reactors.OnBatch(func(context.Context, *observation.EventBatch, observation.BatchContext) error {
		calls++
		return nil
	}, observation.Observes[Ordered](), observation.Observes[Other]())})
	if err != nil {
		t.Fatal(err)
	}
	p := compile(t, d)
	if _, ok := p.BatchMethodFor("Other"); !ok || len(p.EventTypes()) != 2 {
		t.Fatal("callback does not observe both types")
	}
}

func TestInvalidBatchRegistrationsFail(t *testing.T) {
	observesOrdered := reactors.WithBatchMethod("Index", observation.Observes[Ordered]())
	for _, tc := range []struct {
		name string
		d    func() (reactors.Declaration, error)
		want error
	}{
		{"observes nothing", func() (reactors.Declaration, error) { return reactors.Define[BatchOnly](nil) }, observation.ErrBatchMethodObservesNothing},
		{"observes on a per-event method", func() (reactors.Declaration, error) {
			return reactors.Define[*Indexer](nil, observesOrdered, reactors.WithBatchMethod("Mail", observation.Observes[Other]()))
		}, observation.ErrObservesOnlyOnBatchMethods},
		{"observes on an unknown method", func() (reactors.Declaration, error) {
			return reactors.Define[BatchOnly](nil, observesOrdered, reactors.WithBatchMethod("Missing", observation.Observes[Other]()))
		}, observation.ErrObservesOnlyOnBatchMethods},
		{"unknown event type", func() (reactors.Declaration, error) {
			return reactors.Define[BatchOnly](nil, reactors.WithBatchMethod("Index", observation.Observes[Unregistered]()))
		}, observation.ErrBatchMethodObservesUnknownEventType},
		{"batch and per-event for one type", func() (reactors.Declaration, error) {
			return reactors.Define[BatchAndPerEvent](nil, observesOrdered)
		}, observation.ErrEventTypeObservedByMoreThanOneMethod},
		{"two batch methods for one type", func() (reactors.Declaration, error) {
			return reactors.Define[TwoBatches](nil, reactors.WithBatchMethod("First", observation.Observes[Ordered]()), reactors.WithBatchMethod("Second", observation.Observes[Ordered]()))
		}, observation.ErrEventTypeObservedByMoreThanOneMethod},
		{"batch callback and per-event callback for one type", func() (reactors.Declaration, error) {
			return reactors.DefineHandlers("mixed", []reactors.Handler{
				reactors.OnBatch(func(context.Context, *observation.EventBatch, observation.BatchContext) error { return nil }, observation.Observes[Ordered]()),
				reactors.On(func(context.Context, Ordered) error { return nil }),
			})
		}, observation.ErrEventTypeObservedByMoreThanOneMethod},
		{"batch callback observing nothing", func() (reactors.Declaration, error) {
			return reactors.DefineHandlers("nothing", []reactors.Handler{reactors.OnBatch(func(context.Context, *observation.EventBatch, observation.BatchContext) error { return nil })})
		}, observation.ErrBatchMethodObservesNothing},
		{"per-event method taking BatchContext", func() (reactors.Declaration, error) { return reactors.Define[PerEventWithBatchContext](nil) }, observation.ErrBatchContextOnlyForBatchMethods},
		{"batch method returning events", func() (reactors.Declaration, error) {
			return reactors.Define[ReturnsEvents](nil, observesOrdered)
		}, observation.ErrInvalidBatchMethodSignature},
		{"variadic batch method", func() (reactors.Declaration, error) { return reactors.Define[VariadicBatch](nil, observesOrdered) }, observation.ErrInvalidBatchMethodSignature},
		{"replay policy on a batch method", func() (reactors.Declaration, error) {
			return reactors.Define[BatchOnly](nil, observesOrdered, reactors.Replay("Index"))
		}, chronicle.ErrInvalidConfiguration},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := tc.d()
			if err != nil {
				t.Fatal(err)
			}
			c, m := catalogs(t)
			_, err = reactors.Compile(d, c, m, nil)
			if !errors.Is(err, tc.want) || !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			var declaration *reactors.DeclarationError
			if !errors.As(err, &declaration) {
				t.Fatalf("err = %T, want *reactors.DeclarationError", err)
			}
		})
	}
	if _, err := reactors.Define[BatchOnly](nil, reactors.WithBatchMethod("Index")); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("an option without observed types = %v", err)
	}
}
