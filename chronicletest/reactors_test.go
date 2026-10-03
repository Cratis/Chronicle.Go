// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/chronicletest"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/reactors"
)

type mailCommand struct{ Text string }
type commandHandler struct{ calls int }

func (*commandHandler) CanHandleReturnType(typ reflect.Type) bool {
	return typ == reflect.TypeFor[mailCommand]()
}
func (*commandHandler) CanHandle(reactors.SideEffectContext, any) bool { return true }
func (h *commandHandler) Handle(context.Context, reactors.SideEffectContext, any) error {
	h.calls++
	return nil
}

type scenarioReactor struct {
	deliveries *[]reactors.Delivery
	closes     *int
}

func (r *scenarioReactor) Opened(_ context.Context, event AccountOpened, account *Account, delivery reactors.Delivery) ([]any, error) {
	*r.deliveries = append(*r.deliveries, delivery)
	if event.Name == "fail" {
		return nil, errFold
	}
	if event.Name == "panic" {
		panic("handler panic")
	}
	return []any{eventsequences.Entry{Source: "other", Event: WelcomeRequested{Message: event.Name}}, []any{mailCommand{Text: account.Name}}, eventsequences.EventsWithConcurrencyScopes{Events: []eventsequences.Entry{{Source: "third", Event: WelcomeRequested{Message: "batch"}}}}}, nil
}
func (r *scenarioReactor) Close() error { *r.closes++; return nil }

func TestReactorScenarioUsesProductionInvokerSeedsAndMonotonicDeliveries(t *testing.T) {
	registry := eventRegistry(t)
	if _, err := chronicle.RegisterEvent[WelcomeRequested](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterReadModel[Account](registry); err != nil {
		t.Fatal(err)
	}
	var deliveries []reactors.Delivery
	activations, closes := 0, 0
	if err := chronicle.RegisterReactor[*scenarioReactor](registry, func() *scenarioReactor {
		activations++
		return &scenarioReactor{deliveries: &deliveries, closes: &closes}
	}); err != nil {
		t.Fatal(err)
	}
	extension := &commandHandler{}
	if err := chronicle.RegisterReactorSideEffectHandler(registry, extension); err != nil {
		t.Fatal(err)
	}
	s := chronicletest.NewReactorScenario[*scenarioReactor](t, chronicletest.Config{Registry: registry})
	seed := &Account{Name: "seeded"}
	if err := s.SeedReadModel("one", seed); err != nil {
		t.Fatal(err)
	}
	seed.Name = "caller mutation"
	if err := s.Given(t.Context(), "one", AccountOpened{Name: "Ada"}, AccountOpened{Name: "Grace"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Given(t.Context(), "one", AccountOpened{Name: "Lin"}); err != nil {
		t.Fatal(err)
	}
	if activations != 2 || closes != 2 {
		t.Fatalf("scope ownership: %d activations, %d closes", activations, closes)
	}
	if len(deliveries) != 3 {
		t.Fatal(deliveries)
	}
	for i, delivery := range deliveries {
		if delivery.SequenceNumber != events.SequenceNumber(i) || delivery.Partition != "one" {
			t.Fatalf("delivery %d: %+v", i, delivery)
		}
	}
	if deliveries[0].ID() == deliveries[2].ID() {
		t.Fatal("delivery identity reused")
	}
	if len(s.Produced()) != 9 {
		t.Fatalf("flattening: %+v", s.Produced())
	}
	chronicletest.ShouldHaveProduced[mailCommand](t, s.Produced(), func(command mailCommand) bool { return command.Text == "seeded" })
	chronicletest.ShouldHaveProduced[WelcomeRequested](t, s.Produced(), func(event WelcomeRequested) bool { return event.Message == "batch" })
	chronicletest.ShouldNotHaveProduced[AccountClosed](t, s.Produced())
	if extension.calls != 0 {
		t.Fatal("recording executed a real side effect")
	}
	if !errors.Is(s.Fidelity().Require(chronicletest.EffectAcceptance), chronicletest.ErrFidelityUnavailable) {
		t.Fatal("claimed acceptance")
	}
	if err := s.Given(t.Context(), "one", AccountOpened{Name: "fail"}); !errors.Is(err, errFold) {
		t.Fatalf("handler error lost: %v", err)
	}
	if err := s.Given(t.Context(), "one", AccountOpened{Name: "panic"}); err == nil {
		t.Fatal("handler panic lost")
	}
	if closes != 4 {
		t.Fatalf("failure did not close lease: %d", closes)
	}
	if err := s.Given(t.Context(), "one", AccountOpened{Name: "last"}); err != nil {
		t.Fatal(err)
	}
	if deliveries[len(deliveries)-1].SequenceNumber != 5 {
		t.Fatal("failure reused delivery identity")
	}
}

func TestRecorderBoundsCyclicCollectionsWithoutPartialRecordings(t *testing.T) {
	var recorder chronicletest.RecordingReactorSideEffectHandlers
	cycle := make([]any, 1)
	cycle[0] = cycle
	err := recorder.RecordEffect(t.Context(), reactors.SideEffectContext{}, []any{WelcomeRequested{}, cycle})
	if !errors.Is(err, chronicle.ErrInvalidConfiguration) || len(recorder.Produced()) != 0 {
		t.Fatalf("cyclic result: %v %+v", err, recorder.Produced())
	}
}

func TestReactorScenarioStillRejectsUnclaimedSynchronousReturnTypes(t *testing.T) {
	registry := eventRegistry(t)
	if err := chronicle.RegisterReactor[*unclaimedReactor](registry, func() *unclaimedReactor { return &unclaimedReactor{} }); err != nil {
		t.Fatal(err)
	}
	_, err := chronicletest.OpenReactorScenario[*unclaimedReactor](t.Context(), chronicletest.Config{Registry: registry})
	if err == nil {
		t.Fatal("recorder bypassed production declaration validation")
	}
}

type unclaimedReactor struct{}

func (*unclaimedReactor) Handle(AccountOpened) mailCommand { return mailCommand{} }

func TestReactorScenarioExplicitCallbacksShareRecordingAndCancellation(t *testing.T) {
	registry := eventRegistry(t)
	if _, err := chronicle.RegisterEvent[WelcomeRequested](registry); err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterReactorHandlers(registry, "callback", []reactors.Handler{
		reactors.Returning(func(_ context.Context, event AccountOpened) (WelcomeRequested, error) {
			return WelcomeRequested{Message: event.Name}, nil
		}),
	}); err != nil {
		t.Fatal(err)
	}
	s, err := chronicletest.OpenReactorScenarioForID(t.Context(), chronicletest.Config{Registry: registry}, "callback")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	if err = s.Given(t.Context(), "one", AccountOpened{Name: "Ada"}); err != nil {
		t.Fatal(err)
	}
	chronicletest.ShouldHaveProduced[WelcomeRequested](t, s.Produced(), func(event WelcomeRequested) bool { return event.Message == "Ada" })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err = s.Given(ctx, "one", AccountOpened{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if len(s.Produced()) != 1 {
		t.Fatal("canceled invocation produced an effect")
	}
}

func TestReactorScenarioPerEventClosesEveryActivation(t *testing.T) {
	registry := eventRegistry(t)
	if _, err := chronicle.RegisterEvent[WelcomeRequested](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterReadModel[Account](registry); err != nil {
		t.Fatal(err)
	}
	var deliveries []reactors.Delivery
	closes, activations := 0, 0
	if err := chronicle.RegisterReactor[*scenarioReactor](registry, func() *scenarioReactor {
		activations++
		return &scenarioReactor{deliveries: &deliveries, closes: &closes}
	}, reactors.PerEvent()); err != nil {
		t.Fatal(err)
	}
	s := chronicletest.NewReactorScenario[*scenarioReactor](t, chronicletest.Config{Registry: registry})
	if err := s.SeedReadModel("one", Account{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Given(t.Context(), "one", AccountOpened{}, AccountOpened{}); err != nil {
		t.Fatal(err)
	}
	if activations != 2 || closes != 2 {
		t.Fatalf("per-event ownership: %d/%d", activations, closes)
	}
}
