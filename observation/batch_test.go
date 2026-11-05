// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observation_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/observation"
)

type registered struct{ Name string }

func item(source events.SourceID, number events.SequenceNumber) observation.EventAndContext {
	return observation.EventAndContext{Event: registered{string(source)}, Context: events.Context{SourceID: source, SequenceNumber: number}}
}

func TestEventBatchReportsPartitionOutcomesFromTheRightEvent(t *testing.T) {
	batch := observation.NewEventBatch([]observation.EventAndContext{item("a", 1), item("b", 2), item("a", 3), item("c", 4)}, true)
	if got := batch.Partitions(); !slices.Equal(got, []events.SourceID{"a", "b", "c"}) {
		t.Fatalf("partitions = %v", got)
	}
	failure := errors.New("index rejected")
	if err := batch.Fail("a", failure); err != nil {
		t.Fatal(err)
	}
	if err := batch.FailEvent(events.Context{SourceID: "a", SequenceNumber: 3}, nil); err != nil {
		t.Fatal(err)
	}
	if err := batch.Defer("b", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := batch.DeferEvent(events.Context{SourceID: "c", SequenceNumber: 4}, -time.Second); err != nil {
		t.Fatal(err)
	}
	outcomes := batch.Outcomes()
	if len(outcomes) != 4 {
		t.Fatalf("outcomes = %+v", outcomes)
	}
	if o := outcomes[0]; o.Partition != "a" || o.At != 1 || o.Kind != observation.OutcomeFailed || o.Err != failure {
		t.Fatalf("partition failure = %+v", o)
	}
	if o := outcomes[1]; o.At != 3 || !errors.Is(o.Err, observation.ErrPartitionFailedInBatch) {
		t.Fatalf("event failure without an error = %+v", o)
	}
	if o := outcomes[2]; o.Partition != "b" || o.At != 2 || o.Kind != observation.OutcomeDeferred || o.RetryAfter != 2*time.Second || o.Err != nil {
		t.Fatalf("deferral = %+v", o)
	}
	if o := outcomes[3]; o.At != 4 || o.RetryAfter != 0 {
		t.Fatalf("negative delay = %+v", o)
	}
}

func TestEventBatchRefusesPartitionsItDoesNotHold(t *testing.T) {
	batch := observation.NewEventBatch([]observation.EventAndContext{item("a", 1)}, true)
	for name, err := range map[string]error{
		"fail":        batch.Fail("missing", errors.New("x")),
		"fail event":  batch.FailEvent(events.Context{SourceID: "a", SequenceNumber: 9}, nil),
		"defer":       batch.Defer("missing", time.Second),
		"defer event": batch.DeferEvent(events.Context{SourceID: "missing", SequenceNumber: 1}, time.Second),
	} {
		if !errors.Is(err, observation.ErrPartitionNotInBatch) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if len(batch.Outcomes()) != 0 {
		t.Fatal("refused outcome recorded")
	}
}

func TestDeferralOnAKernelWithoutPagesFailsThePartition(t *testing.T) {
	batch := observation.NewEventBatch([]observation.EventAndContext{item("a", 1)}, false)
	if err := batch.Defer("a", time.Second); err != nil {
		t.Fatal(err)
	}
	o := batch.Outcomes()[0]
	if o.Kind != observation.OutcomeFailed || o.RetryAfter != 0 || !errors.Is(o.Err, observation.ErrDeferralNotSupportedByKernel) {
		t.Fatalf("outcome = %+v", o)
	}
}

func TestEventBatchCopiesItsInputAndIteratesInOrder(t *testing.T) {
	input := []observation.EventAndContext{item("a", 1), item("b", 2)}
	batch := observation.NewEventBatch(input, true)
	input[0] = item("z", 9)
	var seen []events.SequenceNumber
	for i, value := range batch.All() {
		if batch.At(i).Context.SequenceNumber != value.Context.SequenceNumber {
			t.Fatal("At and All disagree")
		}
		seen = append(seen, value.Context.SequenceNumber)
	}
	if !slices.Equal(seen, []events.SequenceNumber{1, 2}) || batch.Len() != 2 || batch.Events()[0].Context.SourceID != "a" {
		t.Fatalf("seen = %v", seen)
	}
}

func TestBatchRegistrationErrorsAreConfigurationErrors(t *testing.T) {
	for _, err := range []error{
		observation.ErrBatchMethodObservesNothing, observation.ErrObservesOnlyOnBatchMethods,
		observation.ErrBatchMethodObservesUnknownEventType, observation.ErrEventTypeObservedByMoreThanOneMethod,
		observation.ErrBatchContextOnlyForBatchMethods, observation.ErrInvalidBatchMethodSignature,
		observation.ErrReducerHasMoreThanOneBatchMethod, observation.ErrReducerMixesBatchAndPerEventMethods,
	} {
		if !errors.Is(err, chronicle.ErrInvalidConfiguration) {
			t.Errorf("%v is not a configuration error", err)
		}
	}
	deferred := error(&observation.DeferredError{Partition: "a", RetryAfter: time.Second})
	if !errors.Is(deferred, observation.ErrPartitionDeferred) {
		t.Fatal("deferred error identity lost")
	}
}

func TestOriginNamesMatchCSharp(t *testing.T) {
	context := observation.BatchContext{Origin: observation.OriginReplay}
	if !context.IsReplay() || observation.OriginCatchUp.String() != "CatchUp" || observation.OriginUnknown.String() != "Unknown" {
		t.Fatal("origin")
	}
	if observation.Observes[registered]().Type().Name() != "registered" || (observation.Observed{}).Type() != nil {
		t.Fatal("observed type")
	}
}
