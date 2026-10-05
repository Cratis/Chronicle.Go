//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/transactions"
)

func TestKernelUnitOfWorkSnapshotsAndNestedOrder(t *testing.T) {
	ctx, sequence := batchFixture(t)
	// The kernel supplies request causation for empty chains. Explicit chains
	// let this test distinguish snapshot retention from that server default.
	ctx = metadata.WithCausation(ctx, metadata.Causation{Type: "outer-command", Occurred: time.Now()})
	unit, owner, err := transactions.Begin(ctx, sequence)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Rollback(); err != nil {
			t.Error(err)
		}
	})
	historyA, err := sequence.ReadHistory(ctx, "A", eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	value := &BatchOpened{Value: "A1"}
	if err := unit.Stage(ctx, []eventsequences.Entry{{Source: "A", Event: value, NamedTags: []events.NamedTag{{Name: "unit", Value: "snapshot"}}}}, eventsequences.LabeledScope{Label: "A", Scope: historyA.Scope()}); err != nil {
		t.Fatal(err)
	}
	value.Value = "changed after stage"
	nestedCtx := transactions.WithUnitOfWork(metadata.WithCausation(ctx, metadata.Causation{Type: "nested-command", Occurred: time.Now()}), unit)
	participant, found := transactions.FromContext(nestedCtx)
	if !found || participant != unit {
		t.Fatal("missing joined participant")
	}
	if err := participant.Stage(nestedCtx, []eventsequences.Entry{{Source: "B", Event: BatchChanged{Value: "B1"}}}, sourceScope("B", eventsequences.NoMatchingEvent())); err != nil {
		t.Fatal(err)
	}
	if err := unit.Stage(ctx, []eventsequences.Entry{{Source: "A", Event: BatchOpened{Value: "A2"}}}); err != nil {
		t.Fatal(err)
	}
	if has, err := sequence.HasEvents(ctx, "A"); err != nil || has {
		t.Fatal("staging persisted", has, err)
	}
	result, err := owner.Commit(ctx)
	if err != nil || result.Err() != nil || !unit.IsSuccess() || !reflect.DeepEqual(result.Positions, []events.SequenceNumber{0, 1, 2}) {
		t.Fatal(result, err)
	}
	loaded, err := sequence.ReadFrom(ctx, 0, eventsequences.FromFilter{})
	if err != nil || len(loaded) != 3 {
		t.Fatal(loaded, err)
	}
	for i, expected := range []string{"A1", "B1", "A2"} {
		var content BatchOpened
		if err := json.Unmarshal(loaded[i].Content, &content); err != nil {
			t.Fatal(err)
		}
		if content.Value != expected || loaded[i].Context.CorrelationID != unit.CorrelationID() {
			t.Fatal(loaded[i])
		}
	}
	if len(loaded[0].Context.NamedTags) != 1 || loaded[0].Context.NamedTags[0].Value != "snapshot" || len(loaded[0].Context.Causation) != 1 || loaded[0].Context.Causation[0].Type != "outer-command" || len(loaded[1].Context.Causation) != 2 || loaded[1].Context.Causation[1].Type != "nested-command" || len(loaded[2].Context.Causation) != 1 || loaded[2].Context.Causation[0].Type != "outer-command" {
		t.Fatal("staged metadata changed", loaded)
	}
	if err := participant.Stage(ctx, nil); !errors.Is(err, transactions.ErrCompleted) {
		t.Fatal(err)
	}
	if err := owner.Rollback(); err != nil {
		t.Fatal(err)
	}
	if result, err := owner.Commit(ctx); !errors.Is(err, transactions.ErrCompleted) || result.Disposition != eventsequences.Committed {
		t.Fatal(result, err)
	}
}

func TestKernelUnitOfWorkConstraintViolationRejectsAllSources(t *testing.T) {
	ctx, _, store := constraintsFixture(t, func(e constraintEvents) *constraints.Builder {
		return constraints.UniqueValues("UniqueEmail").On(e.claimed, "email").WithMessage("{PropertyName} conflicts: {PropertyValue}")
	})
	sequence := store.EventLog()
	unit, owner, err := transactions.Begin(ctx, sequence)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Rollback(); err != nil {
			t.Error(err)
		}
	})
	if err := unit.Stage(ctx, []eventsequences.Entry{
		{Source: "left", Event: AddressClaimed{Email: "shared@example.test"}},
		{Source: "right", Event: AddressClaimed{Email: "shared@example.test"}},
	}, sourceScope("left", eventsequences.NoCheck()), sourceScope("right", eventsequences.NoCheck())); err != nil {
		t.Fatal(err)
	}
	result, err := owner.Commit(ctx)
	var constraintError *eventsequences.ConstraintError
	if err != nil || result.Disposition != eventsequences.Rejected || unit.State() != transactions.Rejected || len(result.Positions) != 0 || !errors.As(result.Err(), &constraintError) || len(result.ConstraintViolations) == 0 {
		t.Fatalf("expected atomic constraint rejection: %+v, state %v, error %v", result, unit.State(), err)
	}
	for _, violation := range result.ConstraintViolations {
		if violation.ConstraintName != "UniqueEmail" || violation.Message != "email conflicts: shared@example.test" {
			t.Fatalf("lost constraint name or resolved custom message: %+v", violation)
		}
	}
	loaded, err := sequence.ReadFrom(ctx, 0, eventsequences.FromFilter{})
	if err != nil || len(loaded) != 0 {
		t.Fatalf("rejected unit persisted events: %+v, error %v", loaded, err)
	}
}

func TestKernelUnitOfWorkLoadedHistoryRejectsAllSources(t *testing.T) {
	ctx, sequence := batchFixture(t)
	history, err := sequence.ReadHistory(ctx, "A", eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	unit, owner, err := transactions.Begin(ctx, sequence)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Rollback(); err != nil {
			t.Error(err)
		}
	})
	if err := unit.Stage(ctx, []eventsequences.Entry{{Source: "A", Event: BatchOpened{Value: "stale"}}, {Source: "B", Event: BatchOpened{Value: "must-not-exist"}}}, eventsequences.LabeledScope{Label: "A", Scope: history.Scope()}, sourceScope("B", eventsequences.NoCheck())); err != nil {
		t.Fatal(err)
	}
	advanced, err := sequence.Append(ctx, "A", BatchOpened{Value: "competitor"}, eventsequences.WithScope(sourceScope("A", eventsequences.NoCheck()).Scope))
	if err != nil || advanced.Err() != nil {
		t.Fatal(advanced, err)
	}
	result, err := owner.Commit(ctx)
	var conflict *eventsequences.ConcurrencyError
	if err != nil || result.Disposition != eventsequences.Rejected || unit.State() != transactions.Rejected || !errors.As(result.Err(), &conflict) || len(conflict.Violations) != 1 {
		t.Fatal(result, err)
	}
	if has, err := sequence.HasEvents(ctx, "B"); err != nil || has {
		t.Fatal("atomic rejection failed", has, err)
	}
	loaded, err := sequence.ReadSource(ctx, "A", eventsequences.SourceFilter{})
	if err != nil || len(loaded) != 1 {
		t.Fatal(loaded, err)
	}
	if _, err := owner.Commit(ctx); !errors.Is(err, transactions.ErrCompleted) {
		t.Fatal(err)
	}
}
