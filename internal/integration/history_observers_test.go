//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/google/uuid"
)

type historyObservation struct {
	value string
	state events.ObservationState
}

type HistoryMutationObserver struct{ observed chan historyObservation }

func (r *HistoryMutationObserver) Renamed(ctx context.Context, e ProjectionAccountRenamed, ec events.Context) error {
	return r.send(ctx, e.Name, ec)
}

func (r *HistoryMutationObserver) Redacted(ctx context.Context, e events.EventRedacted, ec events.Context) error {
	return r.send(ctx, string(e.OriginalEventType), ec)
}

func (r *HistoryMutationObserver) send(ctx context.Context, value string, ec events.Context) error {
	select {
	case r.observed <- historyObservation{value, ec.ObservationState}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestKernelHistoryMutationsReplayProjectionsAndReactors(t *testing.T) {
	f := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[ProjectionAccountOpened](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[ProjectionAccountRenamed](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[events.EventRedacted](registry); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[ProjectionAccount](registry)
	if err != nil {
		t.Fatal(err)
	}
	observed := make(chan historyObservation, 16)
	if err = chronicle.RegisterReactor[*HistoryMutationObserver](registry, func() *HistoryMutationObserver { return &HistoryMutationObserver{observed} }, reactors.WithID("history-observer")); err != nil {
		t.Fatal(err)
	}
	client := f.client(registry)
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	// The reactor stream is open, but the kernel subscribes it asynchronously.
	// Appending first would start a reactor catch-up for this source that drops
	// the projection's later live events (https://github.com/Cratis/Chronicle/issues/4558).
	awaitObserversObserving(t, f, store.Namespace(), append([]string{"history-observer", string(model.Identifier())}, eventLogStatisticsObservers...)...)
	sequence := store.EventLog()
	source := events.SourceID(uuid.NewString())
	appendSuccessfully(t, f.ctx, store, source, ProjectionAccountOpened{FullName: "original"})
	renamed, err := sequence.Append(f.ctx, source, ProjectionAccountRenamed{Name: "before"})
	if err != nil || renamed.Err() != nil {
		t.Fatalf("%+v %v", renamed, err)
	}
	reader := readmodels.For(store.ReadModels(), model)
	awaitInitialHistoryProjection(t, f, store, reader, source, string(model.Identifier()), observed)
	awaitHistoryObservation(t, f.ctx, observed, "before", events.ObservationInitial)
	if err = sequence.Revise(f.ctx, *renamed.Position, ProjectionAccountRenamed{Name: "revised"}); err != nil {
		t.Fatal(err)
	}
	awaitProjection(t, f.ctx, reader, readmodels.Key(source), func(m ProjectionAccount) bool { return m.Name == "revised" })
	awaitHistoryObservation(t, f.ctx, observed, "revised", events.ObservationReplay)
	if err = sequence.Redact(f.ctx, *renamed.Position, "remove rename"); err != nil {
		t.Fatal(err)
	}
	awaitProjection(t, f.ctx, reader, readmodels.Key(source), func(m ProjectionAccount) bool { return m.Name == "original" })
	awaitHistoryObservation(t, f.ctx, observed, "ProjectionAccountRenamed", events.ObservationReplay)
}

func awaitHistoryObservation(t *testing.T, parent context.Context, observed <-chan historyObservation, value string, state events.ObservationState) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	for {
		select {
		case got := <-observed:
			if got.value == value {
				if got.state != state {
					t.Fatalf("observation=%+v, expected state %v", got, state)
				}
				return
			}
		case <-ctx.Done():
			t.Fatalf("missing history observation %q: %v", value, ctx.Err())
		}
	}
}
