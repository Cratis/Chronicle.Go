// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/outgoing"
	"github.com/cratis/chronicle.go/internal/reactoreffects"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
)

type returnedReviewConnection struct {
	enrichmentRouteConnection
	t                            *testing.T
	selected                     metadata.CorrelationID
	strategyCalls, tails, writes int
}

func (c *returnedReviewConnection) ConcurrencyPolicy() eventsequences.ConcurrencyPolicy {
	return eventsequences.ConcurrencyPolicy{Strategy: c, CheckFirstAppendIntoAScope: true}
}
func (c *returnedReviewConnection) GetScope(ctx context.Context, _ *eventsequences.Sequence, filter eventsequences.ScopeFilter) (eventsequences.Scope, error) {
	c.strategyCalls++
	if metadata.Correlation(ctx) != c.selected || metadata.Identity(ctx).Subject != "selected" {
		c.t.Error("returned strategy lost selected audit")
	}
	return eventsequences.Scope{Filter: filter}, nil
}
func (c *returnedReviewConnection) Invoke(ctx context.Context, method string, input, output any, options ...grpc.CallOption) error {
	if metadata.Correlation(ctx) != c.selected || metadata.Identity(ctx).Subject != "selected" {
		c.t.Error("returned read/write context lost selected audit")
	}
	if response, ok := output.(*sequences.QueryResult_EventSequenceTailResponse); ok {
		c.tails++
		response.IsAuthorized = true
		response.Data = &sequences.EventSequenceTailResponse{SequenceNumber: uint64(events.Unavailable)}
		return nil
	}
	c.writes++
	c.contents = nil
	if err := c.enrichmentRouteConnection.Invoke(ctx, method, input, output, options...); err != nil {
		return err
	}
	switch response := output.(type) {
	case *sequences.CommandResult_AppendResponse:
		response.Response.ConcurrencyCheckPerformed = true
	case *sequences.CommandResult_AppendManyResponse:
		response.Response.ConcurrencyCheckPerformed = true
	}
	return nil
}

func TestReturnedEffectsUseFrozenAuditForDeferredScopes(t *testing.T) {
	definition, err := events.Define[opened]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(definition.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"single", "many", "batch"} {
		t.Run(kind, func(t *testing.T) {
			selected, err := metadata.NewCorrelationID()
			if err != nil {
				t.Fatal(err)
			}
			parent, err := metadata.NewCorrelationID()
			if err != nil {
				t.Fatal(err)
			}
			providers, enrichers := 0, 0
			conn := &returnedReviewConnection{t: t, selected: selected}
			conn.config = outgoing.Config{Identity: func(context.Context) (identities.Identity, bool, error) {
				return identities.Identity{Subject: "selected"}, true, nil
			}, Correlation: func(context.Context) (metadata.CorrelationID, bool, error) { providers++; return selected, true, nil }, Enrichers: []events.EventEnricher{func(ctx context.Context, _ events.TypeRef, _ *events.EventContent) error {
				enrichers++
				if metadata.Correlation(ctx) != selected {
					t.Error("wrong enrichment context")
				}
				return nil
			}}}
			sequence, err := eventsequences.New("store", "namespace", events.EventLog, catalog, conn)
			if err != nil {
				t.Fatal(err)
			}
			effect := reactoreffects.Preparation[eventsequences.Entry, eventsequences.LabeledScope]{Entries: []eventsequences.Entry{{Source: "A", Event: opened{"original"}}}, Batch: kind == "batch", Bare: kind != "batch", Single: kind == "single"}
			dispatch, err := sequence.PrepareReturnedEvents(metadata.WithCorrelation(t.Context(), parent), effect)
			if err != nil {
				t.Fatal(err)
			}
			if conn.tails != 0 || conn.writes != 0 {
				t.Fatal("preparation performed I/O")
			}
			for range 2 {
				if err := dispatch(metadata.WithCorrelation(t.Context(), parent)); err != nil {
					t.Fatal(err)
				}
			}
			if providers != 1 || enrichers != 1 || conn.strategyCalls != 2 || conn.tails != 2 || conn.writes != 2 {
				t.Fatal(providers, enrichers, conn.strategyCalls, conn.tails, conn.writes)
			}
		})
	}
}

func TestReturnedEmptyCheckAndEmptyEnrollmentRemainDistinct(t *testing.T) {
	definition, err := events.Define[opened]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(definition.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	for _, expectation := range []eventsequences.Expectation{eventsequences.NoCheck(), eventsequences.Exact(0), eventsequences.NoMatchingEvent(), eventsequences.Resolve()} {
		selected, err := metadata.NewCorrelationID()
		if err != nil {
			t.Fatal(err)
		}
		calls := 0
		conn := &returnedReviewConnection{t: t, selected: selected}
		conn.config = outgoing.Config{Identity: func(context.Context) (identities.Identity, bool, error) {
			calls++
			return identities.Identity{Subject: "selected"}, true, nil
		}, Correlation: func(context.Context) (metadata.CorrelationID, bool, error) { return selected, true, nil }}
		sequence, err := eventsequences.New("store", "namespace", events.EventLog, catalog, conn)
		if err != nil {
			t.Fatal(err)
		}
		scopes := []eventsequences.LabeledScope{{Label: "A", Scope: eventsequences.Scope{Expectation: expectation}}}
		dispatch, err := sequence.PrepareReturnedEvents(t.Context(), reactoreffects.Preparation[eventsequences.Entry, eventsequences.LabeledScope]{Batch: true, Scopes: scopes})
		if expectation == eventsequences.NoCheck() {
			if err == nil || dispatch != nil || calls != 0 {
				t.Fatal("unchecked empty returned effect admitted", err, calls)
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if err := dispatch(t.Context()); err != nil {
				t.Fatal(err)
			}
			if conn.writes != 1 {
				t.Fatal("protected empty effect was not dispatched")
			}
		}
		if _, err := sequence.PrepareBatch(t.Context(), nil); err != nil {
			t.Fatal("empty enrollment rejected", err)
		}
	}
}
