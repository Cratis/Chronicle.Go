// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/transactions"
	grpcmetadata "google.golang.org/grpc/metadata"
)

type auditScopeStrategy struct {
	t           *testing.T
	correlation metadata.CorrelationID
	calls       int
}

func (s *auditScopeStrategy) GetScope(ctx context.Context, _ *eventsequences.Sequence, filter eventsequences.ScopeFilter) (eventsequences.Scope, error) {
	s.calls++
	if metadata.Correlation(ctx) != s.correlation || metadata.Identity(ctx).Subject != "selected" || len(metadata.CausationChain(ctx)) != 1 {
		s.t.Error("strategy did not receive frozen selected audit")
	}
	return eventsequences.Scope{Filter: filter}, nil
}

func TestSelectedAuditPrecedesEveryConcurrencyRead(t *testing.T) {
	for _, route := range []string{"single", "many", "batch", "prepared", "unit"} {
		t.Run(route, func(t *testing.T) {
			selected, err := metadata.NewCorrelationID()
			if err != nil {
				t.Fatal(err)
			}
			parent, err := metadata.NewCorrelationID()
			if err != nil {
				t.Fatal(err)
			}
			later, err := metadata.NewCorrelationID()
			if err != nil {
				t.Fatal(err)
			}
			strategy := &auditScopeStrategy{t: t, correlation: selected}
			tails, writes, providers, enrichers := 0, 0, 0, 0
			checkHeader := func(ctx context.Context) {
				md, _ := grpcmetadata.FromIncomingContext(ctx)
				if got := md.Get("x-correlation-id"); len(got) != 1 || got[0] != selected.String() {
					t.Errorf("header=%v, want %s", got, selected)
				}
			}
			kernel := &fakeKernel{tail: func(ctx context.Context, _ *sequences.TailSequenceNumberRequest) (*sequences.QueryResult_EventSequenceTailResponse, error) {
				tails++
				checkHeader(ctx)
				return &sequences.QueryResult_EventSequenceTailResponse{IsAuthorized: true, Data: &sequences.EventSequenceTailResponse{SequenceNumber: uint64(events.Unavailable)}}, nil
			}, append: func(ctx context.Context, r *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
				writes++
				checkHeader(ctx)
				if wire.Correlation(r.CorrelationId) != selected {
					t.Error("single body correlation changed")
				}
				return success(r, 0), nil
			}, appendMany: func(r *sequences.AppendManyRequest) *sequences.CommandResult_AppendManyResponse {
				writes++
				if wire.Correlation(r.CorrelationId) != selected {
					t.Error("many body correlation changed")
				}
				return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{IsSuccess: true, SequenceNumbers: []uint64{0}}}
			}, appendBatch: func(r *sequences.AppendManyForEventSourcesRequest) *sequences.CommandResult_AppendManyResponse {
				writes++
				if wire.Correlation(r.CorrelationId) != selected || r.CausedBy.Subject != "selected" {
					t.Error("batch body audit changed")
				}
				positions := make([]uint64, len(r.Events))
				return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{IsSuccess: true, SequenceNumbers: positions}}
			}}
			client, _ := testClient(t, kernel, chronicle.WithDefaultConcurrencyStrategy(strategy), chronicle.WithIdentityProvider(func(context.Context) (identities.Identity, bool, error) {
				return identities.Identity{Subject: "selected"}, true, nil
			}), chronicle.WithCorrelationProvider(func(context.Context) (metadata.CorrelationID, bool, error) { providers++; return selected, true, nil }), chronicle.WithCausationProvider(func(context.Context) ([]metadata.Causation, bool, error) {
				return []metadata.Causation{{Type: "selected"}}, true, nil
			}), chronicle.WithEventEnrichers(func(ctx context.Context, _ events.TypeRef, _ *events.EventContent) error {
				enrichers++
				if metadata.Correlation(ctx) != selected {
					t.Error("enricher context changed")
				}
				return nil
			}))
			ctx := metadata.WithCorrelation(testContext(t), parent)
			store, err := client.EventStore(ctx, "customers")
			if err != nil {
				t.Fatal(err)
			}
			log := store.EventLog()
			entries := []eventsequences.Entry{{Source: "A", Event: CustomerRegistered{}}}
			switch route {
			case "single":
				_, err = log.Append(ctx, "A", entries[0].Event)
			case "many":
				_, err = log.AppendMany(ctx, "A", []any{entries[0].Event})
			case "batch":
				_, err = log.AppendBatch(ctx, entries)
			case "prepared":
				var prepared *eventsequences.PreparedBatch
				prepared, err = log.PrepareBatch(ctx, entries)
				if err == nil {
					_, err = log.AppendPreparedBatch(metadata.WithCorrelation(ctx, later), prepared)
				}
				if err == nil {
					_, err = log.AppendPreparedBatch(metadata.WithCorrelation(ctx, parent), prepared)
				}
			case "unit":
				var unit *transactions.UnitOfWork
				var owner *transactions.Owner
				unit, owner, err = transactions.Begin(ctx, log)
				if err == nil {
					err = unit.Stage(ctx, entries)
				}
				if err == nil {
					_, err = owner.Commit(metadata.WithCorrelation(ctx, later))
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			want := 1
			if route == "prepared" {
				want = 2
			}
			wantProviders := 1
			if route == "unit" {
				wantProviders = 2
			}
			if tails != want || writes != want || strategy.calls != want || enrichers != 1 || providers != wantProviders {
				t.Fatalf("tails=%d writes=%d strategy=%d enrichers=%d providers=%d", tails, writes, strategy.calls, enrichers, providers)
			}
		})
	}
}

func TestReactorRejectsEmptyUnprotectedBuiltinBeforeCustomEffect(t *testing.T) {
	registry := reactorRegistry(t)
	var trace []string
	registerEffect(t, registry, []any{ReturnedCommand{42}, eventsequences.EventsWithConcurrencyScopes{}}, reactors.WithSideEffectHandlers(&CommandEffects{name: "custom", trace: &trace}))
	kernel := &reactorKernel{}
	preparations, notifications := 0, 0
	_, store, ctx := reactorClient(t, kernel, registry, chronicle.WithIdentityProvider(func(context.Context) (identities.Identity, bool, error) {
		preparations++
		return identities.Identity{}, false, nil
	}))
	unsubscribe := store.EventLog().OnAppend(func(eventsequences.AppendNotification) { notifications++ })
	defer unsubscribe()
	session := receive(t, ctx, kernel.sessions)
	session.batches <- batch(0)
	result := receive(t, ctx, session.results)
	if result.State != contracts.ObservationState_Failed || kernel.appendCalls.Load() != 0 || preparations != 0 || notifications != 0 {
		t.Fatal(result, kernel.appendCalls.Load(), preparations, notifications)
	}
	for _, item := range trace {
		if item == "handle-custom" {
			t.Fatal("custom I/O preceded impossible empty builtin rejection")
		}
	}
}
