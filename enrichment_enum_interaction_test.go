// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/cratis/chronicle.go/transactions"
	grpcmetadata "google.golang.org/grpc/metadata"
)

type enrichmentEnum int32

var enrichmentEnumHooks atomic.Uint64

func (enrichmentEnum) MarshalJSON() ([]byte, error) { enrichmentEnumHooks.Add(1); panic("enum JSON") }
func (enrichmentEnum) MarshalText() ([]byte, error) { enrichmentEnumHooks.Add(1); panic("enum text") }
func (enrichmentEnum) String() string               { enrichmentEnumHooks.Add(1); panic("enum string") }
func (enrichmentEnum) IsZero() bool                 { enrichmentEnumHooks.Add(1); panic("enum zero") }

func enrichmentEnumCodecs(t *testing.T) (*serialization.Codecs, []serialization.EnumMember[enrichmentEnum]) {
	t.Helper()
	before := enrichmentEnumHooks.Load()
	t.Cleanup(func() {
		if enrichmentEnumHooks.Load() != before {
			t.Error("enum invoked an application hook")
		}
	})
	members := []serialization.EnumMember[enrichmentEnum]{{Name: "One", Value: 1}, {Name: "Two", Value: 2}}
	codecs, err := serialization.NewCodecs(serialization.Enum(members...))
	if err != nil {
		t.Fatal(err)
	}
	return codecs, members
}

type enumPrivateConcept struct {
	original reviewConcept
	calls    *int
}

func (enumPrivateConcept) ConceptValue() string { return "" }
func (v enumPrivateConcept) MarshalJSON() ([]byte, error) {
	*v.calls++
	return v.original.MarshalJSON()
}
func (*enumPrivateConcept) UnmarshalJSON([]byte) error  { panic("unexpected decode") }
func (enumPrivateConcept) MarshalText() ([]byte, error) { panic("unexpected text") }
func (*enumPrivateConcept) UnmarshalText([]byte) error  { panic("unexpected decode") }

type enumPrivateEvent struct {
	State enrichmentEnum
	Value enumPrivateConcept
}

func TestEnumOutgoingPrivateBoundaryAndEarlyUnknownRefusal(t *testing.T) {
	for _, mode := range []string{"zero enrichers", "audit only", "unknown before enricher"} {
		t.Run(mode, func(t *testing.T) {
			codecs, _ := enrichmentEnumCodecs(t)
			r := chronicle.NewRegistry()
			definition, err := chronicle.RegisterEvent[enumPrivateEvent](r, events.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			providers, hooks, codecCalls := 0, 0, 0
			options := []chronicle.ClientOption{chronicle.WithRegistry(r)}
			if mode == "audit only" {
				options = append(options, chronicle.WithIdentityProvider(func(context.Context) (identities.Identity, bool, error) {
					providers++
					return identities.Identity{Subject: "actor"}, true, nil
				}))
			}
			if mode == "unknown before enricher" {
				options = append(options, chronicle.WithEventEnrichers(func(context.Context, events.TypeRef, *events.EventContent) error { providers++; return nil }))
			}
			kernel := &fakeKernel{}
			client, _ := testClient(t, kernel, options...)
			ctx := testContext(t)
			store, err := client.EventStore(ctx, "customers")
			if err != nil {
				t.Fatal(err)
			}
			application := reviewCodecError{&hooks}
			state := enrichmentEnum(1)
			if mode == "unknown before enricher" {
				state = 99
			}
			_, err = store.EventLog().Append(ctx, "A", enumPrivateEvent{state, enumPrivateConcept{reviewConcept{application}, &codecCalls}})
			var failure *events.PreparationError
			if !errors.As(err, &failure) || !errors.Is(err, chronicle.ErrUnsupported) || failure.Phase != "content" || failure.ProviderIndex != -1 || failure.EventIndex != 0 {
				t.Fatalf("failure=%#v", err)
			}
			_ = fmt.Sprint(err)
			if errors.Is(err, application) || hooks != 0 || kernel.appendCalls.Load() != 0 {
				t.Fatal("private failure escaped or dispatched", hooks, kernel.appendCalls.Load())
			}
			wantProviders := 0
			if mode == "audit only" {
				wantProviders = 1
			}
			if providers != wantProviders {
				t.Fatal("provider ordering changed", providers)
			}
			wantCodecs := 1
			if mode == "unknown before enricher" {
				wantCodecs = 0
			}
			if codecCalls != wantCodecs {
				t.Fatal("base enum failure reached a later concept codec", codecCalls)
			}
			ordinary := errors.New("ordinary")
			_, err = definition.Descriptor().Marshal(enumPrivateEvent{1, enumPrivateConcept{reviewConcept{ordinary}, &codecCalls}})
			var callback *serialization.CallbackError
			if !errors.Is(err, ordinary) || !errors.As(err, &callback) {
				t.Fatal("standalone callback identity lost")
			}
		})
	}
}

type stagedEnumConcept struct{ calls *int }

func (stagedEnumConcept) ConceptValue() string           { return "" }
func (v stagedEnumConcept) MarshalJSON() ([]byte, error) { *v.calls++; return []byte(`"owned"`), nil }
func (*stagedEnumConcept) UnmarshalJSON([]byte) error    { panic("unexpected decode") }
func (stagedEnumConcept) MarshalText() ([]byte, error)   { panic("unexpected text") }
func (*stagedEnumConcept) UnmarshalText([]byte) error    { panic("unexpected decode") }

type stagedEnumEvent struct {
	State    enrichmentEnum
	Optional *enrichmentEnum
	Many     []enrichmentEnum
	Value    stagedEnumConcept
}

func TestEnumPreparedAndUnitSnapshotsEncodeOnceWithSelectedAudit(t *testing.T) {
	for _, route := range []string{"prepared", "unit"} {
		t.Run(route, func(t *testing.T) {
			codecs, members := enrichmentEnumCodecs(t)
			r := chronicle.NewRegistry()
			event, err := chronicle.RegisterEvent[stagedEnumEvent](r, events.WithCodecs(codecs))
			if err != nil {
				t.Fatal(err)
			}
			members[0].Name, members[0].Value = "Changed", 99
			fields := event.Descriptor().Fields()
			fields[0].Name, fields[0].Path = "changed", "changed"
			selected, err := metadata.NewCorrelationID()
			if err != nil {
				t.Fatal(err)
			}
			later, err := metadata.NewCorrelationID()
			if err != nil {
				t.Fatal(err)
			}
			strategy := &auditScopeStrategy{t: t, correlation: selected}
			codecCalls, providers, enrichers, tails, writes := 0, 0, 0, 0, 0
			const content = `{"state":2,"optional":1,"many":[1,2],"value":"owned"}`
			kernel := &fakeKernel{tail: func(ctx context.Context, _ *sequences.TailSequenceNumberRequest) (*sequences.QueryResult_EventSequenceTailResponse, error) {
				tails++
				md, _ := grpcmetadata.FromIncomingContext(ctx)
				if got := md.Get("x-correlation-id"); len(got) != 1 || got[0] != selected.String() {
					t.Error("tail correlation changed", got)
				}
				return &sequences.QueryResult_EventSequenceTailResponse{IsAuthorized: true, Data: &sequences.EventSequenceTailResponse{SequenceNumber: uint64(events.Unavailable)}}, nil
			}, appendBatch: func(r *sequences.AppendManyForEventSourcesRequest) *sequences.CommandResult_AppendManyResponse {
				writes++
				if len(r.Events) != 1 || r.Events[0].Content != content || wire.Correlation(r.CorrelationId) != selected || r.CausedBy.Subject != "selected" {
					t.Error("staged bytes/audit changed", r)
				}
				return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{IsSuccess: true, SequenceNumbers: []uint64{0}}}
			}}
			client, _ := testClient(t, kernel, chronicle.WithRegistry(r), chronicle.WithNamingPolicy(serialization.CamelCase), chronicle.WithDefaultConcurrencyStrategy(strategy),
				chronicle.WithIdentityProvider(func(context.Context) (identities.Identity, bool, error) {
					return identities.Identity{Subject: "selected"}, true, nil
				}),
				chronicle.WithCorrelationProvider(func(context.Context) (metadata.CorrelationID, bool, error) { providers++; return selected, true, nil }),
				chronicle.WithCausationProvider(func(context.Context) ([]metadata.Causation, bool, error) {
					return []metadata.Causation{{Type: "selected"}}, true, nil
				}),
				chronicle.WithEventEnrichers(func(ctx context.Context, _ events.TypeRef, c *events.EventContent) error {
					enrichers++
					if metadata.Correlation(ctx) != selected {
						t.Error("enricher correlation changed")
					}
					return c.Set("state", enrichmentEnum(2))
				}))
			ctx := testContext(t)
			store, err := client.EventStore(ctx, "customers")
			if err != nil {
				t.Fatal(err)
			}
			optional, many := enrichmentEnum(1), []enrichmentEnum{1, 2}
			entries := []eventsequences.Entry{{Source: "A", Event: stagedEnumEvent{1, &optional, many, stagedEnumConcept{&codecCalls}}}}
			mutate := func() { optional, many[0] = 99, 99; entries[0].Event = stagedEnumEvent{} }
			if route == "prepared" {
				prepared, err := store.EventLog().PrepareBatch(ctx, entries)
				if err != nil {
					t.Fatal(err)
				}
				mutate()
				for range 2 {
					if _, err := store.EventLog().AppendPreparedBatch(metadata.WithCorrelation(ctx, later), prepared); err != nil {
						t.Fatal(err)
					}
				}
			} else {
				unit, owner, err := transactions.Begin(ctx, store.EventLog())
				if err != nil {
					t.Fatal(err)
				}
				if providers != 1 || codecCalls != 0 || enrichers != 0 {
					t.Fatal("Begin encoded content")
				}
				if err := unit.Stage(ctx, entries); err != nil {
					t.Fatal(err)
				}
				mutate()
				if _, err := owner.Commit(metadata.WithCorrelation(ctx, later)); err != nil {
					t.Fatal(err)
				}
			}
			wantWrites, wantProviders := 2, 1
			if route == "unit" {
				wantWrites, wantProviders = 1, 2
			}
			if codecCalls != 1 || enrichers != 1 || providers != wantProviders || writes != wantWrites || tails != wantWrites || strategy.calls != wantWrites {
				t.Fatalf("codec=%d enrichment=%d audit=%d writes=%d tails=%d scopes=%d", codecCalls, enrichers, providers, writes, tails, strategy.calls)
			}
		})
	}
}
