// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/transactions"
)

func TestReactorPreparesAllBuiltinsBeforeAnyCustomEffect(t *testing.T) {
	registry := reactorRegistry(t)
	var trace []string
	handler := &CommandEffects{name: "custom", trace: &trace}
	registerEffect(t, registry, []any{ReturnedCommand{42}, ReactorOutput{1}, ReactorOutput{2}}, reactors.WithSideEffectHandlers(handler))
	calls := 0
	kernel := &reactorKernel{}
	_, _, ctx := reactorClient(t, kernel, registry, chronicle.WithEventEnrichers(func(context.Context, events.TypeRef, *events.EventContent) error {
		calls++
		if calls == 2 {
			return errors.New("local preparation fails")
		}
		return nil
	}))
	session := receive(t, ctx, kernel.sessions)
	session.batches <- batch(0)
	result := receive(t, ctx, session.results)
	if result.State != contracts.ObservationState_Failed || calls != 2 || kernel.appendCalls.Load() != 0 {
		t.Fatal(result, calls, kernel.appendCalls.Load())
	}
	for _, item := range trace {
		if item == "handle-custom" {
			t.Fatal("custom effect ran before preparation finished")
		}
	}
}

func TestEnricherCanCloseClientWithoutHoldingPreparationLease(t *testing.T) {
	var client *chronicle.Client
	kernel := &fakeKernel{}
	var err error
	client, _ = testClient(t, kernel, chronicle.WithEventEnrichers(func(ctx context.Context, _ events.TypeRef, _ *events.EventContent) error {
		return client.CloseContext(ctx)
	}))
	ctx := testContext(t)
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.EventLog().Append(ctx, "A", CustomerRegistered{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	if !errors.Is(err, chronicle.ErrClosed) || kernel.appendCalls.Load() != 0 {
		t.Fatal(err, kernel.appendCalls.Load())
	}
}

type subjectEnriched struct {
	Subject string         `json:"subject" chronicle:"subject"`
	Value   string         `json:"value"`
	State   enrichmentEnum `json:"state"`
}

func TestEnrichmentFreezesSubjectAndRefusesOpaqueDependencies(t *testing.T) {
	for _, test := range []struct {
		name                         string
		resolver, explicit, readOnly bool
		field                        string
		wantFailure                  bool
	}{
		{name: "tagged", field: "subject", wantFailure: true},
		{name: "enum edit", field: "state"},
		{name: "opaque", resolver: true, field: "state", wantFailure: true},
		{name: "opaque explicit bypass", resolver: true, explicit: true, field: "state"},
		{name: "opaque read only", resolver: true, readOnly: true, field: "value"},
	} {
		t.Run(test.name, func(t *testing.T) {
			registry := chronicle.NewRegistry()
			resolverCalls := 0
			codecs, _ := enrichmentEnumCodecs(t)
			opts := []events.TypeOption{events.WithCodecs(codecs)}
			if test.resolver {
				opts = append(opts, events.WithSubjectResolver(func(value subjectEnriched) (events.Subject, bool) {
					resolverCalls++
					return events.Subject(value.Subject), true
				}))
			}
			if _, err := chronicle.RegisterEvent[subjectEnriched](registry, opts...); err != nil {
				t.Fatal(err)
			}
			kernel := &fakeKernel{append: func(_ context.Context, r *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
				wantSubject := "person"
				if test.explicit {
					wantSubject = "explicit"
				}
				wantContent := `{"subject":"person","value":"original","state":1}`
				if test.field == "state" && !test.readOnly {
					wantContent = `{"subject":"person","value":"original","state":2}`
				}
				if r.Subject != wantSubject || r.Content != wantContent {
					t.Error("subject or enum content changed", r.Subject, r.Content)
				}
				return success(r, 0), nil
			}}
			client, _ := testClient(t, kernel, chronicle.WithRegistry(registry), chronicle.WithEventEnrichers(func(_ context.Context, _ events.TypeRef, c *events.EventContent) error {
				if test.readOnly {
					_, _, err := c.Get(test.field)
					return err
				}
				if test.field == "state" {
					_ = c.Set(test.field, enrichmentEnum(2))
				} else {
					_ = c.Set(test.field, "changed")
				}
				return nil
			}))
			ctx := testContext(t)
			store, err := client.EventStore(ctx, "customers")
			if err != nil {
				t.Fatal(err)
			}
			options := []eventsequences.AppendOption{eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})}
			if test.explicit {
				options = append(options, eventsequences.WithSubject("explicit"))
			}
			_, err = store.EventLog().Append(ctx, "A", subjectEnriched{Subject: "person", Value: "original", State: 1}, options...)
			if (err != nil) != test.wantFailure {
				t.Fatal(err)
			}
			if test.explicit && resolverCalls != 0 {
				t.Fatal("explicit subject did not bypass resolver")
			}
			if test.resolver && !test.explicit && resolverCalls != 1 {
				t.Fatal("subject was not selected exactly once", resolverCalls)
			}
			wantAppends := int32(1)
			if test.wantFailure {
				wantAppends = 0
			}
			if kernel.appendCalls.Load() != wantAppends {
				t.Fatal("subject guard changed dispatch", kernel.appendCalls.Load())
			}
		})
	}
}

func TestHandledZeroStageCorrelationDoesNotInheritBoundCorrelation(t *testing.T) {
	calls := 0
	client, _ := testClient(t, &fakeKernel{}, chronicle.WithCorrelationProvider(func(context.Context) (metadata.CorrelationID, bool, error) {
		calls++
		return metadata.CorrelationID{}, calls > 1, nil
	}))
	ctx := testContext(t)
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	unit, _, err := transactions.Begin(ctx, store.EventLog())
	if err != nil {
		t.Fatal(err)
	}
	if err := unit.Stage(ctx, []eventsequences.Entry{{Source: "A", Event: CustomerRegistered{}}}); err == nil || len(unit.GetEvents()) != 0 {
		t.Fatal("handled zero inherited unit correlation", err)
	}
}
