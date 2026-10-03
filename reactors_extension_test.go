// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/observation/reactors"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/reactors"
)

type OrderedCommands struct {
	trace    *[]string
	contexts chan reactors.SideEffectContext
	failAt   int
}

func (*OrderedCommands) CanHandleReturnType(t reflect.Type) bool {
	return t == reflect.TypeFor[ReturnedCommand]()
}
func (h *OrderedCommands) CanHandle(_ reactors.SideEffectContext, value any) bool {
	command, ok := value.(ReturnedCommand)
	if ok && h.trace != nil {
		*h.trace = append(*h.trace, fmt.Sprintf("classify-%d", command.Value))
	}
	return ok
}
func (h *OrderedCommands) Handle(_ context.Context, c reactors.SideEffectContext, value any) error {
	command := value.(ReturnedCommand)
	if h.trace != nil {
		*h.trace = append(*h.trace, fmt.Sprintf("execute-%d", command.Value))
	}
	if h.contexts != nil {
		h.contexts <- c
	}
	if command.Value == h.failAt {
		return errors.New("command rejected")
	}
	return nil
}

type UnserializableEffectEvent struct {
	Value float64 `json:"value"`
}

func TestCustomEffectCollectionPreflightAndDeterministicExecution(t *testing.T) {
	for _, tc := range []struct {
		name    string
		values  []any
		failAt  int
		want    []string
		success bool
	}{
		{"ordered", []any{ReturnedCommand{1}, ReturnedCommand{2}}, 0, []string{"classify-1", "classify-2", "execute-1", "execute-2"}, true},
		{"unknown later element", []any{ReturnedCommand{1}, "not claimed"}, 0, []string{"classify-1"}, false},
		{"unregistered entry", []any{ReturnedCommand{1}, eventsequences.Entry{Source: "x", Event: struct{ Unregistered string }{}}}, 0, []string{"classify-1"}, false},
		{"nil entry event", []any{ReturnedCommand{1}, eventsequences.Entry{Source: "x"}}, 0, []string{"classify-1"}, false},
		{"typed nil entry event", []any{ReturnedCommand{1}, eventsequences.Entry{Source: "x", Event: (*ReactorOutput)(nil)}}, 0, []string{"classify-1"}, false},
		{"empty entry source", []any{ReturnedCommand{1}, eventsequences.Entry{Event: ReactorOutput{1}}}, 0, []string{"classify-1"}, false},
		{"blank entry source", []any{ReturnedCommand{1}, eventsequences.Entry{Source: " ", Event: ReactorOutput{1}}}, 0, []string{"classify-1"}, false},
		{"invalid concurrency batch entry", []any{ReturnedCommand{1}, eventsequences.EventsWithConcurrencyScopes{Events: []eventsequences.Entry{{Source: "x", Event: ReactorOutput{1}}, {Source: "x", Event: struct{ Unregistered string }{}}}}}, 0, []string{"classify-1"}, false},
		{"unserializable bare event", []any{ReturnedCommand{1}, UnserializableEffectEvent{math.NaN()}}, 0, []string{"classify-1"}, false},
		{"unserializable entry", []any{ReturnedCommand{1}, eventsequences.Entry{Source: "x", Event: UnserializableEffectEvent{math.NaN()}}}, 0, []string{"classify-1"}, false},
		{"unserializable concurrency batch", []any{ReturnedCommand{1}, eventsequences.EventsWithConcurrencyScopes{Events: []eventsequences.Entry{{Source: "x", Event: UnserializableEffectEvent{math.NaN()}}}}}, 0, []string{"classify-1"}, false},
		{"stop after rejection", []any{ReturnedCommand{1}, ReturnedCommand{2}, ReturnedCommand{3}}, 2, []string{"classify-1", "classify-2", "classify-3", "execute-1", "execute-2"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := reactorRegistry(t)
			if _, err := chronicle.RegisterEvent[UnserializableEffectEvent](r); err != nil {
				t.Fatal(err)
			}
			var trace []string
			if err := chronicle.RegisterReactorSideEffectHandler(r, &OrderedCommands{trace: &trace, failAt: tc.failAt}); err != nil {
				t.Fatal(err)
			}
			registerEffect(t, r, tc.values)
			k := &reactorKernel{}
			_, _, ctx := reactorClient(t, k, r)
			s := receive(t, ctx, k.sessions)
			s.batches <- batch(0)
			result := receive(t, ctx, s.results)
			if (result.State == contracts.ObservationState_Success) != tc.success || !slices.Equal(trace, tc.want) {
				t.Fatal(result, trace)
			}
			if !tc.success && result.LastSuccessfulObservation != uint64(events.Unavailable) {
				t.Fatal("effect failure acknowledged", result)
			}
		})
	}
}

func TestCustomEffectsFrozenPerStoreAndStartupAdmission(t *testing.T) {
	defaultRegistry := reactorRegistry(t)
	if err := chronicle.RegisterReactorSideEffectHandler(defaultRegistry, &OrderedCommands{}); err != nil {
		t.Fatal(err)
	}
	registerEffect(t, defaultRegistry, ReturnedCommand{1})
	isolated := reactorRegistry(t)
	registerEffect(t, isolated, ReturnedCommand{1})
	client, err := chronicle.NewClient(chronicle.WithRegistry(defaultRegistry), chronicle.WithRegistryForStore("isolated", isolated))
	var declaration *reactors.DeclarationError
	if client != nil || !errors.As(err, &declaration) {
		t.Fatalf("store inherited handler from default: %v %v", client, err)
	}

	var trace []string
	local := &OrderedCommands{trace: &trace}
	if err := chronicle.RegisterReactorSideEffectHandler(isolated, local); err != nil {
		t.Fatal(err)
	}
	k := &reactorKernel{}
	_, _, ctx := reactorClient(t, k, defaultRegistry, chronicle.WithRegistryForStore("reactors", isolated))
	// This would fail the invocation if the client retained the mutable registry.
	if err := chronicle.RegisterReactorSideEffectHandler(isolated, &OrderedCommands{failAt: 1}); err != nil {
		t.Fatal(err)
	}
	s := receive(t, ctx, k.sessions)
	s.batches <- batch(0)
	if result := receive(t, ctx, s.results); result.State != contracts.ObservationState_Success || !slices.Equal(trace, []string{"classify-1", "execute-1"}) {
		t.Fatal(result, trace)
	}

	typed := reactorRegistry(t)
	if err := chronicle.RegisterReactorSideEffectHandler(typed, &OrderedCommands{}); err != nil {
		t.Fatal(err)
	}
	registerEffect(t, typed, []ReturnedCommand{{1}, {2}})
	client, err = chronicle.NewClient(chronicle.WithRegistry(typed))
	if err != nil {
		t.Fatal("typed command collection admission", err)
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCustomEffectsReceiveReplayPolicyAndBorrowedBatchScope(t *testing.T) {
	for _, once := range []bool{false, true} {
		t.Run(fmt.Sprint(once), func(t *testing.T) {
			r := reactorRegistry(t)
			contexts := make(chan reactors.SideEffectContext, 8)
			if err := chronicle.RegisterReactorSideEffectHandler(r, &OrderedCommands{contexts: contexts}); err != nil {
				t.Fatal(err)
			}
			handler := reactors.Returning(func(context.Context, ReactorInput) (ReturnedCommand, error) { return ReturnedCommand{1}, nil })
			if once {
				handler = handler.OnceOnly()
			}
			if err := chronicle.RegisterReactorHandlers(r, "policy", []reactors.Handler{handler}); err != nil {
				t.Fatal(err)
			}
			factory := &ReplayScopeFactory{}
			k := &reactorKernel{}
			_, _, ctx := reactorClient(t, k, r, chronicle.WithServices(factory))
			s := receive(t, ctx, k.sessions)
			s.batches <- batch(0, 1)
			if result := receive(t, ctx, s.results); result.State != contracts.ObservationState_Success {
				t.Fatal(result)
			}
			first, second := receive(t, ctx, contexts), receive(t, ctx, contexts)
			if first.Scope != second.Scope || first.Scope == nil || first.Replay || first.OnceOnly != once || !first.Replayable || first.Delivery.Reactor != "policy" || second.Delivery.SequenceNumber != 1 || factory.closed.Load() != 1 {
				t.Fatal(first, second)
			}
			b := batch(2)
			b.Events[0].Context.ObservationState = contracts.EventObservationState(events.ObservationReplay)
			s.batches <- b
			if result := receive(t, ctx, s.results); result.State != contracts.ObservationState_Success {
				t.Fatal(result)
			}
			if once {
				select {
				case c := <-contexts:
					t.Fatal("OnceOnly leaked replay effect", c)
				default:
				}
			} else {
				c := receive(t, ctx, contexts)
				if !c.Replay || c.OnceOnly || c.Scope == first.Scope {
					t.Fatal(c)
				}
			}
		})
	}
}
