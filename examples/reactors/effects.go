// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package main

import (
	"context"
	"reflect"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/reactors"
)

// OrderEffects demonstrates a targeted event and a replay replacement. Normal
// recovery can repeat Ship: the receiver must still be idempotent.
type OrderEffects struct{}

// Ship returns a self-describing wrapper; it does not inherit reactor routing.
func (*OrderEffects) Ship(event OrderPlaced) eventsequences.Entry {
	return eventsequences.Entry{Source: events.SourceID(event.Product), Event: OrderConfirmed(event), Route: eventsequences.Route{StreamType: "shipping"}}
}

// Rebuild deliberately performs no shipping side effect during replay.
func (*OrderEffects) Rebuild(OrderPlaced) {}

// ReserveStock is an application command, not a Chronicle event.
type ReserveStock struct{ Product string }

// CommandEffects is the seam an Arc adapter can use. The application supplies
// execution (including validation/authorization); failures fail the partition.
// The executor must be concurrency-safe and must not close or retain the scope.
type CommandEffects struct {
	Execute func(context.Context, reactors.SideEffectContext, ReserveStock) error
}

// CanHandleReturnType admits only ReserveStock results at client construction.
func (CommandEffects) CanHandleReturnType(t reflect.Type) bool {
	return t == reflect.TypeFor[ReserveStock]()
}

// CanHandle classifies without executing a command or resolving scoped services.
func (CommandEffects) CanHandle(_ reactors.SideEffectContext, value any) bool {
	_, ok := value.(ReserveStock)
	return ok
}

// Handle executes before acknowledgement with delivery identity and batch scope.
func (h CommandEffects) Handle(ctx context.Context, c reactors.SideEffectContext, value any) error {
	return h.Execute(ctx, c, value.(ReserveStock))
}

func registerOrderEffects(registry *chronicle.Registry, execute func(context.Context, reactors.SideEffectContext, ReserveStock) error) error {
	if _, err := chronicle.RegisterEvent[OrderPlaced](registry); err != nil {
		return err
	}
	if _, err := chronicle.RegisterEvent[OrderConfirmed](registry); err != nil {
		return err
	}
	if err := chronicle.RegisterReactor[*OrderEffects](registry, func() *OrderEffects { return &OrderEffects{} }, reactors.Replay("Rebuild"), reactors.OnceOnly("Ship")); err != nil {
		return err
	}
	if err := chronicle.RegisterReactorSideEffectHandler(registry, CommandEffects{Execute: execute}); err != nil {
		return err
	}
	return chronicle.RegisterReactorHandlers(registry, "reserve-stock", []reactors.Handler{
		reactors.Returning(func(_ context.Context, event OrderPlaced) ([]ReserveStock, error) {
			return []ReserveStock{{Product: event.Product}}, nil
		}).OnceOnly(),
	})
}
