// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/internal/discovery"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
)

// Delivery identifies a repeatable delivery, not an exactly-once guarantee.
// The partition is the event source ID, matching C# ReactorDelivery.For.
type Delivery struct {
	Reactor        ID
	Store          metadata.StoreName
	Namespace      metadata.Namespace
	Sequence       events.SequenceID
	Partition      events.SourceID
	SequenceNumber events.SequenceNumber
}

// ID combines the six components using C# KeyHelper's '#' delimiter. Components
// containing '#' retain C#'s ambiguity; use the structured components when needed.
func (d Delivery) ID() string {
	return strings.Join([]string{string(d.Reactor), string(d.Store), string(d.Namespace), string(d.Sequence), string(d.Partition), strconv.FormatUint(uint64(d.SequenceNumber), 10)}, "#")
}

// Invocation supplies per-event metadata and the borrowed batch scope to middleware.
// The observer runtime supplies Event as *E, even for handlers accepting E.
// Do not retain Scope or mutate Event concurrently with the handler.
type Invocation struct {
	Delivery Delivery
	Context  events.Context
	Event    any
	Scope    Scope
}

// Middleware surrounds each handler and its returned effect. All before hooks run;
// any before failure prevents handling. All after hooks run even after failure,
// and their failures are logged without changing the handling outcome. Hooks run
// in registration order (Go's deterministic alternative to C# Task.WhenAll).
type Middleware interface {
	Before(context.Context, Invocation) error
	After(context.Context, Invocation) error
}

// Runtime supplies store-relative operations without coupling plans to transport.
// ReadModel must return the requested type (nil pointers represent absent models).
// Append must report both transport and domain rejection as errors and never retry.
type Runtime interface {
	ReadModel(context.Context, readmodels.Descriptor, readmodels.Key, reflect.Type) (any, error)
	Append(context.Context, events.SourceID, any) error
}

// Lease owns one activated reactor and middleware chain. Invoke is serial and
// must finish before Close. Close is idempotent; use after Close fails explicitly.
// A scope/provider owns resolved artifacts; this lease owns constructor results.
// Acquire through Plan.Activate; the zero value is not usable.
type Lease struct {
	plan        *Plan
	scope       Scope
	instance    any
	middlewares []Middleware
	resources   *artifacts.Lease
}

// Activate opens a scope and constructs the artifact/middleware chain. ctx carries
// batch coordinates, not the first event's identity. No constructors run at Compile.
// Activation failures close all successfully acquired resources before returning.
func (p *Plan) Activate(ctx context.Context) (lease *Lease, err error) {
	l := &Lease{plan: p}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("activation panic: %v", recovered)
		}
		if err != nil {
			err = errors.Join(err, l.Close(ctx))
			err = &ActivationError{p.Identifier(), "activate", err}
			lease = nil
		}
	}()
	l.resources, err = artifacts.Open(ctx, p.services)
	if err != nil {
		return nil, err
	}
	l.scope = l.resources.Scope
	if !p.declaration.explicit {
		l.instance, err = l.resources.Construct(ctx, p.factory)
		if err != nil {
			return nil, err
		}
	}
	for _, factory := range p.middlewares {
		value, createErr := l.resources.Construct(ctx, factory)
		if createErr != nil {
			return nil, createErr
		}
		middleware, ok := value.(Middleware)
		if !ok {
			return nil, invalid("wrong middleware type")
		}
		l.middlewares = append(l.middlewares, middleware)
	}
	return l, nil
}

// Close releases constructor results in reverse order, then closes the scope.
// Cleanup failures are retained and fail handling before acknowledgement. Effects
// may already have happened: a subsequent kernel retry can repeat them.
func (l *Lease) Close(ctx context.Context) error {
	if l.resources == nil {
		return nil
	}
	return l.resources.Close(ctx)
}

// Invoke runs one handler, then its returned event, surrounded by middleware.
// Panics become handling failures rather than terminating the observer worker.
func (l *Lease) Invoke(ctx context.Context, content any, eventContext events.Context, runtime Runtime) (err error) {
	if l.resources.Closed() {
		return invalid("artifact lease closed")
	}
	delivery := Delivery{l.plan.Identifier(), eventContext.Store, eventContext.Namespace, l.plan.EventSequence(), eventContext.SourceID, eventContext.SequenceNumber}
	invocation := Invocation{delivery, eventContext, content, l.scope}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("reactor invocation panic: %v", recovered)
		}
		for _, middleware := range l.middlewares {
			if afterErr := safeHook(ctx, middleware.After, invocation); afterErr != nil {
				l.plan.declaration.config.logger.ErrorContext(ctx, "reactor after middleware failed", "reactor", delivery.Reactor, "error", afterErr)
			}
		}
	}()
	handler, ok := l.plan.handlers[eventContext.EventType.ID]
	if !ok {
		return nil
	}
	for _, middleware := range l.middlewares {
		err = errors.Join(err, safeHook(ctx, middleware.Before, invocation))
	}
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	args := []reflect.Value{}
	if handler.receiver {
		args = append(args, reflect.ValueOf(l.instance))
	}
	if handler.context {
		args = append(args, reflect.ValueOf(ctx))
	}
	eventValue, valid := discovery.EventArgument(content, handler.event)
	if !valid {
		return invalid("event has wrong type")
	}
	args = append(args, eventValue)
	for _, arg := range handler.args {
		var value any
		switch arg.typ {
		case eventContextType:
			value = eventContext
		case deliveryType:
			value = delivery
		default:
			if arg.model.GoType() != nil {
				key := readmodels.Key(eventContext.SourceID)
				if resolveKey := l.plan.declaration.config.key; resolveKey != nil {
					key, err = resolveKey(ctx, content, eventContext)
				} else if resolver, ok := l.instance.(ReadModelKeyResolver); ok {
					key, err = resolver.ResolveReadModelKey(ctx, content, eventContext)
				}
				if err == nil {
					value, err = runtime.ReadModel(ctx, arg.model, key, arg.typ)
				}
			} else {
				value, err = artifacts.Resolve(ctx, l.scope, arg.typ)
			}
		}
		if err != nil {
			return err
		}
		v := reflect.ValueOf(value)
		if !v.IsValid() || !v.Type().AssignableTo(arg.typ) {
			return invalid("wrong resolved argument type")
		}
		args = append(args, v)
	}
	results := handler.fn.Call(args)
	if handler.returnsError && !results[len(results)-1].IsNil() {
		return results[len(results)-1].Interface().(error)
	}
	if handler.returnsEvent {
		value := results[0].Interface()
		if !nilLike(value) {
			source := eventContext.SourceID
			if provider, ok := l.instance.(EventSourceIDProvider); ok {
				source = provider.GetEventSourceID()
			}
			return runtime.Append(ctx, source, value)
		}
	}
	return nil
}
func safeHook(ctx context.Context, hook func(context.Context, Invocation) error, invocation Invocation) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("middleware panic: %v", p)
		}
	}()
	return hook(ctx, invocation)
}

// Descriptor returns the subscribed event descriptor for generation selection.
func (p *Plan) Descriptor(id events.TypeID) (events.Descriptor, bool) {
	d, ok := p.descriptors[id]
	return d, ok
}
