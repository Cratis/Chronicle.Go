// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/internal/diagnostics"
	"github.com/cratis/chronicle.go/observation"
	"github.com/cratis/chronicle.go/readmodels"
)

var eventBatchType = reflect.TypeFor[observation.EventBatch]()
var eventBatchPointerType = reflect.PointerTo(eventBatchType)
var batchContextType = reflect.TypeFor[observation.BatchContext]()

// WithBatchMethod declares the event types an exported batch method observes; it
// is Go's form of C# [Observes<TEvent>] on that method. A batch method takes an
// optional context.Context, then *observation.EventBatch, then any of
// observation.BatchContext and services, and returns nothing or error. It is
// called once per page with every event of the types it observes, across
// partitions, in sequence order. Repeat the option or list several types; each
// event type must reach exactly one method of the reactor. Registering fails when
// a method taking *observation.EventBatch has no observed types, or when the
// named method does not take one. Batch methods cannot return events.
func WithBatchMethod(method string, observes ...observation.Observed) Option {
	copied := slices.Clone(observes)
	return func(c *configuration) {
		c.invalid = c.invalid || strings.TrimSpace(method) == "" || len(copied) == 0
		if c.batchMethods == nil {
			c.batchMethods = map[string][]observation.Observed{}
		}
		c.batchMethods[method] = append(c.batchMethods[method], copied...)
	}
}

// OnBatch declares a batch callback for DefineHandlers and WithHandler, observing
// the listed event types. It is called once per page; see WithBatchMethod. A
// callback without observed types fails registration.
func OnBatch(handler func(context.Context, *observation.EventBatch, observation.BatchContext) error, observes ...observation.Observed) Handler {
	return Handler{function: handler, batch: true, observes: slices.Clone(observes)}
}

// BatchMethod identifies a compiled batch method or callback of a Plan. It is an
// adapter seam for the observer runtime and the testing package.
type BatchMethod struct {
	index int
	name  string
}

// Name returns the method name, or callback[n] for an OnBatch callback.
func (m BatchMethod) Name() string { return m.name }

type batchCall struct {
	name     string
	fn       reflect.Value
	receiver bool
	context  bool
	args     []reflect.Type
	observed []events.TypeID
}

// BatchMethods returns the compiled batch methods in a deterministic order.
func (p *Plan) BatchMethods() []BatchMethod {
	methods := make([]BatchMethod, len(p.batches))
	for i, b := range p.batches {
		methods[i] = BatchMethod{index: i, name: b.name}
	}
	return methods
}

// BatchMethodFor returns the batch method observing an event type, if any.
func (p *Plan) BatchMethodFor(id events.TypeID) (BatchMethod, bool) {
	for i, b := range p.batches {
		if slices.Contains(b.observed, id) {
			return BatchMethod{index: i, name: b.name}, true
		}
	}
	return BatchMethod{}, false
}

// InvokeBatch calls a batch method with its batch and context. Middleware does
// not surround batch methods, matching C#. Panics become handling failures.
func (l *Lease) InvokeBatch(ctx context.Context, method BatchMethod, batch *observation.EventBatch, batchContext observation.BatchContext) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = &diagnostics.PanicError{}
		}
	}()
	if l.resources.Closed() {
		return invalid("artifact lease closed")
	}
	if method.index < 0 || method.index >= len(l.plan.batches) || batch == nil {
		return invalid("unknown batch method")
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	call := l.plan.batches[method.index]
	args := []reflect.Value{}
	if call.receiver {
		args = append(args, reflect.ValueOf(l.instance))
	}
	if call.context {
		args = append(args, reflect.ValueOf(ctx))
	}
	args = append(args, reflect.ValueOf(batch))
	for _, typ := range call.args {
		var value any
		if typ == batchContextType {
			value = batchContext
		} else if value, err = artifacts.Resolve(ctx, l.scope, typ); err != nil {
			return err
		}
		v := reflect.ValueOf(value)
		if !v.IsValid() || !v.Type().AssignableTo(typ) {
			return invalid("wrong resolved argument type")
		}
		args = append(args, v)
	}
	results := call.fn.Call(args)
	if len(results) == 1 && !results[0].IsNil() {
		return results[0].Interface().(error)
	}
	return nil
}

// takesBatch reports the index of the *EventBatch parameter, after the receiver
// and an optional context, or -1 when the function is not a batch method.
func takesBatch(t reflect.Type, receiver bool) (int, bool) {
	first := 0
	if receiver {
		first++
	}
	if t.NumIn() > first && t.In(first) == contextType {
		first++
	}
	if t.NumIn() <= first {
		return first, false
	}
	return first, t.In(first) == eventBatchPointerType || t.In(first) == eventBatchType
}

func compileBatch(name string, fn reflect.Value, receiver bool, observes []observation.Observed, catalog *events.Catalog, models *readmodels.Catalog, services ScopeFactory) (batchCall, []events.Descriptor, error) {
	t := fn.Type()
	call := batchCall{name: name, fn: fn, receiver: receiver}
	first := 0
	if receiver {
		first++
	}
	if t.NumIn() > first && t.In(first) == contextType {
		call.context = true
		first++
	}
	if t.IsVariadic() || t.NumIn() <= first || t.In(first) != eventBatchPointerType {
		return call, nil, signature("take *observation.EventBatch, not a value or variadic parameters")
	}
	for i := first + 1; i < t.NumIn(); i++ {
		typ := t.In(i)
		switch typ {
		case batchContextType:
		case eventContextType, deliveryType:
			return call, nil, signature("events.Context and reactors.Delivery are per event; read them from the batch")
		default:
			if _, ok := models.LookupType(typ); ok {
				return call, nil, signature("read models are resolved per event; a batch method cannot take one")
			}
			if err := validateService(services, typ); err != nil {
				return call, nil, &DeclarationError{Method: name, Parameter: typ, Cause: err}
			}
		}
		call.args = append(call.args, typ)
	}
	if t.NumOut() > 1 || (t.NumOut() == 1 && t.Out(0) != errorType) {
		return call, nil, signature("a reactor batch method returns nothing or error; returning events from a batch method is not supported")
	}
	if len(observes) == 0 {
		return call, nil, observation.ErrBatchMethodObservesNothing
	}
	var matching []events.Descriptor
	for _, observed := range observes {
		if observed.Type() == nil {
			return call, nil, observation.ErrBatchMethodObservesUnknownEventType
		}
		found := matchingEvents(observed.Type(), catalog)
		if len(found) == 0 {
			return call, nil, fmt.Errorf("%w: %v", observation.ErrBatchMethodObservesUnknownEventType, observed.Type())
		}
		for _, descriptor := range found {
			if !slices.ContainsFunc(matching, func(d events.Descriptor) bool { return d.Ref() == descriptor.Ref() }) {
				matching = append(matching, descriptor)
			}
		}
	}
	return call, matching, nil
}

// bindBatch records a compiled batch method, rejecting an event type that already
// reaches another method.
func (p *Plan) bindBatch(call batchCall, matching []events.Descriptor) error {
	for _, descriptor := range matching {
		id := descriptor.Ref().ID
		if selected, exists := p.descriptors[id]; exists && selected.Ref() != descriptor.Ref() {
			return invalid("a reactor must select one generation per event ID; use separate reactors for different generations")
		}
		if _, ok := p.BatchMethodFor(id); ok {
			return fmt.Errorf("%w: %s", observation.ErrEventTypeObservedByMoreThanOneMethod, id)
		}
		call.observed = append(call.observed, id)
		p.descriptors[id] = descriptor
	}
	p.batches = append(p.batches, call)
	return nil
}

// checkExclusive rejects an event type that reaches a batch method and a
// per-event handler. Replay handlers count as per-event handlers.
func (p *Plan) checkExclusive() error {
	for _, b := range p.batches {
		for _, id := range b.observed {
			_, live := p.handlers[id]
			_, replay := p.replayHandlers[id]
			if live || replay {
				return fmt.Errorf("%w: %s", observation.ErrEventTypeObservedByMoreThanOneMethod, id)
			}
		}
	}
	return nil
}

func signature(message string) error {
	return fmt.Errorf("%w: %s", observation.ErrInvalidBatchMethodSignature, message)
}
