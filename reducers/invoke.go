// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reducers

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/internal/discovery"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/metadata"
)

// Event is a decoded event and its delivery context. Content must match the
// registered event (E or *E). The operation borrows it; do not mutate concurrently.
type Event struct {
	Content any
	Context events.Context
}

// Result distinguishes deleted/absent state, present state and a failed fold.
// State is nil on failure, never partially computed state. On success a non-nil
// State is *M. LastSuccessful is Unavailable before the first successful fold.
type Result struct {
	State          any
	LastSuccessful events.SequenceNumber
	Err            error
}

// Batch contains stable operation coordinates, available to constructors without
// making the first event's context ambient for all subsequent invocations.
type Batch struct {
	Reducer   ID
	Store     metadata.StoreName
	Namespace metadata.Namespace
	Sequence  events.SequenceID
}
type batchKey struct{}

// WithBatch installs operation coordinates before activation.
func WithBatch(ctx context.Context, batch Batch) context.Context {
	return context.WithValue(ctx, batchKey{}, batch)
}

// BatchFromContext returns reducer coordinates if installed by the observer runtime.
func BatchFromContext(ctx context.Context) (Batch, bool) {
	value, ok := ctx.Value(batchKey{}).(Batch)
	return value, ok
}

// Lease owns the same scope/constructor resource machinery as reactors. Invoke is
// serial; it must finish before Close. A lease is acquired only through Activate.
type Lease struct {
	plan      *Plan
	resources *artifacts.Lease
	instance  any
}

// Activate creates exactly one scope and artifact, under system identity. Failed
// activation cleans up partial resources, preserving errors and ownership.
func (p *Plan) Activate(ctx context.Context) (lease *Lease, err error) {
	l := &Lease{plan: p}
	ctx = metadata.WithIdentity(ctx, identities.System())
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("reducer activation panic: %v", recovered)
		}
		if err != nil {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			err = errors.Join(err, l.Close(cleanup))
			cancel()
			err = &ActivationError{p.Identifier(), "activate", err}
			lease = nil
		}
	}()
	l.resources, err = artifacts.Open(ctx, p.services)
	if err != nil {
		return nil, err
	}
	if !p.declaration.explicit {
		l.instance, err = l.resources.Construct(ctx, p.factory)
		if err != nil {
			return nil, err
		}
	}
	return l, nil
}

// Close disposes constructor results and then the scope once. Cleanup errors
// prevent successful acknowledgement even if all folds completed.
func (l *Lease) Close(ctx context.Context) error {
	if l.resources == nil {
		return nil
	}
	return l.resources.Close(ctx)
}

// Invoke applies one event. Nil state means absent/deleted. A value-current M
// parameter receives zero M for absence; use *M to preserve nullable semantics.
// Nil output deletes; an error discards output. Mutating current in place is not
// recommended: return a complete new model. Panics become failures.
func (l *Lease) Invoke(ctx context.Context, event Event, current any) (state any, err error) {
	defer func() {
		if p := recover(); p != nil {
			state = nil
			err = fmt.Errorf("reducer fold panic: %v", p)
		}
	}()
	if l.resources.Closed() {
		return nil, invalid("artifact lease closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, ok := l.plan.folds[event.Context.EventType.ID]
	if !ok {
		return nil, fmt.Errorf("%w: unsubscribed reducer event", faults.ErrProtocol)
	}
	eventValue, ok := discovery.EventArgument(event.Content, f.event)
	if !ok {
		return nil, fmt.Errorf("%w: wrong reducer event content", faults.ErrProtocol)
	}
	model := l.plan.model.GoType()
	value := reflect.Zero(reflect.PointerTo(model))
	if !artifacts.NilLike(current) {
		value = reflect.ValueOf(current)
		if value.Type() == model {
			pointer := reflect.New(model)
			pointer.Elem().Set(value)
			value = pointer
		}
		if value.Type() != reflect.PointerTo(model) {
			return nil, invalid("wrong initial read model")
		}
	}
	if f.valueCurrent {
		if value.IsNil() {
			value = reflect.Zero(model)
		} else {
			value = value.Elem()
		}
	}
	args := []reflect.Value{}
	if f.receiver {
		args = append(args, reflect.ValueOf(l.instance))
	}
	if f.context {
		args = append(args, reflect.ValueOf(metadata.WithIdentity(ctx, identities.System())))
	}
	args = append(args, eventValue, value)
	if f.eventContext {
		args = append(args, reflect.ValueOf(event.Context))
	}
	results := f.function.Call(args)
	if f.returnsError && !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}
	next := results[0]
	if next.Kind() == reflect.Pointer {
		if next.IsNil() {
			return nil, nil
		}
		return next.Interface(), nil
	}
	pointer := reflect.New(model)
	pointer.Elem().Set(next)
	return pointer.Interface(), nil
}

// Reduce folds sequentially in one scope, stopping at the first failure. The
// entire operation fails on decoding/cancellation/activation/cleanup errors and
// never returns partial State. Input order must be strictly increasing.
func (p *Plan) Reduce(ctx context.Context, batch []Event, initial any) (result Result) {
	result.LastSuccessful = events.Unavailable
	ctx = metadata.WithIdentity(ctx, identities.System())
	lease, err := p.Activate(ctx)
	if err != nil {
		result.Err = err
		return result
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			result.Err = fmt.Errorf("reducer batch panic: %v", recovered)
		}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err := lease.Close(cleanup)
		cancel()
		if err != nil {
			result.Err = errors.Join(result.Err, &ActivationError{p.Identifier(), "close", err})
			result.LastSuccessful = events.Unavailable
		}
		if result.Err == nil {
			result.Err = ctx.Err()
		}
		if result.Err != nil {
			result.State = nil
		}
	}()
	result.State = initial
	var previous events.SequenceNumber
	for i, event := range batch {
		if event.Context.SequenceNumber >= events.Unavailable-2 || (i > 0 && event.Context.SequenceNumber <= previous) {
			result.Err = faults.ErrProtocol
			return result
		}
		previous = event.Context.SequenceNumber
		result.State, result.Err = lease.Invoke(ctx, event, result.State)
		if result.Err != nil {
			return result
		}
		result.LastSuccessful = event.Context.SequenceNumber
	}
	return result
}
