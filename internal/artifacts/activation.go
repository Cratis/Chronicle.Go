// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package artifacts

import (
	"context"
	"errors"
	"reflect"
	"sync"
)

var contextType = reflect.TypeFor[context.Context]()
var errorType = reflect.TypeFor[error]()
var scopeType = reflect.TypeFor[Scope]()

// Constructor is an immutable activation plan.
type Constructor struct {
	typ      reflect.Type
	fn       reflect.Value
	context  bool
	args     []reflect.Type
	borrowed bool
}

// ParameterError retains the unresolvable constructor dependency.
type ParameterError struct {
	Type  reflect.Type
	Cause error
}

func (e *ParameterError) Error() string { return e.Cause.Error() }
func (e *ParameterError) Unwrap() error { return e.Cause }

// ValidateService checks only an advertised catalog; no services are resolved.
func ValidateService(factory ScopeFactory, typ reflect.Type) error {
	if typ == scopeType {
		return nil
	}
	if catalog, ok := factory.(Catalog); ok && !catalog.Contains(typ) {
		return invalid("unresolvable service parameter: " + typ.String())
	}
	return nil
}

// CompileConstructor preserves registered-service precedence over constructors.
func CompileConstructor(typ reflect.Type, factory any, services ScopeFactory) (Constructor, error) {
	c := Constructor{typ: typ}
	_, plain := services.(defaultFactory)
	catalog, hasCatalog := services.(Catalog)
	if factory == nil || (!plain && hasCatalog && catalog.Contains(typ)) {
		c.borrowed = true
		return c, ValidateService(services, typ)
	}
	if NilLike(factory) {
		return c, invalid("nil constructor")
	}
	t := reflect.TypeOf(factory)
	if t.Kind() != reflect.Func || t.IsVariadic() || t.NumOut() < 1 || t.NumOut() > 2 || t.Out(0) != typ || (t.NumOut() == 2 && t.Out(1) != errorType) {
		return c, invalid("constructor must return the registered type, optionally followed by error")
	}
	c.fn = reflect.ValueOf(factory)
	for i := 0; i < t.NumIn(); i++ {
		arg := t.In(i)
		if i == 0 && arg == contextType {
			c.context = true
			continue
		}
		if err := ValidateService(services, arg); err != nil {
			return c, &ParameterError{arg, err}
		}
		c.args = append(c.args, arg)
	}
	return c, nil
}

// Lease owns one operation's scope and constructor results. Invoke and Close must
// not overlap. Repeated Close joins cleanup and returns its retained outcome.
type Lease struct {
	Scope     Scope
	owned     []any
	mu        sync.Mutex
	closed    bool
	closeErr  error
	closeDone chan struct{}
}

// Open obtains a scope. The returned lease must be closed even on failure.
func Open(ctx context.Context, services ScopeFactory) (l *Lease, err error) {
	l = &Lease{closeDone: make(chan struct{})}
	defer func() {
		if p := recover(); p != nil {
			err = &panicError{value: p}
		}
	}()
	l.Scope, err = services.NewScope(ctx)
	if NilLike(l.Scope) {
		l.Scope = nil
		if err == nil {
			err = invalid("scope factory returned nil")
		}
	}
	return l, err
}

// Construct obtains a borrowed service or owns the result of a constructor,
// including non-nil partial values returned alongside errors.
func (l *Lease) Construct(ctx context.Context, c Constructor) (any, error) {
	return l.construct(ctx, c, l.Scope)
}

func (l *Lease) construct(ctx context.Context, c Constructor, scope Scope) (any, error) {
	if c.borrowed {
		return Resolve(ctx, scope, c.typ)
	}
	args := []reflect.Value{}
	if c.context {
		args = append(args, reflect.ValueOf(ctx))
	}
	for _, t := range c.args {
		value, err := Resolve(ctx, scope, t)
		if err != nil {
			return nil, err
		}
		args = append(args, reflect.ValueOf(value))
	}
	results := c.fn.Call(args)
	value := results[0].Interface()
	if !NilLike(value) {
		l.owned = append(l.owned, value)
	}
	if len(results) == 2 && !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}
	if NilLike(value) {
		return nil, invalid("constructor returned nil")
	}
	return value, nil
}

// Resolve validates exact resolver results before reflective calls.
func Resolve(ctx context.Context, scope Scope, t reflect.Type) (any, error) {
	if t == scopeType {
		return scope, nil
	}
	value, err := scope.Resolve(ctx, t)
	if err != nil {
		return nil, err
	}
	if NilLike(value) || !reflect.TypeOf(value).AssignableTo(t) {
		return nil, invalid("resolver returned nil or wrong service type: " + t.String())
	}
	return value, nil
}

// Closed guards use after cleanup.
func (l *Lease) Closed() bool { l.mu.Lock(); defer l.mu.Unlock(); return l.closed }

// Close disposes constructor results in reverse order, then the scope.
func (l *Lease) Close(ctx context.Context) error {
	l.mu.Lock()
	if l.closed {
		done := l.closeDone
		l.mu.Unlock()
		<-done
		return l.closeErr
	}
	l.closed = true
	l.mu.Unlock()
	var err error
	for i := len(l.owned) - 1; i >= 0; i-- {
		err = errors.Join(err, closeValue(ctx, l.owned[i]))
	}
	if l.Scope != nil {
		err = errors.Join(err, closeValue(ctx, l.Scope))
	}
	l.mu.Lock()
	l.closeErr = err
	close(l.closeDone)
	l.mu.Unlock()
	return err
}
