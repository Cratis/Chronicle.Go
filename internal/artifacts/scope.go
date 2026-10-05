// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package artifacts shares observer construction, resolver and ownership semantics.
package artifacts

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"sync"

	"github.com/cratis/chronicle.go/internal/faults"
)

// Scope is a borrowed resolver during activation and handling. Implementations
// must honor cancellation. Chronicle closes each scope after effects, before ack.
// The optional services package adapts Fundamentals.Go scopes to this core seam.
type Scope interface {
	Resolve(context.Context, reflect.Type) (any, error)
	Close(context.Context) error
}

// ScopeFactory opens a scope per received batch (or event with PerEvent).
// The factory/provider itself is borrowed and is never closed by Chronicle.
type ScopeFactory interface {
	NewScope(context.Context) (Scope, error)
}

// Catalog is an optional ScopeFactory capability for startup validation. Without
// it, service parameters are assumed resolvable, as in C# DefaultServiceProvider.
type Catalog interface{ Contains(reflect.Type) bool }

// DefaultScopeFactory supplies the zero-container path: zero-valued structs,
// pointers to structs, and empty slices. Interfaces require explicit constructors
// or an opt-in resolver. Constructed values are owned by the scope.
func DefaultScopeFactory() ScopeFactory { return defaultFactory{} }

// IsDefaultScopeFactory identifies zero-container activation without wrapping or
// changing the concrete factory used for constructor precedence.
func IsDefaultScopeFactory(factory ScopeFactory) bool {
	_, ok := factory.(defaultFactory)
	return ok
}

type defaultFactory struct{}

func (defaultFactory) NewScope(context.Context) (Scope, error) { return &defaultScope{}, nil }
func (defaultFactory) Contains(t reflect.Type) bool {
	base := t
	if base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	// The SDK client has identity and lifecycle ownership; zero construction (or
	// a value copy) cannot supply a usable borrowed client to an artifact.
	if base == reflect.TypeFor[slog.Logger]() {
		// A zero logger has no handler. Diagnostic options do not supply services.
		return false
	}
	if base.PkgPath() == "github.com/cratis/chronicle.go" && base.Name() == "Client" {
		return false
	}
	return t.Kind() == reflect.Struct || (t.Kind() == reflect.Pointer && t.Elem().Kind() == reflect.Struct) || t.Kind() == reflect.Slice
}

type defaultScope struct {
	mu     sync.Mutex
	closed bool
	values []any
}

func (s *defaultScope) Resolve(ctx context.Context, typ reflect.Type) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, invalid("scope closed")
	}
	if !(defaultFactory{}).Contains(typ) {
		return nil, invalid("service requires an explicit constructor or resolver")
	}
	var value any
	switch typ.Kind() {
	case reflect.Pointer:
		value = reflect.New(typ.Elem()).Interface()
	case reflect.Slice:
		value = reflect.MakeSlice(typ, 0, 0).Interface()
	default:
		value = reflect.Zero(typ).Interface()
	}
	s.values = append(s.values, value)
	return value, nil
}
func (s *defaultScope) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	values := s.values
	s.values = nil
	s.mu.Unlock()
	var err error
	for i := len(values) - 1; i >= 0; i-- {
		err = errors.Join(err, closeValue(ctx, values[i]))
	}
	return err
}
func closeValue(ctx context.Context, value any) (err error) {
	defer func() {
		if recover() != nil {
			err = &panicError{}
		}
	}()
	if closer, ok := value.(interface{ CloseContext(context.Context) error }); ok {
		return closer.CloseContext(ctx)
	}
	if closer, ok := value.(interface{ Close(context.Context) error }); ok {
		return closer.Close(ctx)
	}
	if closer, ok := value.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// NilLike recognizes typed nils at user extension boundaries.
func NilLike(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Pointer, reflect.Func, reflect.Interface, reflect.Map, reflect.Slice, reflect.Chan:
		return r.IsNil()
	}
	return false
}

func invalid(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}
