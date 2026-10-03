// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package services opts Chronicle into Fundamentals.Go operation scopes. Import
// this adapter only when using dependency injection; the core SDK does not import
// dependencyinjection, so ordinary Go programs retain a container-free graph.
package services

import (
	"context"
	"reflect"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/fundamentals.go/dependencyinjection"
)

// WithServices borrows a Fundamentals.Go ScopeFactory. Optional Catalog support
// is preserved for NewClient parameter validation. The caller owns the provider;
// close Chronicle before closing it. Client-lifetime collaborators should use
// Singleton bindings, not an operation scope retained for the client's lifetime.
// Provider errors are sanitized: panic payloads/causes are discarded. Inspection
// admits provider diagnostics, standard errors.Join/fmt wrappers, and comparable
// ordinary leaves without Unwrap/As/Is hooks. Safe non-nil pointer leaves retain
// identity unless reachable from a known panic payload/cause. Opaque inspection hooks,
// ambiguous panic identities, cycles, or traversal limits discard the entire
// diagnostic graph; application inspection hooks are never called or forwarded.
func WithServices(factory dependencyinjection.ScopeFactory) chronicle.ClientOption {
	return chronicle.WithServices(adaptFactory(factory))
}

// PrepareClient prepares the captured identity with borrowed Fundamentals scopes.
// Nil selects captured WithServices or the container-free default. Argument and
// retained-outcome rules are those of ClientPreparation.Prepare. Close Chronicle
// and join any outstanding PrepareClient call before closing the provider.
func PrepareClient(ctx context.Context, p *chronicle.ClientPreparation, scopes dependencyinjection.ScopeFactory) (*chronicle.Client, error) {
	return p.Prepare(ctx, adaptFactory(scopes))
}

func adaptFactory(factory dependencyinjection.ScopeFactory) reactors.ScopeFactory {
	if factory == nil {
		return nil
	}
	value := reflect.ValueOf(factory)
	switch value.Kind() {
	case reflect.Pointer, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan, reflect.Interface:
		if value.IsNil() {
			// Preserve typed nil for Prepare's pre-admission validation.
			return (*scopeFactory)(nil)
		}
	}
	adapter := scopeFactory{factory}
	if catalog, ok := factory.(dependencyinjection.Catalog); ok {
		return catalogFactory{adapter, catalog}
	}
	return adapter
}

type scopeFactory struct {
	factory dependencyinjection.ScopeFactory
}

func (f scopeFactory) NewScope(ctx context.Context) (reactors.Scope, error) {
	scope, err := f.factory.NewScope(ctx)
	// Sanitize after acquiring the scope, with its own recovery boundary, so a
	// hostile error tree cannot lose a partially opened scope's cleanup ownership.
	err = sanitizeError(err)
	if scope == nil || (reflect.ValueOf(scope).Kind() == reflect.Pointer && reflect.ValueOf(scope).IsNil()) {
		if err != nil {
			return nil, err
		}
		return nil, dependencyinjection.ErrInvalidScope
	}
	// A failed open may still own resources. Preserve that scope for the lease
	// to release; never close provider-resolved artifacts separately.
	return adaptedScope{scope}, err
}

type catalogFactory struct {
	scopeFactory
	catalog dependencyinjection.Catalog
}

func (f catalogFactory) Contains(typ reflect.Type) bool {
	key, err := dependencyinjection.KeyOf(typ)
	return err == nil && f.catalog.Contains(key)
}

type adaptedScope struct{ scope dependencyinjection.Scope }

func (s adaptedScope) Resolve(ctx context.Context, typ reflect.Type) (any, error) {
	key, err := dependencyinjection.KeyOf(typ)
	if err != nil {
		return nil, err
	}
	value, err := s.scope.Resolve(ctx, key)
	return value, sanitizeError(err)
}
func (s adaptedScope) Close(ctx context.Context) error { return sanitizeError(s.scope.Close(ctx)) }

// Scope unwraps the borrowed Fundamentals scope during a middleware/factory call.
// Arc adapters can verify ScopeOwner/ContextChecker before borrowing it. Never
// close it or retain it past the delivery; Chronicle owns its operation lifetime.
func Scope(scope reactors.Scope) (dependencyinjection.Scope, bool) {
	if wrapped, ok := scope.(interface{ UnderlyingScope() reactors.Scope }); ok {
		scope = wrapped.UnderlyingScope()
	}
	adapted, ok := scope.(adaptedScope)
	return adapted.scope, ok
}
