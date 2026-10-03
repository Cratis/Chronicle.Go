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
func WithServices(factory dependencyinjection.ScopeFactory) chronicle.ClientOption {
	if factory == nil || (reflect.ValueOf(factory).Kind() == reflect.Pointer && reflect.ValueOf(factory).IsNil()) {
		return chronicle.WithServices(nil)
	}
	adapter := scopeFactory{factory}
	if catalog, ok := factory.(dependencyinjection.Catalog); ok {
		return chronicle.WithServices(catalogFactory{adapter, catalog})
	}
	return chronicle.WithServices(adapter)
}

type scopeFactory struct {
	factory dependencyinjection.ScopeFactory
}

func (f scopeFactory) NewScope(ctx context.Context) (reactors.Scope, error) {
	scope, err := f.factory.NewScope(ctx)
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
	return s.scope.Resolve(ctx, key)
}
func (s adaptedScope) Close(ctx context.Context) error { return s.scope.Close(ctx) }

// Scope unwraps the borrowed Fundamentals scope during a middleware/factory call.
// Arc adapters can verify ScopeOwner/ContextChecker before borrowing it. Never
// close it or retain it past the delivery; Chronicle owns its operation lifetime.
func Scope(scope reactors.Scope) (dependencyinjection.Scope, bool) {
	adapted, ok := scope.(adaptedScope)
	return adapted.scope, ok
}
