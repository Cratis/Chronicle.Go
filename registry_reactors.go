// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"reflect"

	"github.com/cratis/chronicle.go/reactors"
)

type reactorDeclaration = reactors.Declaration
type reactorPlan = reactors.Plan
type reactorScopeFactory = reactors.ScopeFactory
type reactorCatalogs struct {
	defaults []*reactors.Plan
	stores   map[StoreName][]*reactors.Plan
}

// RegisterReactor explicitly admits R, discovering its exported event handler
// methods when NewClient freezes the registry. factory is a constructor returning
// R or (R,error), with optional context and resolved services. Plain func() R
// closures need no dependency injection imports. Nil requests service activation.
func RegisterReactor[R any](registry *Registry, factory any, options ...reactors.Option) error {
	declaration, err := reactors.Define[R](factory, options...)
	if err != nil {
		return err
	}
	return addReactor(registry, declaration)
}

// RegisterReactorHandler registers a typed closure without an artifact or DI.
// Additional typed handlers can be supplied using reactors.WithHandler.
func RegisterReactorHandler[E any](registry *Registry, id reactors.ID, handler func(context.Context, E) error, options ...reactors.Option) error {
	declaration, err := reactors.DefineHandler(id, handler, options...)
	if err != nil {
		return err
	}
	return addReactor(registry, declaration)
}

// RegisterReactorHandlers registers one or more typed callbacks, including a
// reactor composed solely of reactors.Returning callbacks. Input slices are copied.
func RegisterReactorHandlers(registry *Registry, id reactors.ID, handlers []reactors.Handler, options ...reactors.Option) error {
	declaration, err := reactors.DefineHandlers(id, handlers, options...)
	if err != nil {
		return err
	}
	return addReactor(registry, declaration)
}

// RegisterReactorMiddleware adds a constructor for every reactor in this registry.
// Registry middleware runs in registration order before per-reactor middleware.
// Factory signatures and services are validated at NewClient, without activation,
// just like reactors.WithMiddleware. Duplicate registrations run independently.
// NewClient freezes the list; later registrations do not affect existing clients.
func RegisterReactorMiddleware(registry *Registry, factory any) error {
	if registry == nil || nilValue(factory) {
		return fmt.Errorf("%w: registry and middleware factory required", ErrInvalidConfiguration)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.reactorMiddlewares = append(registry.reactorMiddlewares, factory)
	return nil
}

func addReactor(registry *Registry, declaration reactors.Declaration) error {
	if registry == nil {
		return fmt.Errorf("%w: nil registry", ErrInvalidConfiguration)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, existing := range registry.reactors {
		if existing.Identifier() == declaration.Identifier() || (declaration.GoType() != nil && reactorBaseType(existing.GoType()) == reactorBaseType(declaration.GoType())) {
			return fmt.Errorf("%w: duplicate reactor identity or type", ErrInvalidConfiguration)
		}
	}
	registry.reactors = append(registry.reactors, declaration)
	return nil
}

func reactorBaseType(typ reflect.Type) reflect.Type {
	if typ != nil && typ.Kind() == reflect.Pointer {
		return typ.Elem()
	}
	return typ
}

// WithServices borrows operation resources through a container-independent seam.
// For a Fundamentals.Go ScopeFactory use services.WithServices from the optional
// github.com/cratis/chronicle.go/services adapter. No provider is owned or closed.
func WithServices(factory reactors.ScopeFactory) ClientOption {
	return func(c *clientConfig) { c.reactorServices = factory; c.reactorServicesSet = true }
}

func compileReactors(snapshot *registrySnapshot, declarations []reactorDeclaration, services reactorScopeFactory, middlewares []any) error {
	for _, declaration := range declarations {
		plan, err := reactors.CompileWithMiddleware(declaration, snapshot.events, snapshot.models, services, middlewares)
		if err != nil {
			return err
		}
		for _, projection := range snapshot.projections {
			if projection.Identifier() == string(plan.Identifier()) {
				return fmt.Errorf("%w: reactor and projection share observer identity", ErrInvalidConfiguration)
			}
		}
		snapshot.reactors = append(snapshot.reactors, plan)
	}
	return nil
}
