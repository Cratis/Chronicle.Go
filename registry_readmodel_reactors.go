// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"fmt"

	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
)

type readModelReactorCatalogs struct {
	defaults []*reactors.ReadModelPlan
	stores   map[StoreName][]*reactors.ReadModelPlan
}

// RegisterReadModelReactor registers R for M, discovering exact Added, Modified
// and Removed names at NewClient. factory follows RegisterReactor's constructor
// and service precedence rules. Every callback gets its own scope, activation,
// effects and cleanup. These are best-effort model changes, NOT durable observers.
func RegisterReadModelReactor[R, M any](registry *Registry, model readmodels.Model[M], factory any, options ...reactors.ReadModelOption) error {
	declaration, err := reactors.DefineReadModel[R](model, factory, options...)
	if err != nil {
		return err
	}
	return addReadModelReactor(registry, declaration)
}

// RegisterReadModelReactorHandlers registers explicit callbacks without an
// artifact or container. Multiple callbacks for the same change all run, each
// with a fresh scope, in registration order. NewClient validates signatures.
func RegisterReadModelReactorHandlers[M any](registry *Registry, id reactors.ID, model readmodels.Model[M], handlers []reactors.ReadModelHandler, options ...reactors.ReadModelOption) error {
	declaration, err := reactors.DefineReadModelHandlers(id, model, handlers, options...)
	if err != nil {
		return err
	}
	return addReadModelReactor(registry, declaration)
}
func addReadModelReactor(registry *Registry, declaration reactors.ReadModelDeclaration) error {
	if registry == nil {
		return fmt.Errorf("%w: nil registry", ErrInvalidConfiguration)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, existing := range registry.readModelReactors {
		if existing.Identifier() == declaration.Identifier() {
			return fmt.Errorf("%w: duplicate read-model reactor identity", ErrInvalidConfiguration)
		}
	}
	registry.readModelReactors = append(registry.readModelReactors, declaration)
	return nil
}
func compileReadModelReactors(snapshot *registrySnapshot, declarations []reactors.ReadModelDeclaration, services reactorScopeFactory, effects []reactorSideEffectHandler) error {
	for _, declaration := range declarations {
		plan, err := reactors.CompileReadModel(declaration, snapshot.events, snapshot.models, services, effects)
		if err != nil {
			return err
		}
		snapshot.readModelReactors = append(snapshot.readModelReactors, plan)
	}
	return nil
}
