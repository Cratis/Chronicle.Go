// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"fmt"

	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

type reducerCatalogs struct {
	defaults []*reducers.Plan
	stores   map[StoreName][]*reducers.Plan
}

// RegisterReducer associates R with one model. NewClient discovers and validates
// exported folds. factory is a plain constructor (optionally with context and
// injected dependencies), or nil for service/default activation. Dependencies
// belong in the constructor, never in fold method parameters.
func RegisterReducer[R, M any](registry *Registry, model readmodels.Model[M], factory any, options ...reducers.Option) error {
	declaration, err := reducers.Define[R](model, factory, options...)
	if err != nil {
		return err
	}
	return addReducer(registry, declaration)
}

// RegisterReducerHandlers binds explicit callbacks to one read model, without DI
// or an artifact type. All callbacks compile to the same fold plan as discovery.
func RegisterReducerHandlers[M any](registry *Registry, model readmodels.Model[M], id reducers.ID, handlers []reducers.Handler, options ...reducers.Option) error {
	declaration, err := reducers.DefineHandlers(model, id, handlers, options...)
	if err != nil {
		return err
	}
	return addReducer(registry, declaration)
}
func addReducer(registry *Registry, declaration reducers.Declaration) error {
	if registry == nil {
		return fmt.Errorf("%w: nil registry", ErrInvalidConfiguration)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, existing := range registry.reducers {
		if existing.Identifier() == declaration.Identifier() || existing.Model().GoType() == declaration.Model().GoType() || (declaration.GoType() != nil && reactorBaseType(existing.GoType()) == reactorBaseType(declaration.GoType())) {
			return &reducers.DeclarationError{Reducer: declaration.Identifier(), Cause: fmt.Errorf("%w: duplicate reducer identity, type or model", ErrInvalidConfiguration)}
		}
	}
	registry.reducers = append(registry.reducers, declaration)
	return nil
}
func compileReducers(snapshot *registrySnapshot, declarations []reducers.Declaration, services reactorScopeFactory) error {
	models := snapshot.models.Descriptors()
	for _, declaration := range declarations {
		fail := func(message string) error {
			return &reducers.DeclarationError{Reducer: declaration.Identifier(), Cause: fmt.Errorf("%w: %s", ErrInvalidConfiguration, message)}
		}
		for _, projection := range snapshot.projections {
			if projection.Identifier() == string(declaration.Identifier()) || projection.Model().Identifier() == declaration.Model().Identifier() {
				return fail("reducer conflicts with projection producer or identity")
			}
		}
		for _, reactor := range snapshot.reactors {
			if string(reactor.Identifier()) == string(declaration.Identifier()) {
				return fail("reducer and reactor share observer identity")
			}
		}
		plan, err := reducers.Compile(declaration, snapshot.events, snapshot.models, services)
		if err != nil {
			return err
		}
		snapshot.reducers = append(snapshot.reducers, plan)
		for i, model := range models {
			if model.Identifier() == plan.Model().Identifier() {
				models[i] = plan.Model()
				break
			}
		}
	}
	for _, model := range models {
		if err := model.ValidateProducer(); err != nil {
			return err
		}
	}
	var err error
	snapshot.models, err = readmodels.NewCatalog(models...)
	return err
}
