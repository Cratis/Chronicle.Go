// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"slices"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"github.com/cratis/chronicle.go/seeding"
	"github.com/cratis/chronicle.go/serialization"
)

// AddProjection admits immutable authoring metadata. Duplicate identities/models
// fail immediately; forward event references and the entire graph are resolved
// atomically by NewClient against this registry, never a global event catalog.
func (r *Registry) AddProjection(declaration projections.Declaration) error {
	if r == nil || declaration.Identifier() == "" || declaration.Model().GoType() == nil {
		return fmt.Errorf("%w: registry and projection declaration required", ErrInvalidConfiguration)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.projections {
		if existing.Identifier() == declaration.Identifier() || existing.Model().GoType() == declaration.Model().GoType() {
			return fmt.Errorf("%w: duplicate projection identity or model", ErrInvalidConfiguration)
		}
	}
	r.projections = append(r.projections, declaration)
	return nil
}

type registrySnapshot struct {
	events      *events.Catalog
	models      *readmodels.Catalog
	constraints []constraints.Definition
	projections []projections.Definition
	reactors    []*reactorPlan
	reducers    []*reducers.Plan
	seeds       seeding.Definition
}

func freezeRegistry(ctx context.Context, registry *Registry, policy serialization.NamingPolicy, services reactorScopeFactory, validateGenerations bool) (registrySnapshot, error) {
	var eventTypes []events.Descriptor
	var migrations []events.MigrationDeclaration
	var constraintCompositions []constraintComposition
	var models []readmodels.Descriptor
	var declarations []projections.Declaration
	var reactorDeclarations []reactorDeclaration
	var reducerDeclarations []reducers.Declaration
	var seeders []seederDeclaration
	var reactorMiddlewares []any
	var reactorSideEffects []reactorSideEffectHandler
	snapshot := registrySnapshot{}
	if registry != nil {
		registry.mu.Lock()
		eventTypes = slices.Clone(registry.descriptors)
		migrations = slices.Clone(registry.migrations)
		models = slices.Clone(registry.readModels)
		declarations = slices.Clone(registry.projections)
		reactorDeclarations = slices.Clone(registry.reactors)
		reducerDeclarations = slices.Clone(registry.reducers)
		seeders = slices.Clone(registry.seeders)
		reactorMiddlewares = slices.Clone(registry.reactorMiddlewares)
		reactorSideEffects = slices.Clone(registry.reactorSideEffects)
		snapshot.constraints = slices.Clone(registry.constraints)
		constraintCompositions = slices.Clone(registry.constraintCompositions)
		registry.mu.Unlock()
	}
	catalog, err := events.NewCatalog(eventTypes...)
	if err != nil {
		return snapshot, err
	}
	if err := catalog.ValidateDeclarations(); err != nil {
		return snapshot, err
	}
	snapshot.constraints, err = compileDeclaredConstraints(catalog, snapshot.constraints, constraintCompositions)
	if err != nil {
		return snapshot, err
	}
	snapshot.events = catalog
	modelCatalog, err := readmodels.NewCatalog(models...)
	if err != nil {
		return snapshot, err
	}
	for _, declaration := range reducerDeclarations {
		model, ok := modelCatalog.LookupIdentifier(declaration.Model().Identifier())
		if !ok || model != declaration.Model() {
			return snapshot, &reducers.DeclarationError{Reducer: declaration.Identifier(), Cause: fmt.Errorf("%w: reducer model is not registered in this store", ErrInvalidConfiguration)}
		}
	}
	claimed := make(map[readmodels.Identifier]bool)
	for _, declaration := range declarations {
		if declaration.IsGlobal() {
			for _, model := range models {
				if model.GoType() == declaration.Model().GoType() {
					return snapshot, fmt.Errorf("%w: global handler must not be registered as a read model", ErrInvalidConfiguration)
				}
			}
			continue
		}
		model := declaration.Model()
		registered, ok := modelCatalog.LookupIdentifier(model.Identifier())
		if !ok || registered != model {
			return snapshot, fmt.Errorf("%w: projection model is not registered in this store", ErrInvalidConfiguration)
		}
		claimed[model.Identifier()] = true
	}
	for _, model := range models {
		if !claimed[model.Identifier()] && projections.HasMappings(model) {
			declarations = append(declarations, projections.ModelBoundDescriptor(model))
		}
	}
	snapshot.projections, err = projections.CompileGroup(declarations, catalog)
	if err != nil {
		return snapshot, err
	}
	for _, compiled := range snapshot.projections {
		for i, model := range models {
			if model.Identifier() == compiled.Model().Identifier() {
				models[i] = compiled.Model()
				break
			}
		}
	}
	// Resolve authoring against declaration plans first, then rebind every path by
	// field identity into detached client plans. The registry and its handles stay immutable.
	for i, event := range eventTypes {
		eventTypes[i], err = event.WithNamingPolicy(policy)
		if err != nil {
			return registrySnapshot{}, err
		}
	}
	snapshot.events, err = events.NewCatalog(eventTypes...)
	if err != nil {
		return registrySnapshot{}, err
	}
	snapshot.events, err = snapshot.events.WithMigrations(migrations, validateGenerations)
	if err != nil {
		return registrySnapshot{}, err
	}
	for i, model := range models {
		models[i], err = model.WithNamingPolicy(policy)
		if err != nil {
			return registrySnapshot{}, err
		}
	}
	snapshot.models, err = readmodels.NewCatalog(models...)
	if err != nil {
		return registrySnapshot{}, err
	}
	for i, constraint := range snapshot.constraints {
		snapshot.constraints[i], err = constraint.Rebind(snapshot.events)
		if err != nil {
			return registrySnapshot{}, err
		}
	}
	for i, projection := range snapshot.projections {
		model, _ := snapshot.models.LookupIdentifier(projection.Model().Identifier())
		snapshot.projections[i], err = projection.Rebind(model, catalog, snapshot.events)
		if err != nil {
			return registrySnapshot{}, err
		}
	}
	err = compileReactors(&snapshot, reactorDeclarations, services, reactorMiddlewares, reactorSideEffects)
	if err != nil {
		return snapshot, err
	}
	if err = compileReducers(&snapshot, reducerDeclarations, services); err != nil {
		return snapshot, err
	}
	snapshot.seeds, err = prepareSeeders(ctx, snapshot.events, seeders, services)
	return snapshot, err
}
