// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"slices"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/reactors"
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
	if projectionIdentityTaken(r, declaration.Identifier(), declaration.Model()) {
		return fmt.Errorf("%w: duplicate projection identity or model", ErrInvalidConfiguration)
	}
	r.projections = append(r.projections, declaration)
	return nil
}

type registrySnapshot struct {
	events            *events.Catalog
	models            *readmodels.Catalog
	constraints       []constraints.Definition
	projections       []projections.Definition
	reactors          []*reactorPlan
	readModelReactors []*reactors.ReadModelPlan
	reducers          []*reducers.Plan
	seeds             seeding.Definition
}

// compileRegistry resolves only the captured declarations. No mutable Registry is
// consulted here, including after constraint composition or seeder preparation.
func compileRegistry(ctx context.Context, captured *registryDeclarations, policy serialization.NamingPolicy, services reactorScopeFactory, validateGenerations bool) (registrySnapshot, error) {
	if err := validateDefinitionMetadata(captured); err != nil {
		return registrySnapshot{}, err
	}
	plans, err := preflightDefinitionFactories(captured, services, nil)
	if err != nil {
		return registrySnapshot{}, err
	}
	schemas, err := prepareRegistrySchemas(captured, policy)
	if err != nil {
		return registrySnapshot{}, err
	}
	return compilePreparedRegistry(ctx, captured, schemas, services, validateGenerations, plans)
}

func compilePreparedRegistry(ctx context.Context, captured *registryDeclarations, schemas registrySchemas, services reactorScopeFactory, validateGenerations bool, plans registryFactoryPlans) (snapshot registrySnapshot, err error) {
	err = artifacts.Protect("registry", "compile", func() error {
		prepared, prepareErr := prepareDefinitionFactories(ctx, captured, plans)
		if prepareErr != nil {
			return prepareErr
		}
		snapshot, prepareErr = compileRegistryDefinitions(ctx, prepared, schemas, services, validateGenerations, plans)
		return prepareErr
	})
	if err != nil {
		return registrySnapshot{}, err
	}
	return snapshot, nil
}

func compileRegistryDefinitions(ctx context.Context, captured *registryDeclarations, schemas registrySchemas, services reactorScopeFactory, validateGenerations bool, plans registryFactoryPlans) (registrySnapshot, error) {
	models := slices.Clone(captured.readModels)
	declarations := slices.Clone(captured.projections)
	snapshot := registrySnapshot{constraints: slices.Clone(captured.constraints)}
	catalog, err := events.NewCatalog(captured.descriptors...)
	if err != nil {
		return snapshot, err
	}
	if err := catalog.ValidateDeclarations(); err != nil {
		return snapshot, err
	}
	snapshot.constraints, err = compileDeclaredConstraints(catalog, snapshot.constraints, captured.constraintCompositions)
	if err != nil {
		return snapshot, err
	}
	snapshot.events = catalog
	modelCatalog, err := readmodels.NewCatalog(models...)
	if err != nil {
		return snapshot, err
	}
	for _, declaration := range captured.reducers {
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
	// Authoring used original field names. Only bind producer metadata onto the
	// already frozen named models; never re-evaluate their classification providers.
	models = schemas.models.Descriptors()
	for _, compiled := range snapshot.projections {
		for i, model := range models {
			if model.Identifier() == compiled.Model().Identifier() {
				models[i], err = readmodels.BindProjection(model, compiled.Identifier(), compiled.EventSequence(), compiled.IsPassive())
				if err != nil {
					return registrySnapshot{}, err
				}
				break
			}
		}
	}
	snapshot.events, err = schemas.events.WithMigrations(captured.migrations, validateGenerations)
	if err != nil {
		return registrySnapshot{}, err
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
	err = compileReactors(&snapshot, captured.reactors, services, captured.reactorMiddlewares, captured.reactorSideEffects)
	if err != nil {
		return snapshot, err
	}
	if err = compileReducers(&snapshot, captured.reducers, services); err != nil {
		return snapshot, err
	}
	if err = compileReadModelReactors(&snapshot, captured.readModelReactors, services, captured.reactorSideEffects); err != nil {
		return snapshot, err
	}
	snapshot.seeds, err = prepareSeeders(ctx, snapshot.events, captured.seeders, plans)
	return snapshot, err
}
