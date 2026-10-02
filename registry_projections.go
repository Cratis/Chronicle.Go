// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"fmt"
	"slices"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
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
}

func freezeRegistry(registry *Registry, services ...reactorScopeFactory) (registrySnapshot, error) {
	var eventTypes []events.Descriptor
	var models []readmodels.Descriptor
	var declarations []projections.Declaration
	var reactorDeclarations []reactorDeclaration
	snapshot := registrySnapshot{}
	if registry != nil {
		registry.mu.Lock()
		eventTypes = slices.Clone(registry.descriptors)
		models = slices.Clone(registry.readModels)
		declarations = slices.Clone(registry.projections)
		reactorDeclarations = slices.Clone(registry.reactors)
		snapshot.constraints = slices.Clone(registry.constraints)
		registry.mu.Unlock()
	}
	catalog, err := events.NewCatalog(eventTypes...)
	if err != nil {
		return snapshot, err
	}
	snapshot.events = catalog
	modelCatalog, err := readmodels.NewCatalog(models...)
	if err != nil {
		return snapshot, err
	}
	claimed := make(map[readmodels.Identifier]bool)
	for _, declaration := range declarations {
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
	ids := map[string]bool{}
	for _, declaration := range declarations {
		if ids[declaration.Identifier()] {
			return snapshot, fmt.Errorf("%w: duplicate projection identity", ErrInvalidConfiguration)
		}
		ids[declaration.Identifier()] = true
		compiled, compileErr := projections.Compile(declaration, catalog)
		if compileErr != nil {
			return snapshot, compileErr
		}
		snapshot.projections = append(snapshot.projections, compiled)
		for i, model := range models {
			if model.Identifier() == compiled.Model().Identifier() {
				models[i] = compiled.Model()
				break
			}
		}
	}
	snapshot.models, err = readmodels.NewCatalog(models...)
	if err != nil {
		return snapshot, err
	}
	var factory reactorScopeFactory
	if len(services) > 0 {
		factory = services[0]
	}
	err = compileReactors(&snapshot, reactorDeclarations, factory)
	return snapshot, err
}
