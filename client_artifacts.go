// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"slices"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

// Artifacts is a detached view of the immutable production compilation results.
// Plans borrow the services supplied to NewClient; no constructors run when read.
type Artifacts struct {
	// Events contains naming-bound event declarations.
	Events *events.Catalog
	// ReadModels contains naming- and producer-bound model declarations.
	ReadModels *readmodels.Catalog
	// Projections contains store-bound compiled projection definitions.
	Projections []projections.Definition
	// Reactors contains immutable production invocation plans.
	Reactors []*reactors.Plan
	// Reducers contains immutable production fold plans.
	Reducers []*reducers.Plan
	// Constraints contains the naming-bound append constraints.
	Constraints []constraints.Definition
}

// Artifacts returns the same frozen plans used by runtime registration and
// invocation. It performs no I/O and is safe after Close following successful
// preparation, like Catalogs. An unprepared identity returns ErrNotPrepared.
func (c *Client) Artifacts(store StoreName) (Artifacts, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requirePreparedLocked("artifacts", true); err != nil {
		return Artifacts{}, err
	}
	snapshot, err := c.selectedStoreSnapshotLocked(store)
	if err != nil {
		return Artifacts{}, err
	}
	reactorPlans := snapshot.reactors
	reducerPlans := snapshot.reducers
	return Artifacts{snapshot.events, snapshot.models, slices.Clone(snapshot.projections), slices.Clone(reactorPlans), slices.Clone(reducerPlans), slices.Clone(snapshot.constraints)}, nil
}

// WithProjection returns a detached registry with a replacement producer for the
// declaration's model. It removes that model's prior projection/factory/reducer, retains
// all other registrations, and never mutates r. NewClient performs normal complete
// compilation/validation. This supports scenario-local inline overrides without
// introducing another discovery pipeline. The model must already be registered.
func (r *Registry) WithProjection(declaration projections.Declaration) (*Registry, error) {
	if r == nil || declaration.Model().GoType() == nil || declaration.IsGlobal() {
		return nil, ErrInvalidConfiguration
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	found := false
	for _, model := range r.readModels {
		if model == declaration.Model() {
			found = true
		}
	}
	if !found {
		return nil, ErrNotRegistered
	}
	result := &Registry{
		constraintFactories: slices.Clone(r.constraintFactories), migrationFactories: slices.Clone(r.migrationFactories),
		descriptors: slices.Clone(r.descriptors), constraints: slices.Clone(r.constraints), readModels: slices.Clone(r.readModels),
		constraintCompositions: slices.Clone(r.constraintCompositions), migrations: slices.Clone(r.migrations), seeders: slices.Clone(r.seeders),
		readModelReactors: slices.Clone(r.readModelReactors),
		reactors:          slices.Clone(r.reactors), reactorMiddlewares: slices.Clone(r.reactorMiddlewares), reactorSideEffects: slices.Clone(r.reactorSideEffects),
	}
	for _, existing := range r.projections {
		if existing.Model().GoType() != declaration.Model().GoType() {
			result.projections = append(result.projections, existing)
		}
	}
	for _, existing := range r.projectionFactories {
		if existing.model.GoType() != declaration.Model().GoType() {
			result.projectionFactories = append(result.projectionFactories, existing)
		}
	}
	for _, existing := range r.reducers {
		if existing.Model().GoType() != declaration.Model().GoType() {
			result.reducers = append(result.reducers, existing)
		}
	}
	if err := result.AddProjection(declaration); err != nil {
		return nil, err
	}
	return result, nil
}
