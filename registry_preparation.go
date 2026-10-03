// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"slices"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type registrySchemas struct {
	events *events.Catalog
	models *readmodels.Catalog
}

// registryAuthoringOutput retains original descriptor identities and finalized
// definitions, including initial JSON and the complete projection relationship
// graph. It is not a declaration capture: factories and composition callbacks
// have finished, and these definitions must never be rebuilt from Go types.
type registryAuthoringOutput struct {
	events      *events.Catalog
	models      *readmodels.Catalog
	constraints []constraints.Definition
	projections []projections.Definition
}

// registryPreparationOutput owns accepted authoring and naming-bound runtime
// outputs. No preparation context, definition factory, seeder or temporary scoped
// result is kept. Observer plans retain their existing runtime constructors,
// callbacks and borrowed provider; composing views must reuse those plans, not
// compile their declarations again. Direct runtime constraint messages also keep
// their existing borrowed-callback contract.
type registryPreparationOutput struct {
	authoring registryAuthoringOutput
	frozen    registrySnapshot
}

// compose makes a detached publication view without application code, catalog
// construction, schema compilation or observer recompilation. Definitions own
// immutable data and expose defensive accessors; only their containing slices
// need copying. Catalogs, closed codec graphs, serialized seeds and observer
// plans deliberately keep the exact identities accepted during preparation.
func (output *registryPreparationOutput) compose() registrySnapshot {
	snapshot := output.frozen
	snapshot.constraints = slices.Clone(snapshot.constraints)
	snapshot.projections = slices.Clone(snapshot.projections)
	snapshot.reactors = slices.Clone(snapshot.reactors)
	snapshot.reducers = slices.Clone(snapshot.reducers)
	snapshot.readModelReactors = slices.Clone(snapshot.readModelReactors)
	return snapshot
}

// validateDefinitionMetadata is callback-free admission against the captured
// epoch. Output identities are known before scopes, classifiers or constructors.
func validateDefinitionMetadata(d *registryDeclarations) error {
	catalog, err := events.NewCatalog(d.descriptors...)
	if err != nil {
		return err
	}
	if err := catalog.ValidateDeclarations(); err != nil {
		return err
	}
	models, err := readmodels.NewCatalog(d.readModels...)
	if err != nil {
		return err
	}
	for _, factory := range d.projectionFactories {
		model, ok := models.LookupIdentifier(factory.model.Identifier())
		if !ok || model != factory.model {
			return invalidFactory("projection factory model is not registered in this store")
		}
		for _, reducer := range d.reducers {
			if reducer.Model() == model || string(reducer.Identifier()) == factory.id {
				return invalidFactory("projection factory conflicts with reducer")
			}
		}
	}
	derived, err := constraints.CompileDeclarations(catalog)
	if err != nil {
		return err
	}
	for _, factory := range d.constraintFactories {
		for _, name := range factory.names {
			for _, definition := range derived {
				if definition.Name() == name {
					return invalidFactory("factory and model-bound constraint names conflict")
				}
			}
		}
	}
	for _, factory := range d.migrationFactories {
		for _, endpoint := range []events.Descriptor{factory.upgrade, factory.previous} {
			registered, ok := catalog.LookupRef(endpoint.Ref())
			if !ok || !registered.SameDeclaration(endpoint) {
				return invalidFactory("migration factory endpoint is not registered in this store")
			}
		}
	}
	return nil
}

// prepareRegistrySchemas freezes every base plan before dependent factory or
// composition callbacks. The client runs this phase for ALL captured registries
// before compiling any one registry's definitions.
func prepareRegistrySchemas(captured *registryDeclarations, policy serialization.NamingPolicy) (registrySchemas, error) {
	return prepareRegistrySchemasWithSink(captured, policy, readmodels.MongoDB)
}

func prepareRegistrySchemasWithSink(captured *registryDeclarations, policy serialization.NamingPolicy, sink readmodels.SinkType) (schemas registrySchemas, err error) {
	err = artifacts.Protect("registry", "schemas", func() error {
		eventTypes := make([]events.Descriptor, len(captured.descriptors))
		for i, event := range captured.descriptors {
			var compileErr error
			eventTypes[i], compileErr = event.WithNamingPolicy(policy)
			if compileErr != nil {
				return compileErr
			}
		}
		var compileErr error
		schemas.events, compileErr = events.NewCatalog(eventTypes...)
		if compileErr != nil {
			return compileErr
		}
		models := make([]readmodels.Descriptor, len(captured.readModels))
		for i, model := range captured.readModels {
			models[i], compileErr = model.WithNamingPolicy(policy)
			if compileErr != nil {
				return compileErr
			}
			models[i], compileErr = models[i].WithDefaultSinkType(sink)
			if compileErr != nil {
				return compileErr
			}
		}
		schemas.models, compileErr = readmodels.NewCatalog(models...)
		return compileErr
	})
	return schemas, err
}
