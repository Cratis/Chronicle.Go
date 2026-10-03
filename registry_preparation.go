// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type registrySchemas struct {
	events *events.Catalog
	models *readmodels.Catalog
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
