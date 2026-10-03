// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"fmt"
	"sync"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

// Registry is an isolated collection of explicit declarations. Its zero value is
// usable. Registration and snapshots are concurrency-safe. NewClient takes a frozen
// snapshot; adding declarations afterwards does not mutate existing clients.
type Registry struct {
	mu                     sync.Mutex
	descriptors            []events.Descriptor
	migrations             []events.MigrationDeclaration
	constraints            []constraints.Definition
	constraintCompositions []constraintComposition
	readModels             []readmodels.Descriptor
	projections            []projections.Declaration
	reactors               []reactorDeclaration
	reducers               []reducers.Declaration
	seeders                []seederDeclaration
	reactorMiddlewares     []any
	reactorSideEffects     []reactorSideEffectHandler
}

// NewRegistry returns an empty registry; there is no global discovery or init hook.
func NewRegistry() *Registry { return &Registry{} }

// RegisterEvent declares T with a stable ID and positive generation. T must be a
// named struct; appending either T or *T resolves the same declaration. Duplicate
// Go types or current persisted IDs fail without partially modifying the registry.
func RegisterEvent[T any](registry *Registry, options ...events.TypeOption) (events.Type[T], error) {
	if registry == nil {
		return events.Type[T]{}, fmt.Errorf("%w: nil registry", ErrInvalidConfiguration)
	}
	declaration, err := events.Define[T](options...)
	if err != nil {
		return events.Type[T]{}, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	descriptor := declaration.Descriptor()
	for _, existing := range registry.descriptors {
		if existing.GoType() == descriptor.GoType() || existing.Ref().ID == descriptor.Ref().ID {
			return events.Type[T]{}, fmt.Errorf("%w: duplicate event type %s", ErrInvalidConfiguration, descriptor.Ref().ID)
		}
	}
	registry.descriptors = append(registry.descriptors, descriptor)
	return declaration, nil
}

func snapshot(registry *Registry) (*events.Catalog, []constraints.Definition) {
	var descriptors []events.Descriptor
	var definitions []constraints.Definition
	if registry != nil {
		registry.mu.Lock()
		descriptors = append(descriptors, registry.descriptors...)
		definitions = append(definitions, registry.constraints...)
		registry.mu.Unlock()
	}
	// Registry admission enforces all NewCatalog invariants; this cannot fail.
	catalog, _ := events.NewCatalog(descriptors...)
	return catalog, definitions
}
