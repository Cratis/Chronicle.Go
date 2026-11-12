// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"fmt"
	"slices"

	"github.com/cratis/chronicle.go/graphs"
)

// AddGraph admits a graph projection: a projection that fills a property graph of
// nodes and edges, stored by a graph sink such as Neo4j. The graph is registered
// as a read model and as a projection, through the same registration pass as every
// other projection. Duplicate graph names fail immediately; event types, property
// names and conflicts with other artifacts are resolved atomically by NewClient
// against this registry.
func (r *Registry) AddGraph(declaration graphs.Declaration) error {
	if r == nil || declaration.Name() == "" {
		return fmt.Errorf("%w: registry and graph declaration required", ErrInvalidConfiguration)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if slices.ContainsFunc(r.graphs, func(existing graphs.Declaration) bool { return existing.Name() == declaration.Name() }) {
		return fmt.Errorf("%w: duplicate graph %s", ErrInvalidConfiguration, declaration.Name())
	}
	r.graphs = append(r.graphs, declaration)
	return nil
}

// compileGraphs resolves captured graphs against the frozen event catalog and
// refuses a graph that collides with another artifact: a graph is stored as a
// read model and observed as a projection under its name, and its node and edge
// types cannot also be read models.
func compileGraphs(declarations []graphs.Declaration, schemas registrySchemas, snapshot registrySnapshot) ([]graphs.Definition, error) {
	if len(declarations) == 0 {
		return nil, nil
	}
	definitions := make([]graphs.Definition, 0, len(declarations))
	for _, declaration := range declarations {
		for _, typ := range declaration.Types() {
			for _, model := range snapshot.models.Descriptors() {
				if model.GoType() == typ {
					return nil, &graphs.DeclarationError{Graph: declaration.Name(), Detail: typ.String(), Err: graphs.ErrElementIsReadModel}
				}
			}
		}
		definition, err := declaration.Compile(schemas.events, schemas.policy, schemas.graphSink)
		if err != nil {
			return nil, err
		}
		if err := graphNameAvailable(definition.Name(), snapshot, definitions); err != nil {
			return nil, err
		}
		definitions = append(definitions, definition)
	}
	return definitions, nil
}

func graphNameAvailable(name string, snapshot registrySnapshot, earlier []graphs.Definition) error {
	taken := func(what string) error {
		return fmt.Errorf("%w: graph %s collides with %s of the same name", ErrInvalidConfiguration, name, what)
	}
	for _, model := range snapshot.models.Descriptors() {
		if string(model.Identifier()) == name || model.ContainerName() == name {
			return taken("a read model")
		}
	}
	for _, projection := range snapshot.projections {
		if projection.Identifier() == name {
			return taken("a projection")
		}
	}
	for _, plan := range snapshot.reactors {
		if string(plan.Identifier()) == name {
			return taken("a reactor")
		}
	}
	for _, plan := range snapshot.reducers {
		if string(plan.Identifier()) == name {
			return taken("a reducer")
		}
	}
	for _, plan := range snapshot.readModelReactors {
		if string(plan.Identifier()) == name {
			return taken("a read-model reactor")
		}
	}
	for _, existing := range earlier {
		if existing.Name() == name {
			return taken("a graph")
		}
	}
	return nil
}

// registersReadModels reports whether the read-model stage has anything to send.
// A graph is registered as a read model too.
func registersReadModels(snapshot registrySnapshot) bool {
	return len(snapshot.models.Descriptors()) > 0 || len(snapshot.graphs) > 0
}

// registersProjections reports whether the projection stage has anything to send.
func registersProjections(snapshot registrySnapshot) bool {
	return len(snapshot.projections) > 0 || len(snapshot.graphs) > 0
}
