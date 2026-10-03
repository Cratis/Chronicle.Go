// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"fmt"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
)

// Catalogs returns immutable event and read-model catalogs selected for a nonblank
// store name. WithRegistryForStore replaces the default registry, not merges it.
// Descriptors use the client's naming policy and store-bound producer metadata,
// exactly as EventStore does. This performs no I/O, creates no namespace or
// observers, and invokes no constructors. It is safe for concurrent use, including
// after Close following successful preparation; catalogs describe frozen
// configuration, not server registration. Before preparation it returns
// ErrNotPrepared (or ErrClosed). A blank prepared store name is invalid.
func (c *Client) Catalogs(store StoreName) (*events.Catalog, *readmodels.Catalog, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requirePreparedLocked("catalogs", true); err != nil {
		return nil, nil, err
	}
	snapshot, err := c.selectedStoreSnapshotLocked(store)
	if err != nil {
		return nil, nil, err
	}
	return snapshot.events, snapshot.models, nil
}

func (c *Client) selectedStoreSnapshot(store StoreName) (registrySnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requirePreparedLocked("catalogs", true); err != nil {
		return registrySnapshot{}, err
	}
	return c.selectedStoreSnapshotLocked(store)
}

// selectedStoreSnapshotLocked binds the already compiled registry; it never compiles
// declarations again. Callers hold c.mu and have checked preparation readiness.
func (c *Client) selectedStoreSnapshotLocked(store StoreName) (registrySnapshot, error) {
	if strings.TrimSpace(string(store)) == "" {
		return registrySnapshot{}, fmt.Errorf("%w: store must be nonblank", ErrInvalidConfiguration)
	}
	snapshot := registrySnapshot{events: c.catalog, models: c.readModelCatalog, constraints: c.constraints, projections: c.projections}
	if selected, ok := c.catalogs[store]; ok {
		snapshot.events, snapshot.models = selected, c.readModelCatalogs[store]
		snapshot.constraints, snapshot.projections = c.storeConstraints[store], c.storeProjections[store]
	}
	snapshot.reactors = c.reactors.defaults
	if selected, ok := c.reactors.stores[store]; ok {
		snapshot.reactors = selected
	}
	snapshot.reactors = slices.Clone(snapshot.reactors)
	for i, plan := range snapshot.reactors {
		snapshot.reactors[i] = plan.ForStore(string(store))
	}
	snapshot.reducers = c.reducers.defaults
	if selected, ok := c.reducers.stores[store]; ok {
		snapshot.reducers = selected
	}
	snapshot.reducers = slices.Clone(snapshot.reducers)
	models := snapshot.models.Descriptors()
	for i, plan := range snapshot.reducers {
		bound, err := plan.ForStore(string(store))
		if err != nil {
			return registrySnapshot{}, err
		}
		snapshot.reducers[i] = bound
		for j, model := range models {
			if model.Identifier() == bound.Model().Identifier() {
				models[j] = bound.Model()
			}
		}
	}
	snapshot.projections = slices.Clone(snapshot.projections)
	for i, definition := range snapshot.projections {
		bound, err := definition.ForStore(string(store))
		if err != nil {
			return registrySnapshot{}, err
		}
		snapshot.projections[i] = bound
		for j, model := range models {
			if model.Identifier() == bound.Model().Identifier() {
				models[j] = bound.Model()
			}
		}
	}
	var err error
	snapshot.models, err = readmodels.NewCatalog(models...)
	if err != nil {
		return registrySnapshot{}, err
	}
	snapshot.readModelReactors = c.readModelReactors.defaults
	if selected, ok := c.readModelReactors.stores[store]; ok {
		snapshot.readModelReactors = selected
	}
	snapshot.readModelReactors = slices.Clone(snapshot.readModelReactors)
	for i, plan := range snapshot.readModelReactors {
		model, ok := snapshot.models.LookupIdentifier(plan.Model().Identifier())
		if !ok {
			return registrySnapshot{}, fmt.Errorf("%w: read-model reactor model missing from selected store", ErrInvalidConfiguration)
		}
		bound, err := plan.ForModel(model)
		if err != nil {
			return registrySnapshot{}, err
		}
		snapshot.readModelReactors[i] = bound
	}
	return snapshot, nil
}
