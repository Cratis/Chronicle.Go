// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"fmt"

	"github.com/cratis/chronicle.go/events"
)

// RegisterEventGeneration declares Previous as a historical shape of current.
// The current handle must belong to this registry. Historical values can be
// appended and handled, but never introduce another current event for the ID.
func RegisterEventGeneration[Previous, Current any](registry *Registry, current events.Type[Current], generation events.Generation, options ...events.TypeOption) (events.Type[Previous], error) {
	if registry == nil {
		return events.Type[Previous]{}, fmt.Errorf("%w: nil registry", ErrInvalidConfiguration)
	}
	declaration, err := events.DefineGeneration[Previous](current, generation, options...)
	if err != nil {
		return events.Type[Previous]{}, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	found := false
	for _, existing := range registry.descriptors {
		if existing.Ref() == current.Ref() && existing.GoType() == current.Descriptor().GoType() {
			found = true
		}
		if existing.GoType() == declaration.Descriptor().GoType() || existing.Ref() == declaration.Ref() {
			return events.Type[Previous]{}, fmt.Errorf("%w: duplicate historical event", ErrInvalidConfiguration)
		}
	}
	if !found {
		return events.Type[Previous]{}, fmt.Errorf("%w: current event is not registered here", ErrInvalidConfiguration)
	}
	registry.descriptors = append(registry.descriptors, declaration.Descriptor())
	return declaration, nil
}

// RegisterEventMigration records both directions between adjacent generations.
// Authoring callbacks run synchronously once; their builders and literal values
// are snapshotted. NewClient validates identity, adjacency, duplicate migrators,
// property paths and (when enabled) the complete chain, before any network I/O.
func RegisterEventMigration[Upgrade, Previous any](registry *Registry, upgrade events.Type[Upgrade], previous events.Type[Previous], migration events.Migration[Upgrade, Previous]) error {
	if registry == nil {
		return fmt.Errorf("%w: nil registry", ErrInvalidConfiguration)
	}
	declaration, err := events.DefineMigration(upgrade, previous, migration)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.migrations = append(registry.migrations, declaration)
	return nil
}
