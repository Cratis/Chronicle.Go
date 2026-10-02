// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"fmt"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
)

// AddConstraint adds a built constraint after all its participating and removal
// event types have been registered. Duplicate names, zero definitions and foreign
// descriptors fail with ErrInvalidConfiguration without changing the registry.
// Group mutually exclusive types in one UniqueEventTypes definition rather than
// adding separate definitions with the same name. Definitions are immutable.
func (r *Registry) AddConstraint(definition constraints.Definition) error {
	if r == nil || definition.Name() == "" {
		return fmt.Errorf("%w: registry and built constraint required", ErrInvalidConfiguration)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, existing := range r.constraints {
		if existing.Name() == definition.Name() {
			return fmt.Errorf("%w: duplicate constraint name %q", ErrInvalidConfiguration, definition.Name())
		}
	}
	for _, descriptor := range append(definition.EventTypes(), definition.RemovalTypes()...) {
		if !containsConstraintEvent(r.descriptors, descriptor) {
			return fmt.Errorf("%w: constraint %q references unregistered event %q", ErrInvalidConfiguration, definition.Name(), descriptor.Ref().ID)
		}
	}
	r.constraints = append(r.constraints, definition)
	return nil
}

func containsConstraintEvent(descriptors []events.Descriptor, target events.Descriptor) bool {
	for _, descriptor := range descriptors {
		if descriptor.GoType() == target.GoType() && descriptor.Ref() == target.Ref() && descriptor.Schema() == target.Schema() {
			return true
		}
	}
	return false
}
