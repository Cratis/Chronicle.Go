// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"fmt"

	"github.com/cratis/chronicle.go/readmodels"
)

// RegisterReadModel explicitly declares a typed model schema, identity, sink and
// indexes. Registration does not create a producer or materialize an instance.
// Duplicate Go types/identifiers fail atomically. NewClient freezes declarations.
func RegisterReadModel[T any](registry *Registry, options ...readmodels.ModelOption) (readmodels.Model[T], error) {
	if registry == nil {
		return readmodels.Model[T]{}, fmt.Errorf("%w: nil registry", ErrInvalidConfiguration)
	}
	model, err := readmodels.Define[T](options...)
	if err != nil {
		return readmodels.Model[T]{}, err
	}
	descriptor := model.Descriptor()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, existing := range registry.readModels {
		if existing.GoType() == descriptor.GoType() || existing.Identifier() == descriptor.Identifier() {
			return readmodels.Model[T]{}, fmt.Errorf("%w: duplicate read model", ErrInvalidConfiguration)
		}
	}
	registry.readModels = append(registry.readModels, descriptor)
	return model, nil
}
