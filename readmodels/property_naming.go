// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"slices"

	"github.com/cratis/chronicle.go/serialization"
)

// WithNamingPolicy compiles detached property metadata for registry composition.
// Container names and declaration identity are unchanged; existing typed handles
// remain valid when used with the resulting client catalog.
func (d Descriptor) WithNamingPolicy(policy serialization.NamingPolicy) (Descriptor, error) {
	if d.definition == nil {
		return Descriptor{}, invalid("model required")
	}
	plan, err := serialization.CompileReadModel(d.GoType(), policy)
	if err != nil {
		return Descriptor{}, err
	}
	copy := *d.definition
	copy.plan = plan
	copy.config.indexes = slices.Clone(copy.config.indexes)
	copy.config.pii = slices.Clone(copy.config.pii)
	for _, paths := range [][]string{copy.config.indexes, copy.config.pii} {
		for i, path := range paths {
			paths[i], err = serialization.RebindPath(path, d.Fields(), plan.Fields())
			if err != nil {
				return Descriptor{}, err
			}
		}
	}
	copy.config.subject, err = serialization.RebindPath(copy.config.subject, d.Fields(), plan.Fields())
	if err != nil {
		return Descriptor{}, err
	}
	copy.schema, err = modelSchema(plan.Schema(), copy.config)
	if err != nil {
		return Descriptor{}, err
	}
	return Descriptor{definition: &copy}, nil
}
