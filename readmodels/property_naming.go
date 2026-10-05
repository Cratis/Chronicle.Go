// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"slices"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/internal/classifications"
	"github.com/cratis/chronicle.go/serialization"
)

// WithNamingPolicy compiles detached property metadata for registry composition.
// Container names and declaration identity are unchanged; existing typed handles
// remain valid when used with the resulting client catalog. Provider classifications
// are frozen by Go member identity for subsequent naming and producer binding.
func (d Descriptor) WithNamingPolicy(policy serialization.NamingPolicy) (Descriptor, error) {
	if d.definition == nil {
		return Descriptor{}, invalid("model required")
	}
	plan, err := d.definition.plan.WithNamingPolicy(policy)
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
	copy.config.protection = slices.Clone(copy.config.protection)
	for i, declaration := range copy.config.protection {
		if declaration.Path() == "" {
			continue
		}
		path, err := serialization.RebindPath(declaration.Path(), d.Fields(), plan.Fields())
		if err != nil {
			return Descriptor{}, err
		}
		copy.config.protection[i] = compliance.Property(path, declaration.Metadata())
	}
	copy.config.protection, err = classifications.Snapshot(copy.config.protection, func(declarations []compliance.Declaration) error {
		copy.config.protection = declarations
		var schemaErr error
		copy.schema, schemaErr = modelSchema(plan, copy.config)
		return schemaErr
	})
	if err != nil {
		return Descriptor{}, err
	}
	copy.protected, err = serialization.ProtectionRoots(copy.schema)
	if err != nil {
		return Descriptor{}, err
	}
	return Descriptor{definition: &copy}, nil
}
