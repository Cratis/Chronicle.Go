// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import (
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/serialization"
	"slices"
)

// WithNamingPolicy compiles a detached descriptor for registry composition. It
// does not mutate this descriptor, its handles, or any existing catalog.
func (d Descriptor) WithNamingPolicy(policy serialization.NamingPolicy) (Descriptor, error) {
	plan, err := d.plan.WithNamingPolicy(policy)
	if err != nil {
		return Descriptor{}, err
	}
	d.protection = slices.Clone(d.protection)
	for i, declaration := range d.protection {
		if declaration.Path() == "" {
			continue
		}
		path, err := serialization.RebindPath(declaration.Path(), d.Fields(), plan.Fields())
		if err != nil {
			return Descriptor{}, err
		}
		d.protection[i] = compliance.Property(path, declaration.Metadata())
	}
	d.plan = plan
	return d.compileDeclarations()
}
