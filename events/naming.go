// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import "github.com/cratis/chronicle.go/serialization"

// WithNamingPolicy compiles a detached descriptor for registry composition. It
// does not mutate this descriptor, its handles, or any existing catalog.
func (d Descriptor) WithNamingPolicy(policy serialization.NamingPolicy) (Descriptor, error) {
	plan, err := serialization.Compile(d.typ, policy)
	if err != nil {
		return Descriptor{}, err
	}
	d.plan = plan
	return d.compileDeclarations()
}
