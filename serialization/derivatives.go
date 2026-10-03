// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import "reflect"

// Derivative is detached, variant-qualified metadata. Type preserves the exact
// registered concrete type; ID is the stable discriminator, not a writable field.
// Fields are never flattened into the owning family's ordinary property paths.
type Derivative struct {
	Type reflect.Type
	ID   string
	plan *node
}

// Derivatives returns admitted variants of this field (or its collection element)
// in registration order. The returned slice and field snapshots are caller-owned.
func (f Field) Derivatives() []Derivative {
	n := f.plan
	if n == nil {
		return nil
	}
	for n.item != nil {
		n = n.item
	}
	if n.reference != nil {
		n = n.reference
	}
	result := make([]Derivative, 0, len(n.derivatives))
	for _, d := range n.derivatives {
		result = append(result, Derivative{Type: d.registration.concrete, ID: d.registration.id, plan: d.node})
	}
	return result
}

// Fields returns detached local field metadata for this concrete variant.
func (d Derivative) Fields() []Field { return (Field{plan: d.plan}).Fields() }
