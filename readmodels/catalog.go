// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import "reflect"

// Catalog is an immutable, concurrency-safe collection. Use NewCatalog; the zero
// value is an empty catalog. Descriptors never retain caller-mutable collections.
type Catalog struct {
	ordered     []Descriptor
	types       map[reflect.Type]Descriptor
	identifiers map[Identifier]Descriptor
}

// NewCatalog validates and snapshots declarations, rejecting duplicate types and
// identifiers. A model has one current generation; this is not a migration catalog.
func NewCatalog(descriptors ...Descriptor) (*Catalog, error) {
	c := &Catalog{ordered: append([]Descriptor(nil), descriptors...), types: make(map[reflect.Type]Descriptor), identifiers: make(map[Identifier]Descriptor)}
	for _, d := range descriptors {
		if d.GoType() == nil {
			return nil, invalid("empty read-model descriptor")
		}
		if _, ok := c.types[d.GoType()]; ok {
			return nil, invalid("duplicate read-model Go type")
		}
		if _, ok := c.identifiers[d.Identifier()]; ok {
			return nil, invalid("duplicate read-model identifier")
		}
		c.types[d.GoType()], c.identifiers[d.Identifier()] = d, d
	}
	return c, nil
}

// Descriptors returns a defensive copy in registration order.
func (c *Catalog) Descriptors() []Descriptor { return append([]Descriptor(nil), c.ordered...) }

// Lookup resolves a value or non-nil pointer to a registered model.
func (c *Catalog) Lookup(value any) (Descriptor, bool) {
	if value == nil {
		return Descriptor{}, false
	}
	typ := reflect.TypeOf(value)
	if typ.Kind() == reflect.Pointer {
		if reflect.ValueOf(value).IsNil() {
			return Descriptor{}, false
		}
		typ = typ.Elem()
	}
	return c.LookupType(typ)
}

// LookupType resolves a Go struct type or pointer to it, without constructing a value.
func (c *Catalog) LookupType(typ reflect.Type) (Descriptor, bool) {
	if typ == nil {
		return Descriptor{}, false
	}
	if typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	d, ok := c.types[typ]
	return d, ok
}

// LookupIdentifier resolves the persisted model identity.
func (c *Catalog) LookupIdentifier(id Identifier) (Descriptor, bool) {
	d, ok := c.identifiers[id]
	return d, ok
}
