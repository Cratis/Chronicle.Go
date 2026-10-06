// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package constraints

import (
	"fmt"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

// Rebind returns a detached definition using the catalog's compiled property
// names. Paths are resolved by declared field identity, never by recasing text.
// This is a registry-composition hook; the catalog must contain the same events.
func (d Definition) Rebind(catalog *events.Catalog) (Definition, error) {
	resolve := func(old events.Descriptor) (events.Descriptor, error) {
		if catalog != nil {
			if event, ok := catalog.LookupRef(old.Ref()); ok && event.GoType() == old.GoType() {
				return event, nil
			}
		}
		return events.Descriptor{}, fmt.Errorf("%w: constraint event missing from catalog", faults.ErrInvalidConfiguration)
	}
	d.types, d.removers, d.fields = d.EventTypes(), d.RemovalTypes(), d.Fields()
	for _, descriptors := range [][]events.Descriptor{d.types, d.removers} {
		for i, old := range descriptors {
			var err error
			descriptors[i], err = resolve(old)
			if err != nil {
				return Definition{}, err
			}
		}
	}
	for i, fields := range d.fields {
		event, err := resolve(fields.Event)
		if err != nil {
			return Definition{}, err
		}
		for j, path := range fields.Properties {
			d.fields[i].Properties[j], err = serialization.RebindPath(path, fields.Event.Fields(), event.Fields())
			if err != nil {
				return Definition{}, err
			}
			rebound := d.fields[i].Properties[j]
			field, ok := serialization.FieldAtWithCapability(event.Fields(), rebound, serialization.Field.ContainsBinary)
			if ok && field.ContainsBinary() {
				return Definition{}, fmt.Errorf("%w: binary unique property %q is not supported", faults.ErrInvalidConfiguration, rebound)
			}
		}
		d.fields[i].Event = event
	}
	return d, nil
}
