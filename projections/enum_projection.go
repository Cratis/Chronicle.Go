// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"slices"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

// AutoMap executes in the kernel, so matching enum members must be checked even
// when no explicit Go write was produced. Ordinary non-enum mapping is unchanged.
func validateEnumAutoMap(n *nodeDefinition, fields []serialization.Field, catalog *events.Catalog) error {
	if n.noAuto {
		return nil
	}
	froms := slices.Clone(n.from)
	for _, join := range n.joins {
		froms = append(froms, join.fromDefinition)
	}
	for _, from := range froms {
		event, ok := catalog.LookupRef(from.event)
		if !ok {
			return invalid("enum auto-map event is not registered")
		}
		for _, target := range serialization.RootFields(fields) {
			if slices.Contains(n.exclusions, target.Path) || hasWrite(from.writes, target.Path) {
				continue
			}
			source, exists := serialization.FieldAt(event.Fields(), target.Name)
			if exists && (source.IsEnum() || target.IsEnum()) && !target.SameRepresentation(source) {
				return invalid("auto-map enum profiles must match exactly")
			}
		}
	}
	return nil
}

func sameEventEnumProfiles(a, b events.Descriptor) bool {
	for _, f := range a.Fields() {
		other, exists := serialization.FieldAt(b.Fields(), f.Path)
		if (f.IsEnum() || other.IsEnum()) && (!exists || !f.SameRepresentation(other)) {
			return false
		}
	}
	return true
}
