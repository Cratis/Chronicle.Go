// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"slices"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

// Rebind returns a detached definition whose property paths come exclusively from
// the supplied plans. Authoring is resolved before rebinding, so tags and fluent
// paths use declaration names even when a client selects another naming policy.
// This registry-composition hook requires catalogs of the same declared events.
func (d Definition) Rebind(model readmodels.Descriptor, before, after *events.Catalog) (Definition, error) {
	if d.data == nil || model.GoType() != d.Model().GoType() || before == nil || after == nil {
		return Definition{}, invalid("matching model and event catalogs required")
	}
	copy := *d.data
	copy.model = model
	var err error
	rebindModel := func(path string) string {
		if err != nil {
			return ""
		}
		var result string
		result, err = serialization.RebindPath(path, d.Model().Fields(), model.Fields())
		return result
	}
	copy.keyField = rebindModel(copy.keyField)
	copy.exclusions = slices.Clone(copy.exclusions)
	for i, path := range copy.exclusions {
		copy.exclusions[i] = rebindModel(path)
	}
	copy.provenance = slices.Clone(copy.provenance)
	for i := range copy.provenance {
		copy.provenance[i].Path = rebindModel(copy.provenance[i].Path)
	}
	copy.diagnostics = slices.Clone(copy.diagnostics)
	for i := range copy.diagnostics {
		copy.diagnostics[i].Previous.Path = rebindModel(copy.diagnostics[i].Previous.Path)
		copy.diagnostics[i].Replacement.Path = rebindModel(copy.diagnostics[i].Replacement.Path)
	}
	copy.from = slices.Clone(copy.from)
	for i, from := range copy.from {
		old, oldOK := before.LookupRef(from.event)
		next, nextOK := after.LookupRef(from.event)
		if !oldOK || !nextOK || old.GoType() != next.GoType() {
			return Definition{}, invalid("projection event missing from catalog")
		}
		rebindExpression := func(e expression) expression {
			if e.kind == pathExpression && err == nil {
				e.text, err = serialization.RebindPath(e.text, old.Fields(), next.Fields())
			}
			return e
		}
		copy.from[i].key = rebindExpression(from.key)
		copy.from[i].parent = rebindExpression(from.parent)
		copy.from[i].writes = slices.Clone(from.writes)
		for j, w := range from.writes {
			w.path = rebindModel(w.path)
			w.provenance.Path = rebindModel(w.provenance.Path)
			w.expression = rebindExpression(w.expression)
			copy.from[i].writes[j] = w
		}
	}
	if err != nil {
		return Definition{}, err
	}
	return Definition{data: &copy}, nil
}
