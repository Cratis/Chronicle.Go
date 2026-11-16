// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
)

func TestBinaryValidationReportsMissingEventsThroughoutGraph(t *testing.T) {
	catalog, err := events.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	missing := fromDefinition{event: events.TypeRef{}}
	for _, tc := range []struct {
		name string
		node nodeDefinition
	}{
		{"from", nodeDefinition{from: []fromDefinition{missing}}},
		{"removal", nodeDefinition{removals: []removalDefinition{{event: missing.event}}}},
		{"join", nodeDefinition{joins: []joinDefinition{{fromDefinition: missing}}}},
		{"child", nodeDefinition{children: map[string]*nodeDefinition{"Items": {from: []fromDefinition{missing}}}}},
		{"nested", nodeDefinition{nested: map[string]*nodeDefinition{"Details": {from: []fromDefinition{missing}, inheritAuto: true}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &definition{id: "missing-binary-event", nodeDefinition: tc.node}
			err := validateBinaryNode(d, &tc.node, nil, catalog, false, false)
			if !errors.Is(err, faults.ErrInvalidConfiguration) {
				t.Fatalf("missing event error = %v", err)
			}
			if err.Error() != invalid("binary mapping event is not registered").Error() {
				t.Fatalf("unexpected missing event error: %v", err)
			}
		})
	}
}
