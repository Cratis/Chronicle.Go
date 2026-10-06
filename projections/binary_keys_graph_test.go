// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestBinaryRebindRechecksEveryKeyLikePath(t *testing.T) {
	model, err := readmodels.Define[graphBinaryModel]()
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.Define[graphBinaryModel]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	path := expression{kind: pathExpression, text: "Payload"}
	composite := expression{kind: compositeExpression, parts: []keyPart{{name: "Part", expression: path}}}
	ref := event.Descriptor().Ref()
	for _, tc := range []struct {
		name string
		node nodeDefinition
	}{
		{"model key and variant key", nodeDefinition{keyField: "Payload"}},
		{"identifiedBy", nodeDefinition{identifiedBy: "Payload"}},
		{"from key", nodeDefinition{from: []fromDefinition{{event: ref, key: path}}}},
		{"from parent", nodeDefinition{from: []fromDefinition{{event: ref, parent: path}}}},
		{"composite key", nodeDefinition{from: []fromDefinition{{event: ref, key: composite}}}},
		{"composite parent", nodeDefinition{from: []fromDefinition{{event: ref, parent: composite}}}},
		{"removal key", nodeDefinition{removals: []removalDefinition{{event: ref, key: path}}}},
		{"removal parent", nodeDefinition{removals: []removalDefinition{{event: ref, parent: path}}}},
		{"removal composite", nodeDefinition{removals: []removalDefinition{{event: ref, key: composite}}}},
		{"join key", nodeDefinition{joins: []joinDefinition{{fromDefinition: fromDefinition{event: ref, key: path}}}}},
		{"join parent", nodeDefinition{joins: []joinDefinition{{fromDefinition: fromDefinition{event: ref, parent: path}}}}},
		{"join target", nodeDefinition{joins: []joinDefinition{{fromDefinition: fromDefinition{event: ref}, on: "Payload"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Seed an otherwise inadmissible graph so the defense-in-depth Rebind
			// check is exercised independently of plan and authoring admission.
			tc.node.noAuto = true
			d := Definition{data: &definition{id: "binary-key-rebind", model: model.Descriptor(), initialState: "{}", nodeDefinition: tc.node}}
			_, err := d.Rebind(model.Descriptor(), catalog, catalog)
			if !errors.Is(err, faults.ErrUnsupported) {
				t.Fatalf("binary key graph admitted: %v", err)
			}
		})
	}
}

type graphDottedStringEvent struct {
	Alias string `json:"Data.Payload"`
}
type graphDottedStringModel struct {
	Alias string                   `json:"Data.Payload"`
	Data  struct{ Payload string } `chronicle:"no-auto"`
}

func TestBinaryAutoMapChecksGeneratedTargetCandidates(t *testing.T) {
	event, err := events.Define[graphDottedStringEvent]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[graphDottedStringModel]()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := readmodels.Define[graphBinaryModel]()
	if err != nil {
		t.Fatal(err)
	}
	for _, aliasFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "alias first", false: "alias last"}[aliasFirst], func(t *testing.T) {
			fields := model.Descriptor().Fields()
			// Detached metadata simulates a binary candidate behind the string
			// alias. Such a plan itself is now refused; test the consumer too.
			leaf := binary.Descriptor().Fields()[0]
			leaf.Path, leaf.GoField, leaf.Index = "Data.Payload", "Data.Payload", []int{1, 0}
			fields[2] = leaf
			if !aliasFirst {
				fields[0], fields[2] = fields[2], fields[0]
			}
			n := nodeDefinition{exclusions: []string{"Data"}, from: []fromDefinition{{event: event.Descriptor().Ref()}}}
			d := &definition{id: "binary-automap-candidates", model: model.Descriptor(), nodeDefinition: n}
			if err := validateBinaryNode(d, &n, fields, catalog, false, false); !errors.Is(err, faults.ErrUnsupported) {
				t.Fatalf("generated target hid binary: %v", err)
			}
		})
	}
}
