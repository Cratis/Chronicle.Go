// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type graphEnum int32
type graphEnumModel struct{ Value graphEnum }
type graphEvent struct{ Ready bool }

func TestFinalEnumGraphRefusesSparseLiteralWithAnyHandlerShape(t *testing.T) {
	codecs, err := serialization.NewCodecs(serialization.Enum(serialization.EnumMember[graphEnum]{Name: "Zero", Value: 0}))
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[graphEnumModel](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.Define[graphEvent]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	for _, handler := range []string{"none", "From", "Join"} {
		t.Run(handler, func(t *testing.T) {
			n := nodeDefinition{noAuto: true, all: []write{{path: "Value", expression: expression{kind: pathExpression, text: "true"}}}}
			from := fromDefinition{event: event.Descriptor().Ref()}
			if handler == "From" {
				n.from = []fromDefinition{from}
			}
			if handler == "Join" {
				n.joins = []joinDefinition{{fromDefinition: from}}
			}
			d := &definition{id: "sparse-final", model: model.Descriptor(), nodeDefinition: n}
			err := validateEnumGraph(d, catalog)
			var located *DeclarationError
			if !errors.Is(err, faults.ErrInvalidConfiguration) || !errors.As(err, &located) || located.Path != "Value" {
				t.Fatalf("sparse final graph: %v", err)
			}
		})
	}
}

func TestFinalEnumGraphRefusesWildcardEveryRegardlessOfExpression(t *testing.T) {
	codecs, err := serialization.NewCodecs(serialization.Enum(serialization.EnumMember[graphEnum]{Name: "Zero", Value: 0}))
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[graphEnumModel](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []expressionKind{pathExpression, literalExpression, nullExpression} {
		d := &definition{id: "wildcard-final", model: model.Descriptor(), initialState: "{}", subscribesAll: true,
			nodeDefinition: nodeDefinition{noAuto: true, all: []write{{path: "Value", expression: expression{kind: kind, text: "Value"}}}}}
		// Rebind clones node mappings and must retain actual root wildcard metadata.
		_, reboundErr := (Definition{data: d}).Rebind(model.Descriptor(), catalog, catalog)
		for _, err := range []error{validateEnumGraph(d, catalog), reboundErr} {
			var located *DeclarationError
			if !errors.Is(err, faults.ErrInvalidConfiguration) || !errors.As(err, &located) || located.Path != "Value" || located.GoField != "Value" || located.Directive != "every" {
				t.Fatalf("wildcard final graph kind %d: %v", kind, err)
			}
		}
		if kind == pathExpression {
			d.subscribesAll = false
			if err := validateEnumGraph(d, catalog); err != nil {
				t.Fatalf("handlerless Every without wildcard: %v", err)
			}
		}
	}
}

func TestFinalEnumGraphRefusesGeneratedJoinEvenWithoutAutoMap(t *testing.T) {
	codecs, err := serialization.NewCodecs(serialization.Enum(serialization.EnumMember[graphEnum]{Name: "Zero", Value: 0}))
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[graphEnumModel](readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.Define[graphEvent]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	// Generated joins bypass addJoin. Seed the already-lowered graph directly
	// to specify the independent final guard, rather than only VariantKey's guard.
	d := &definition{id: "generated-enum-join", model: model.Descriptor(), nodeDefinition: nodeDefinition{noAuto: true, joins: []joinDefinition{{on: "Value", fromDefinition: fromDefinition{event: event.Descriptor().Ref()}}}}}
	err = validateEnumGraph(d, catalog)
	var located *DeclarationError
	if !errors.Is(err, faults.ErrInvalidConfiguration) || !errors.As(err, &located) || located.Path != "Value" || located.EventReference == "" {
		t.Fatalf("generated enum join: %v", err)
	}
}
