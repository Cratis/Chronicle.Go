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

type graphBinaryModel struct{ Payload []byte }
type graphBinaryUnicodeModel struct {
	Payload []byte
}
type graphStringUnicodeModel struct {
	Payload string `json:"É"`
}
type graphBinaryUnicodeEvent struct {
	Payload []byte
}
type graphStringUnicodeEvent struct {
	Payload string `json:"é"`
}

func TestBinaryNamingRebindRevalidatesFinalGraph(t *testing.T) {
	for name, check := range map[string]func(*testing.T){
		"binary to string": func(t *testing.T) { checkBinaryUnicodeRebind[graphStringUnicodeModel, graphBinaryUnicodeEvent](t) },
		"string to binary": func(t *testing.T) { checkBinaryUnicodeRebind[graphBinaryUnicodeModel, graphStringUnicodeEvent](t) },
	} {
		t.Run(name, check)
	}
}

func checkBinaryUnicodeRebind[M, E any](t *testing.T) {
	t.Helper()
	model, err := readmodels.Define[M]()
	if err != nil {
		t.Fatal(err)
	}
	event, err := events.Define[E]()
	if err != nil {
		t.Fatal(err)
	}
	before, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	for _, join := range []bool{false, true} {
		t.Run(map[bool]string{false: "From", true: "Join"}[join], func(t *testing.T) {
			n := nodeDefinition{}
			from := fromDefinition{event: event.Descriptor().Ref()}
			if join {
				n.joins = []joinDefinition{{fromDefinition: from}}
			} else {
				n.from = []fromDefinition{from}
			}
			// Only the binary-free peer may still declare Unicode names. Seed
			// a lowered graph to exercise Rebind independently of admission.
			d := &definition{id: "binary-rebind-unicode", model: model.Descriptor(), initialState: "{}", nodeDefinition: n}
			bound, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
			if err != nil {
				t.Fatal(err)
			}
			e, err := event.Descriptor().WithNamingPolicy(serialization.CamelCase)
			if err != nil {
				t.Fatal(err)
			}
			after, err := events.NewCatalog(e)
			if err != nil {
				t.Fatal(err)
			}
			_, err = (Definition{data: d}).Rebind(bound, before, after)
			if !errors.Is(err, faults.ErrUnsupported) {
				t.Fatalf("rebound Unicode binary mapping admitted: %v", err)
			}
		})
	}
}

func TestBinaryAllEventGraphRefusesUnknownRepresentations(t *testing.T) {
	model, err := readmodels.Define[graphBinaryModel]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []expressionKind{pathExpression, literalExpression, nullExpression} {
		d := &definition{id: "binary-all-final", model: model.Descriptor(), initialState: "{}", subscribesAll: true, nodeDefinition: nodeDefinition{noAuto: true, all: []write{{path: "Payload", expression: expression{kind: kind, text: "Payload"}}}}}
		if err := validateBinaryGraph(d, catalog); !errors.Is(err, faults.ErrUnsupported) {
			t.Fatalf("binary All graph admitted: %v", err)
		}
	}
}
