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

type globalNamingFirst struct{ Value string }
type globalNamingSecond struct{ Value string }

func inconsistentGlobalCatalogs(t *testing.T) (*events.Catalog, *events.Catalog) {
	t.Helper()
	first, err := events.Define[globalNamingFirst]()
	if err != nil {
		t.Fatal(err)
	}
	second, err := events.Define[globalNamingSecond]()
	if err != nil {
		t.Fatal(err)
	}
	before, err := events.NewCatalog(first.Descriptor(), second.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	camel, err := second.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	after, err := events.NewCatalog(first.Descriptor(), camel)
	if err != nil {
		t.Fatal(err)
	}
	return before, after
}

func TestRebindRejectsInconsistentGlobalPayloadNaming(t *testing.T) {
	model, err := readmodels.Define[globalNamingFirst]()
	if err != nil {
		t.Fatal(err)
	}
	before, after := inconsistentGlobalCatalogs(t)
	d := Definition{data: &definition{
		id: "global-naming", model: model.Descriptor(), initialState: "{}",
		nodeDefinition: nodeDefinition{all: []write{{path: "Value", expression: expression{kind: pathExpression, text: "Value"}}}},
	}}
	_, err = d.Rebind(model.Descriptor(), before, after)
	if !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("inconsistent naming error = %v", err)
	}
	if err.Error() != invalid("global payload path has inconsistent naming across events").Error() {
		t.Fatalf("unexpected naming error: %v", err)
	}
	if d.data.all[0].expression.text != "Value" {
		t.Fatal("rebind mutated the original global expression")
	}
}
