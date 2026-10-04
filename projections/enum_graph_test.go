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
