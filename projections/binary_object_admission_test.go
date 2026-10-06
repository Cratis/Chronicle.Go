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

type binaryObjectCopy struct{ Blob struct{ Inner []byte } }
type binaryValueObjectCopy struct {
	Blob struct {
		Value []byte `json:"value"`
	}
}

func TestBinaryObjectAutoMapRefusesEvenIdenticalRepresentations(t *testing.T) {
	event, err := events.Define[binaryObjectCopy]()
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[graphBinaryModel]()
	if err != nil {
		t.Fatal(err)
	}
	for _, join := range []bool{false, true} {
		for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase} {
			e, err := event.Descriptor().WithNamingPolicy(policy)
			if err != nil {
				t.Fatal(err)
			}
			bound, err := events.NewCatalog(e)
			if err != nil {
				t.Fatal(err)
			}
			from := fromDefinition{event: e.Ref()}
			n := nodeDefinition{}
			if join {
				n.joins = []joinDefinition{{fromDefinition: from}}
			} else {
				n.from = []fromDefinition{from}
			}
			d := &definition{id: "binary-object-copy", model: model.Descriptor(), nodeDefinition: n}
			// Seed detached object fields: read-model admission is independently
			// narrowed to root leaves, but the final graph must also reject objects.
			if err := validateBinaryNode(d, &n, e.Fields(), bound, false, false); !errors.Is(err, faults.ErrInvalidConfiguration) {
				t.Fatalf("whole-object binary AutoMap admitted (join=%v, policy=%v): %v", join, policy, err)
			}
		}
	}
	// The public path also refuses the single-value shape from review five.
	if _, err := events.Define[binaryValueObjectCopy](); !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("single-value object admitted: %v", err)
	}
	if _, err := readmodels.Define[binaryObjectCopy](); !errors.Is(err, faults.ErrUnsupported) {
		t.Fatalf("nested read-model binary admitted: %v", err)
	}
}

func TestBinarySingleValueWholeObjectAutoMapRefusesBeforeRegistration(t *testing.T) {
	// Express the complete public workflow, while accepting the earlier plan
	// refusal that now prevents either artifact from reaching the builder.
	event, err := events.Define[binaryValueObjectCopy]()
	if errors.Is(err, faults.ErrUnsupported) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[binaryValueObjectCopy]()
	if errors.Is(err, faults.ErrUnsupported) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	builder := NewBuilder("binary-single-value", model)
	From(builder, event, nil)
	if _, err := builder.Build(); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("Blob{value []byte} whole-object AutoMap admitted: %v", err)
	}
}

func TestBinaryFreeWholeObjectAutoMapRemainsAdmitted(t *testing.T) {
	type ordinary struct {
		Blob struct {
			Value string `json:"value"`
		}
	}
	event, err := events.Define[ordinary]()
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[ordinary]()
	if err != nil {
		t.Fatal(err)
	}
	builder := NewBuilder("ordinary-value", model)
	From(builder, event, nil)
	if _, err := builder.Build(); err != nil {
		t.Fatal(err)
	}
}
