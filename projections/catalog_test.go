// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/projections/internal/first"
	"github.com/cratis/chronicle.go/projections/internal/second"
)

type AmbiguousModel struct {
	Name string `json:"name" chronicle:"set(Opened)"`
}

func TestAmbiguousReferencesListQualifiedCandidates(t *testing.T) {
	one := mustEvent[first.Opened](t, events.WithID("one"))
	two := mustEvent[second.Opened](t, events.WithID("two"))
	catalog, err := events.NewCatalog(two.Descriptor(), one.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	_, err = projections.Compile(projections.ModelBound(mustModel[AmbiguousModel](t)), catalog)
	var declaration *projections.DeclarationError
	if !errors.As(err, &declaration) || !strings.Contains(err.Error(), "first.Opened") || !strings.Contains(err.Error(), "second.Opened") || declaration.EventReference != "Opened" {
		t.Fatalf("%v", err)
	}
}

type UpperModel struct {
	Name string `json:"DisplayName"`
}
type UpperEvent struct {
	Name string `json:"PayloadName"`
}

func TestExactSerializedPathsAndNilOptions(t *testing.T) {
	event := mustEvent[UpperEvent](t)
	model := mustModel[UpperModel](t)
	builder := projections.NewBuilder("upper", model)
	projections.From(builder, event, func(from *projections.FromBuilder[UpperModel, UpperEvent]) {
		projections.Map(from, projections.Path[UpperModel, string]("DisplayName"), projections.Path[UpperEvent, string]("PayloadName"))
	})
	declaration, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if got := mustCompile(t, declaration, event.Descriptor()).KernelDefinition().From[0].Value.Properties["DisplayName"]; got != "PayloadName" {
		t.Fatal("compiler changed serialized spelling")
	}
	for _, declaration := range []projections.Declaration{
		projections.ModelBound(model, nil, projections.FromEvent(event)),
		projections.ModelBound(model, projections.FromEvent(event, nil, projections.UsingConstantKey("later"))),
	} {
		catalog, err := events.NewCatalog(event.Descriptor())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = projections.Compile(declaration, catalog); err == nil {
			t.Fatal("nil option silently overwritten")
		}
	}
}

type NullableUnsigned struct {
	Value *uint64 `json:"value"`
}

func TestPointerLiteralsRespectKernelIntegerRange(t *testing.T) {
	event := mustEvent[Opened](t)
	builder := projections.NewBuilder("unsigned", mustModel[NullableUnsigned](t))
	value := uint64(1) << 63
	projections.From(builder, event, func(from *projections.FromBuilder[NullableUnsigned, Opened]) {
		projections.Value(from, projections.Path[NullableUnsigned, *uint64]("value"), &value)
	})
	if _, err := builder.Build(); err == nil {
		t.Fatal("pointer bypassed kernel integer range")
	}
}
