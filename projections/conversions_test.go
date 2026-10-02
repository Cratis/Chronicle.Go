// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/projections/internal/first"
	"github.com/cratis/chronicle.go/projections/internal/second"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/fundamentals.go/concepts"
	"github.com/google/uuid"
)

type conversionEvent[T any] struct{ Value T }
type conversionModel[T any] struct {
	Value T `chronicle:"set(@changed,from=Value)"`
}
type autoConversionModel[T any] struct{ Value T }

func checkConversion[S, T any](t *testing.T) {
	t.Helper()
	for _, auto := range []bool{false, true} {
		r := chronicle.NewRegistry()
		e, err := chronicle.RegisterEvent[conversionEvent[S]](r)
		if err != nil {
			t.Fatal(err)
		}
		if auto {
			m, err := chronicle.RegisterReadModel[autoConversionModel[T]](r, readmodels.WithIdentifier("auto-model"), readmodels.WithContainerName("Models"))
			if err != nil {
				t.Fatal(err)
			}
			err = r.AddProjection(projections.ModelBound(m, projections.FromEvent(e)))
			if err != nil {
				t.Fatal(err)
			}
		} else {
			m, err := chronicle.RegisterReadModel[conversionModel[T]](r, readmodels.WithIdentifier("model"), readmodels.WithContainerName("Models"))
			if err != nil {
				t.Fatal(err)
			}
			err = r.AddProjection(projections.ModelBound(m, projections.BindEvent("changed", e)))
			if err != nil {
				t.Fatal(err)
			}
		}
		c, err := chronicle.NewClient(chronicle.WithRegistry(r))
		if err != nil {
			t.Fatalf("auto=%v: %v", auto, err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProjectionAcceptsKernelScalarAndObjectConversions(t *testing.T) {
	t.Run("int32 to int", checkConversion[int32, int])
	t.Run("float32 to float64", checkConversion[float32, float64])
	t.Run("integer to number", checkConversion[int32, float64])
	t.Run("google uuid to string", checkConversion[uuid.UUID, string])
	t.Run("shared uuid to string", checkConversion[concepts.UUID, string])
	t.Run("concept to string", checkConversion[conceptfixtures.AuthorID, string])
	t.Run("nullable to string", checkConversion[*string, string])
	t.Run("objects in different packages", checkConversion[first.Opened, second.Opened])
}

type contextConversionModel struct {
	Value       string
	Correlation string `chronicle:"context(@changed,from=correlationId)"`
}

func TestContextOnlySubscriptionDoesNotPrevalidateAutoMap(t *testing.T) {
	r := chronicle.NewRegistry()
	e, err := chronicle.RegisterEvent[conversionEvent[int32]](r)
	if err != nil {
		t.Fatal(err)
	}
	m, err := chronicle.RegisterReadModel[contextConversionModel](r)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AddProjection(projections.ModelBound(m, projections.BindEvent("changed", e))); err != nil {
		t.Fatal(err)
	}
	c, err := chronicle.NewClient(chronicle.WithRegistry(r))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTypedConceptPathsPreserveDeclaredDomainIdentity(t *testing.T) {
	r := chronicle.NewRegistry()
	e, err := chronicle.RegisterEvent[conversionEvent[conceptfixtures.AuthorID]](r)
	if err != nil {
		t.Fatal(err)
	}
	m, err := chronicle.RegisterReadModel[autoConversionModel[conceptfixtures.AuthorID]](r, readmodels.WithIdentifier("ids"), readmodels.WithContainerName("IDs"))
	if err != nil {
		t.Fatal(err)
	}
	// UUID and AuthorID have the same wire representation, not the same declared type.
	if err := r.AddProjection(projections.ModelBound(m, projections.FromEvent(e, projections.UsingKey(projections.Path[conversionEvent[conceptfixtures.AuthorID], concepts.UUID]("Value"))))); err != nil {
		t.Fatal(err)
	}
	if c, err := chronicle.NewClient(chronicle.WithRegistry(r)); err == nil {
		_ = c.Close()
		t.Fatal("typed identity mismatch accepted")
	}
}
