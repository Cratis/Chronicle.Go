// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

type enumCaseEvent struct {
	Status projectionEnum `json:"status"`
}
type enumIntegerCaseEvent struct {
	Status int32 `json:"status"`
}
type enumAmbiguousCaseEvent struct {
	Status projectionEnum
	Other  int32 `json:"status"`
}
type enumUnicodeCaseEvent struct {
	Status projectionEnum `json:"ſtatus"`
}
type enumAmbiguousCaseModel struct {
	ID    string
	Value projectionEnum `json:"Status"`
	Other int32          `json:"status"`
}
type enumCaseModel struct {
	ID     string `json:"ID"`
	Status projectionEnum
}
type enumVariantModel struct {
	ID     string `json:"ID" chronicle:"key"`
	Status projectionEnum
	Ready  bool
}
type enumKeyModel struct {
	ID    projectionEnum `json:"ID"`
	Ready bool
}
type enumBoundKeyModel struct {
	ID    projectionEnum `json:"ID" chronicle:"key"`
	Ready bool
}
type enumGlobalEvent struct {
	Status int32 `json:"status"`
	Ready  bool
}
type enumGlobalModel struct {
	Ready bool `chronicle:"set(enumGlobalEvent)"`
}

func enumBoundaryFailure(t *testing.T, err error, path string) {
	t.Helper()
	var located *projections.DeclarationError
	if !errors.Is(err, chronicle.ErrInvalidConfiguration) || !errors.As(err, &located) || located.Artifact == "" {
		t.Fatalf("want located invalid declaration, got %v", err)
	}
	if path != "" && located.Path != path {
		t.Fatalf("error path = %q, want %q", located.Path, path)
	}
}

func enumCaseMapping[E any](t *testing.T, event events.Type[E], join bool) error {
	t.Helper()
	b := projections.NewBuilder("enum-case", mustModel[enumCaseModel](t, readmodels.WithCodecs(projectionEnumCodecs(t, false))))
	if join {
		projections.Join(b, event, projections.Path[enumCaseModel, string]("ID"), nil)
	} else {
		projections.From(b, event, nil)
	}
	_, err := b.Build()
	return err
}

func TestEnumAutoMapCaseInsensitiveProfiles(t *testing.T) {
	for _, join := range []bool{false, true} {
		name := "From"
		if join {
			name = "Join"
		}
		t.Run(name, func(t *testing.T) {
			t.Run("same profile", func(t *testing.T) {
				if err := enumCaseMapping(t, mustEvent[enumCaseEvent](t, events.WithCodecs(projectionEnumCodecs(t, false))), join); err != nil {
					t.Fatal(err)
				}
			})
			t.Run("plain integer", func(t *testing.T) {
				enumBoundaryFailure(t, enumCaseMapping(t, mustEvent[enumIntegerCaseEvent](t), join), "Status")
			})
			t.Run("incompatible table", func(t *testing.T) {
				enumBoundaryFailure(t, enumCaseMapping(t, mustEvent[enumCaseEvent](t, events.WithCodecs(projectionEnumCodecs(t, true))), join), "Status")
			})
			t.Run("ambiguous event names", func(t *testing.T) {
				enumBoundaryFailure(t, enumCaseMapping(t, mustEvent[enumAmbiguousCaseEvent](t, events.WithCodecs(projectionEnumCodecs(t, false))), join), "Status")
			})
			t.Run("unqualified Unicode comparison", func(t *testing.T) {
				enumBoundaryFailure(t, enumCaseMapping(t, mustEvent[enumUnicodeCaseEvent](t, events.WithCodecs(projectionEnumCodecs(t, false))), join), "Status")
			})
		})
	}
}

func TestEnumAutoMapAmbiguousModelNames(t *testing.T) {
	c := projectionEnumCodecs(t, false)
	b := projections.NewBuilder("enum-ambiguous-model", mustModel[enumAmbiguousCaseModel](t, readmodels.WithCodecs(c)))
	projections.From(b, mustEvent[enumCaseEvent](t, events.WithCodecs(c)), nil)
	_, err := b.Build()
	enumBoundaryFailure(t, err, "Status")
}

func TestEnumVariantEntersOnFinalAutoMap(t *testing.T) {
	c := projectionEnumCodecs(t, false)
	bad := mustEvent[enumIntegerCaseEvent](t)
	good := mustEvent[enumCaseEvent](t, events.WithCodecs(c))
	for _, bound := range []bool{false, true} {
		name := "fluent"
		if bound {
			name = "model-bound"
		}
		t.Run(name, func(t *testing.T) {
			model := mustModel[enumVariantModel](t, readmodels.WithCodecs(c))
			if bound {
				catalog, err := events.NewCatalog(bad.Descriptor())
				if err != nil {
					t.Fatal(err)
				}
				_, err = projections.Compile(projections.ModelBound(model, projections.VariantOf[WorkItem](), projections.EntersOn(bad)), catalog)
				enumBoundaryFailure(t, err, "Status")
			} else {
				b := projections.NewBuilder("enum-enter", model, projections.VariantOf[WorkItem](), projections.VariantKey(projections.Path[enumVariantModel, string]("ID")), projections.EntersOn(bad))
				_, err := b.Build()
				enumBoundaryFailure(t, err, "Status")
			}
			b := projections.NewBuilder("enum-enter-good", model, projections.VariantOf[WorkItem](), projections.VariantKey(projections.Path[enumVariantModel, string]("ID")), projections.EntersOn(good))
			if _, err := b.Build(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEnumVariantGlobalFinalAutoMap(t *testing.T) {
	c := projectionEnumCodecs(t, false)
	entered := mustEvent[enumCaseEvent](t, events.WithCodecs(c))
	updated := mustEvent[enumGlobalEvent](t)
	catalog, err := events.NewCatalog(entered.Descriptor(), updated.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	global, err := projections.Global[enumGlobalModel](projections.GlobalFor[WorkItem]())
	if err != nil {
		t.Fatal(err)
	}
	variant := projections.ModelBound(mustModel[enumVariantModel](t, readmodels.WithCodecs(c)), projections.VariantOf[WorkItem](), projections.EntersOn(entered))
	_, err = projections.CompileGroup([]projections.Declaration{variant, global}, catalog)
	enumBoundaryFailure(t, err, "Status")
}

func TestEnumVariantKeyBothFrontEndsRefuse(t *testing.T) {
	c := projectionEnumCodecs(t, false)
	entered := mustEvent[IssueCreated](t)
	updated := mustEvent[TitleChanged](t)
	b := projections.NewBuilder("enum-key", mustModel[enumKeyModel](t, readmodels.WithCodecs(c)), projections.NoAutoMap(), projections.VariantOf[WorkItem](), projections.VariantKey(projections.Path[enumKeyModel, projectionEnum]("ID")), projections.EntersOn(entered))
	projections.From(b, updated, nil)
	_, err := b.Build()
	enumBoundaryFailure(t, err, "ID")
	catalog, err := events.NewCatalog(entered.Descriptor(), updated.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	_, err = projections.Compile(projections.ModelBound(mustModel[enumBoundKeyModel](t, readmodels.WithCodecs(c)), projections.NoAutoMap(), projections.VariantOf[WorkItem](), projections.EntersOn(entered), projections.FromEvent(updated)), catalog)
	enumBoundaryFailure(t, err, "ID")
}
