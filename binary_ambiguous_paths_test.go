// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type binaryAliasFirst struct {
	Alias string `json:"Data.Payload"`
	Data  struct{ Payload []byte }
}
type binaryAliasLast struct {
	Data  struct{ Payload []byte }
	Alias string `json:"Data.Payload"`
}

type binaryNamedAliasFirst struct {
	Alias []byte `json:"data.payload"`
	Data  struct{ Payload string }
}
type binaryNamedAliasLast struct {
	Data  struct{ Payload string }
	Alias []byte `json:"data.payload"`
}

func binaryConstraintNamingAdmission[T any]() error {
	event, err := events.Define[T]()
	if err != nil {
		return err
	}
	definition, err := constraints.UniqueValues("binary-naming").On(event.Descriptor(), "Data.Payload").Build()
	if err != nil {
		return err
	}
	named, err := event.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		return err
	}
	catalog, err := events.NewCatalog(named)
	if err != nil {
		return err
	}
	_, err = definition.Rebind(catalog)
	return err
}

func TestBinaryConstraintRebindRefusesNewPathAmbiguity(t *testing.T) {
	for name, check := range map[string]func() error{
		"alias first": binaryConstraintNamingAdmission[binaryNamedAliasFirst],
		"alias last":  binaryConstraintNamingAdmission[binaryNamedAliasLast],
	} {
		t.Run(name, func(t *testing.T) {
			if err := check(); !errors.Is(err, chronicle.ErrUnsupported) && !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("naming introduced unguarded binary constraint ambiguity: %v", err)
			}
		})
	}
}

func TestBinaryAmbiguousPathsRefuseBeforeIO(t *testing.T) {
	t.Run("alias first", testBinaryAmbiguousPaths[binaryAliasFirst])
	t.Run("alias last", testBinaryAmbiguousPaths[binaryAliasLast])
}

func testBinaryAmbiguousPaths[T any](t *testing.T) {
	t.Helper()
	event, eventErr := events.Define[T]()
	model, modelErr := readmodels.Define[T]()
	build := func(option projections.Option, configure func(*projections.Builder[T])) error {
		if eventErr != nil {
			return eventErr
		}
		if modelErr != nil {
			return modelErr
		}
		b := projections.NewBuilder("binary-ambiguous", model, projections.NoAutoMap(), option)
		configure(b)
		_, err := b.Build()
		return err
	}
	cases := []struct {
		name  string
		check func() error
	}{
		{"index", func() error { _, err := readmodels.Define[T](readmodels.WithIndexes("Data.Payload")); return err }},
		{"unique", func() error {
			if eventErr != nil {
				return eventErr
			}
			_, err := constraints.UniqueValues("binary-ambiguous").On(event.Descriptor(), "Data.Payload").Build()
			return err
		}},
		{"subject", func() error {
			_, err := readmodels.Define[T](readmodels.WithSubjectProperty("Data.Payload"))
			return err
		}},
		{"initial value", func() error {
			return build(projections.WithInitialValue(projections.Path[T, string]("Data.Payload"), "text"), func(b *projections.Builder[T]) { projections.From(b, event, nil) })
		}},
		{"literal", func() error {
			return build(projections.NoAutoMap(), func(b *projections.Builder[T]) {
				projections.From(b, event, func(f *projections.FromBuilder[T, T]) {
					projections.Value(f, projections.Path[T, string]("Data.Payload"), "text")
				})
			})
		}},
		{"key", func() error {
			return build(projections.NoAutoMap(), func(b *projections.Builder[T]) {
				projections.From(b, event, nil, projections.UsingKey(projections.Path[T, string]("Data.Payload")))
			})
		}},
		{"all-event write", func() error {
			return build(projections.NoAutoMap(), func(b *projections.Builder[T]) {
				projections.All(b, func(f *projections.EveryBuilder[T]) {
					projections.EveryMap(f, projections.Path[T, string]("Data.Payload"), "Name")
				})
			})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.check(); !errors.Is(err, chronicle.ErrUnsupported) && !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("ambiguous binary path admitted before I/O: %v", err)
			}
		})
	}
}
