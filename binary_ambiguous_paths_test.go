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
)

type binaryAliasFirst struct {
	Alias string `json:"Data.Payload"`
	Data  struct{ Payload []byte }
}
type binaryAliasLast struct {
	Data  struct{ Payload []byte }
	Alias string `json:"Data.Payload"`
}

func TestBinaryAmbiguousPathsRefuseBeforeIO(t *testing.T) {
	t.Run("alias first", testBinaryAmbiguousPaths[binaryAliasFirst])
	t.Run("alias last", testBinaryAmbiguousPaths[binaryAliasLast])
}

func testBinaryAmbiguousPaths[T any](t *testing.T) {
	t.Helper()
	event, err := events.Define[T]()
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[T]()
	if err != nil {
		t.Fatal(err)
	}
	build := func(option projections.Option, configure func(*projections.Builder[T])) error {
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
