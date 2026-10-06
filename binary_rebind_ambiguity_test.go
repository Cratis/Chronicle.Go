// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type binaryKeyAliasFirst struct {
	Alias string `json:"data.payload"`
	Data  struct{ Payload []byte }
}
type binaryKeyAliasLast struct {
	Data  struct{ Payload []byte }
	Alias string `json:"data.payload"`
}
type binaryRebindKey struct{ Part string }
type binaryRebindModel struct{ Name string }

func TestBinaryProjectionRebindAmbiguityRefusesBeforeIO(t *testing.T) {
	t.Run("alias first", testBinaryProjectionRebindAmbiguity[binaryKeyAliasFirst])
	t.Run("alias last", testBinaryProjectionRebindAmbiguity[binaryKeyAliasLast])
}

func testBinaryProjectionRebindAmbiguity[E any](t *testing.T) {
	t.Helper()
	event, err := events.Define[E]()
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[binaryRebindModel]()
	if err != nil {
		t.Fatal(err)
	}
	before, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	t.Run("NewClient", func(t *testing.T) {
		r := chronicle.NewRegistry()
		e, err := chronicle.RegisterEvent[E](r)
		if err != nil {
			t.Fatal(err)
		}
		m, err := chronicle.RegisterReadModel[binaryRebindModel](r)
		if err != nil {
			t.Fatal(err)
		}
		b := projections.NewBuilder("binary-client-rebind", m, projections.NoAutoMap())
		projections.From(b, e, nil, projections.UsingKey(projections.Path[E, string]("data.payload")))
		d, err := b.Build()
		if err != nil {
			t.Fatal(err)
		}
		if err := r.AddProjection(d); err != nil {
			t.Fatal(err)
		}
		client, err := chronicle.NewClient(chronicle.WithRegistry(r), chronicle.WithNamingPolicy(serialization.CamelCase))
		if client != nil {
			t.Cleanup(func() {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		if !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatalf("client observed an ambiguous binary plan: %v", err)
		}
	})
	key := projections.UsingKey(projections.Path[E, string]("data.payload"))
	parent := projections.UsingParentKey(projections.Path[E, string]("data.payload"))
	composite := projections.UsingCompositeKey(func(b *projections.CompositeKeyBuilder[binaryRebindKey, E]) {
		projections.KeyPart(b, projections.Path[binaryRebindKey, string]("Part"), projections.Path[E, string]("data.payload"))
	})
	compositeParent := projections.UsingCompositeParentKey(func(b *projections.CompositeKeyBuilder[binaryRebindKey, E]) {
		projections.KeyPart(b, projections.Path[binaryRebindKey, string]("Part"), projections.Path[E, string]("data.payload"))
	})
	for _, tc := range []struct {
		name      string
		configure func(*projections.Builder[binaryRebindModel])
	}{
		{"key", func(b *projections.Builder[binaryRebindModel]) { projections.From(b, event, nil, key) }},
		{"parent", func(b *projections.Builder[binaryRebindModel]) { projections.From(b, event, nil, parent) }},
		{"composite", func(b *projections.Builder[binaryRebindModel]) { projections.From(b, event, nil, composite) }},
		{"composite parent", func(b *projections.Builder[binaryRebindModel]) { projections.From(b, event, nil, compositeParent) }},
		{"removal key", func(b *projections.Builder[binaryRebindModel]) { b.Configure(projections.RemovedWith(event, key)) }},
		{"removal parent", func(b *projections.Builder[binaryRebindModel]) { b.Configure(projections.RemovedWith(event, parent)) }},
		{"join correlation", func(b *projections.Builder[binaryRebindModel]) {
			projections.Join(b, event, projections.Path[binaryRebindModel, string]("Name"), nil, key)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := projections.NewBuilder("binary-rebind-ambiguity", model, projections.NoAutoMap())
			tc.configure(b)
			declaration, err := b.Build()
			if err != nil {
				t.Fatal(err)
			}
			definition, err := projections.Compile(declaration, before)
			if err != nil {
				t.Fatal(err)
			}
			named, err := event.Descriptor().WithNamingPolicy(serialization.CamelCase)
			if err == nil {
				after, catalogErr := events.NewCatalog(named)
				if catalogErr != nil {
					t.Fatal(catalogErr)
				}
				_, err = definition.Rebind(model.Descriptor(), before, after)
			}
			if !errors.Is(err, chronicle.ErrUnsupported) && !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("binary correlation ambiguity admitted: %v", err)
			}
		})
	}
}

func TestBinaryModelNamingAmbiguityRefusesBeforeIO(t *testing.T) {
	t.Run("alias first", testBinaryModelNamingAmbiguity[binaryNamedAliasFirst])
	t.Run("alias last", testBinaryModelNamingAmbiguity[binaryNamedAliasLast])
}
func testBinaryModelNamingAmbiguity[M any](t *testing.T) {
	t.Helper()
	for _, tc := range []struct {
		name    string
		options []readmodels.ModelOption
	}{
		{"payload", nil},
		{"index", []readmodels.ModelOption{readmodels.WithIndexes("Data.Payload")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			model, err := readmodels.Define[M](tc.options...)
			if err != nil {
				t.Fatal(err)
			}
			_, err = model.Descriptor().WithNamingPolicy(serialization.CamelCase)
			if !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatalf("ambiguous model naming admitted: %v", err)
			}
		})
	}
}

type binaryAutoMapAliasFirst struct {
	Alias string `json:"Data.Payload"`
	Data  struct{ Payload []byte }
}
type binaryAutoMapAliasLast struct {
	Data  struct{ Payload []byte }
	Alias string `json:"Data.Payload"`
}
type stringAutoMapAliasFirst struct {
	Alias string                   `json:"Data.Payload"`
	Data  struct{ Payload string } `chronicle:"no-auto"`
}
type stringAutoMapAliasLast struct {
	Data  struct{ Payload string } `chronicle:"no-auto"`
	Alias string                   `json:"Data.Payload"`
}

func TestBinaryAutoMapAliasCandidatesRefuseBeforeIO(t *testing.T) {
	t.Run("alias first", testBinaryAutoMapAliasCandidates[stringAutoMapAliasFirst, binaryAutoMapAliasFirst])
	t.Run("alias last", testBinaryAutoMapAliasCandidates[stringAutoMapAliasLast, binaryAutoMapAliasLast])
}
func testBinaryAutoMapAliasCandidates[M, E any](t *testing.T) {
	t.Helper()
	for _, join := range []bool{false, true} {
		t.Run(map[bool]string{true: "Join", false: "From"}[join], func(t *testing.T) {
			event, err := events.Define[E]()
			if err == nil {
				model, modelErr := readmodels.Define[M]()
				if modelErr != nil {
					t.Fatal(modelErr)
				}
				b := projections.NewBuilder("binary-automap-alias", model)
				if join {
					projections.Join(b, event, projections.Path[M, string]("Data.Payload"), nil)
				} else {
					projections.From(b, event, nil)
				}
				_, err = b.Build()
			}
			if !errors.Is(err, chronicle.ErrUnsupported) && !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("binary alias AutoMap admitted: %v", err)
			}
		})
	}
}
