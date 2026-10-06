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
	"github.com/cratis/chronicle.go/serialization"
)

type binarySerializedID struct {
	Key []byte `json:"id"`
}
type binarySerializedPascalID struct {
	Key []byte `json:"Id"`
}

func TestBinarySerializedIdentitiesRefuse(t *testing.T) {
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		for name, build := range map[string]func() error{
			"id": func() error {
				declared, err := readmodels.Define[binarySerializedID]()
				if err != nil {
					return err
				}
				model, err := declared.Descriptor().WithNamingPolicy(policy)
				if err != nil {
					t.Fatal(err)
				}
				event := mustEvent[binaryMappedEvent](t)
				catalog, err := events.NewCatalog(event.Descriptor())
				if err != nil {
					t.Fatal(err)
				}
				_, err = projections.Compile(projections.ModelBoundDescriptor(model, projections.NoAutoMap(), projections.FromEvent(event)), catalog)
				return err
			},
			"Id": func() error {
				declared, err := readmodels.Define[binarySerializedPascalID]()
				if err != nil {
					return err
				}
				model, err := declared.Descriptor().WithNamingPolicy(policy)
				if err != nil {
					t.Fatal(err)
				}
				event := mustEvent[binaryMappedEvent](t)
				catalog, err := events.NewCatalog(event.Descriptor())
				if err != nil {
					t.Fatal(err)
				}
				_, err = projections.Compile(projections.ModelBoundDescriptor(model, projections.NoAutoMap(), projections.FromEvent(event)), catalog)
				return err
			},
		} {
			t.Run(name, func(t *testing.T) { binaryMappingFailure(t, build()) })
		}
	}
}

type binaryUnicodeEvent struct {
	Key     string
	Payload []byte `json:"é"`
}
type binaryUnicodeStringEvent struct {
	Key     string
	Payload string `json:"é"`
}
type binaryUnicodeModel struct {
	Key     string
	Payload []byte `json:"É"`
}
type binaryUnicodeStringModel struct {
	Key     string
	Payload string `json:"É"`
}

func TestBinaryUnicodeAutoMapRefusesBothDirectionsAndJoins(t *testing.T) {
	for _, join := range []bool{false, true} {
		t.Run(map[bool]string{false: "From", true: "Join"}[join], func(t *testing.T) {
			toString := projections.NewBuilder("binary-unicode-string", mustModel[binaryUnicodeStringModel](t))
			if join {
				projections.Join(toString, mustEvent[binaryUnicodeEvent](t), projections.Path[binaryUnicodeStringModel, string]("Key"), nil)
			} else {
				projections.From(toString, mustEvent[binaryUnicodeEvent](t), nil)
			}
			_, err := toString.Build()
			binaryMappingFailure(t, err)
			toBinary := projections.NewBuilder("binary-unicode-bytes", mustModel[binaryUnicodeModel](t))
			if join {
				projections.Join(toBinary, mustEvent[binaryUnicodeStringEvent](t), projections.Path[binaryUnicodeModel, string]("Key"), nil)
			} else {
				projections.From(toBinary, mustEvent[binaryUnicodeStringEvent](t), nil)
			}
			_, err = toBinary.Build()
			binaryMappingFailure(t, err)
		})
	}
}

type binaryTaggedKeyModel struct {
	Payload []byte `chronicle:"key"`
}
type binaryCompositeKey struct{ Payload []byte }
type binaryNestedOwner struct{ Nested *binaryStringModel }

func TestBinaryInitialValuesRefuse(t *testing.T) {
	for name, option := range map[string]projections.Option{
		"scalar":      projections.WithInitialValue(projections.Path[binaryMappedModel, []byte]("Payload"), []byte{1}),
		"whole model": projections.WithInitialValues(binaryMappedModel{Payload: []byte{1}}),
	} {
		t.Run(name, func(t *testing.T) {
			b := projections.NewBuilder("binary-initial", mustModel[binaryMappedModel](t), projections.NoAutoMap(), option)
			projections.From(b, mustEvent[binaryMappedEvent](t), nil)
			_, err := b.Build()
			binaryMappingFailure(t, err)
		})
	}
}

func TestBinaryAdditionalProjectionBoundariesRefuse(t *testing.T) {
	t.Run("key tag", func(t *testing.T) {
		catalog, err := events.NewCatalog()
		if err != nil {
			t.Fatal(err)
		}
		model, err := readmodels.Define[binaryTaggedKeyModel]()
		if errors.Is(err, chronicle.ErrUnsupported) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		_, err = projections.Compile(projections.ModelBound(model, projections.FromEvent(mustEvent[binaryMappedEvent](t))), catalog)
		binaryMappingFailure(t, err)
	})
	t.Run("composite key part", func(t *testing.T) {
		key := projections.UsingCompositeKey(func(k *projections.CompositeKeyBuilder[binaryCompositeKey, binaryMappedEvent]) {
			projections.KeyPart(k, projections.Path[binaryCompositeKey, []byte]("Payload"), projections.Path[binaryMappedEvent, []byte]("Payload"))
		})
		b := projections.NewBuilder("binary-composite", mustModel[binaryMappedModel](t), projections.NoAutoMap())
		projections.From(b, mustEvent[binaryMappedEvent](t), nil, key)
		_, err := b.Build()
		binaryMappingFailure(t, err)
	})
	t.Run("variant key", func(t *testing.T) {
		b := projections.NewBuilder("binary-variant", mustModel[binaryMappedModel](t), projections.NoAutoMap(), projections.VariantOf[WorkItem](), projections.VariantKey(projections.Path[binaryMappedModel, []byte]("Payload")), projections.EntersOn(mustEvent[binaryMappedEvent](t)))
		_, err := b.Build()
		binaryMappingFailure(t, err)
	})
	t.Run("all events", func(t *testing.T) {
		b := projections.NewBuilder("binary-all", mustModel[binaryMappedModel](t), projections.NoAutoMap())
		projections.All(b, func(e *projections.EveryBuilder[binaryMappedModel]) {
			projections.EveryMap(e, projections.Path[binaryMappedModel, []byte]("Payload"), "Payload")
		})
		_, err := b.Build()
		binaryMappingFailure(t, err)
	})
	t.Run("nested mapping", func(t *testing.T) {
		b := projections.NewBuilder("binary-nested", mustModel[binaryNestedOwner](t))
		projections.Nested(b, projections.Path[binaryNestedOwner, *binaryStringModel]("Nested"), func(n *projections.Builder[binaryStringModel]) {
			projections.From(n, mustEvent[binaryMappedEvent](t), nil)
		})
		_, err := b.Build()
		binaryMappingFailure(t, err)
	})
}
