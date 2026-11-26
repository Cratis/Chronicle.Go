// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	"github.com/cratis/chronicle.go/projections"
	"github.com/google/uuid"
)

type guidControlKey struct {
	Item string `json:"item"`
}
type guidDirectKey struct {
	Item   string               `json:"item"`
	Labels map[uuid.UUID]string `json:"labels"`
}
type guidNestedKey struct {
	Item  string `json:"item"`
	Inner struct {
		Rows []map[string]map[uuid.UUID]string `json:"rows"`
	} `json:"inner"`
}

// guidIgnoredKey carries UUID maps the serializer never emits (ignored and
// unexported), so they cannot affect the identity.
type guidIgnoredKey struct {
	Item   string               `json:"item"`
	Hidden map[uuid.UUID]int    `json:"-"`
	hidden map[uuid.UUID]string //nolint:unused // serializer skips unexported fields
	Nested struct {
		Name    string                            `json:"name"`
		Skipped []map[string]map[uuid.UUID]string `json:"-"`
	} `json:"nested"`
}

func buildGuidCompositeKey[K any](t *testing.T, parent bool) error {
	t.Helper()
	define := func(b *projections.CompositeKeyBuilder[K, ItemAdded]) {
		projections.KeyPart(b, projections.Path[K, string]("item"), projections.Path[ItemAdded, string]("itemId"))
	}
	option := projections.UsingCompositeKey(define)
	if parent {
		option = projections.UsingCompositeParentKey(define)
	}
	builder := projections.NewBuilder("guid-key", mustModel[CompositeFluent](t))
	projections.From(builder, mustEvent[ItemAdded](t), nil, option)
	_, err := builder.Build()
	return err
}

// The event/document plan admits map[uuid.UUID]string, but a correlation
// identity must not carry one, in any serialized member.
func TestCompositeKeysRefuseUUIDDictionaries(t *testing.T) {
	for _, parent := range []bool{false, true} {
		if err := buildGuidCompositeKey[guidControlKey](t, parent); err != nil {
			t.Fatalf("control key (parent=%v): %v", parent, err)
		}
		if err := buildGuidCompositeKey[guidIgnoredKey](t, parent); err != nil {
			t.Fatalf("ignored/unexported UUID maps rejected (parent=%v): %v", parent, err)
		}
		if err := buildGuidCompositeKey[guidDirectKey](t, parent); err == nil {
			t.Fatalf("direct UUID dictionary admitted in key (parent=%v)", parent)
		}
		if err := buildGuidCompositeKey[guidNestedKey](t, parent); err == nil {
			t.Fatalf("nested UUID dictionary admitted in key (parent=%v)", parent)
		}
	}
}
