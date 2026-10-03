// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/projections"
)

type UniqueCompositeKey struct {
	Item string `json:"item" chronicle:"unique"`
}
type SubjectCompositeKey struct {
	Item string `json:"item" chronicle:"subject"`
}
type IndexCompositeKey struct {
	Item string `json:"item"`
	// Even unused key fields must not silently accept unsupported declarations.
	Other string `json:"other" chronicle:"index"`
}

func assertCompositeDeclarationRejected[K any](t *testing.T, directive, field string) {
	t.Helper()
	define := func(b *projections.CompositeKeyBuilder[K, ItemAdded]) {
		projections.KeyPart(b, projections.Path[K, string]("item"), projections.Path[ItemAdded, string]("itemId"))
	}
	for name, option := range map[string]projections.FromOption{
		"key":    projections.UsingCompositeKey(define),
		"parent": projections.UsingCompositeParentKey(define),
	} {
		t.Run(name, func(t *testing.T) {
			builder := projections.NewBuilder("invalid", mustModel[CompositeFluent](t))
			projections.From(builder, mustEvent[ItemAdded](t), nil, option)
			_, err := builder.Build()
			var declaration *declarations.DeclarationError
			if !errors.As(err, &declaration) || declaration.Directive != directive || declaration.Path != field {
				t.Fatalf("error = %v, want typed %s error on %s", err, directive, field)
			}
		})
	}
}

func TestCompositeKeysRejectEventAndIndexDeclarations(t *testing.T) {
	t.Run("unique", func(t *testing.T) { assertCompositeDeclarationRejected[UniqueCompositeKey](t, "unique", "item") })
	t.Run("subject", func(t *testing.T) { assertCompositeDeclarationRejected[SubjectCompositeKey](t, "subject", "item") })
	t.Run("index", func(t *testing.T) { assertCompositeDeclarationRejected[IndexCompositeKey](t, "index", "other") })
}
