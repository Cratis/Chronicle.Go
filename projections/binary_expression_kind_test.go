// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	"github.com/cratis/chronicle.go/projections"
)

type binaryExpressionEvent struct {
	Payload []byte
	Subject []byte `json:"subject"`
}
type binaryExpressionModel struct{ Name string }

func TestBinaryExpressionTextDoesNotImplyEventPath(t *testing.T) {
	for _, kind := range []string{"literal", "context"} {
		t.Run(kind, func(t *testing.T) {
			b := projections.NewBuilder("binary-expression", mustModel[binaryExpressionModel](t), projections.NoAutoMap())
			projections.From(b, mustEvent[binaryExpressionEvent](t), func(f *projections.FromBuilder[binaryExpressionModel, binaryExpressionEvent]) {
				target := projections.Path[binaryExpressionModel, string]("Name")
				if kind == "literal" {
					projections.Value(f, target, "Payload")
				} else {
					projections.Context(f, target, "subject")
				}
			})
			if _, err := b.Build(); err != nil {
				t.Fatalf("non-path expression collides with binary property: %v", err)
			}
		})
	}
}
