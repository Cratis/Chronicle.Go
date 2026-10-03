// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

type foldedCollisionVariant struct {
	Value string `json:"_DerivedTypeId"`
}
type foldedCollisionAlias = foldedCollisionVariant
type foldedZeroCollisionVariant struct {
	Value zeroPanic `json:"_DERIVEDTYPEID,omitzero"`
}
type promotedFoldedCollisionVariant struct{ foldedCollisionVariant }
type promotedFoldedPointerCollisionVariant struct{ *foldedCollisionVariant }

func TestDerivedCaseFoldedDiscriminatorPropertiesAreNeverAdmitted(t *testing.T) {
	for name, registration := range map[string]serialization.Codec{
		"exact tag":        serialization.Derived[any, collisionVariant]("private-discriminator"),
		"case folded tag":  serialization.Derived[any, foldedCollisionVariant]("private-discriminator"),
		"concrete alias":   serialization.Derived[any, foldedCollisionAlias]("private-discriminator"),
		"omitzero":         serialization.Derived[any, foldedZeroCollisionVariant]("private-discriminator"),
		"embedded":         serialization.Derived[any, promotedFoldedCollisionVariant]("private-discriminator"),
		"embedded pointer": serialization.Derived[any, promotedFoldedPointerCollisionVariant]("private-discriminator"),
	} {
		t.Run(name, func(t *testing.T) {
			codecs, err := serialization.NewCodecs(registration)
			if err != nil {
				t.Fatal(err)
			}
			for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
				for _, typ := range []reflect.Type{reflect.TypeFor[derivedEnvelope](), reflect.TypeFor[harmlessVariant]()} {
					plan, err := serialization.CompileWith(typ, serialization.Config{Codecs: codecs, NamingPolicy: policy})
					var collision *serialization.CodecError
					if plan != nil || !errors.Is(err, faults.ErrInvalidConfiguration) || !errors.As(err, &collision) || collision.Field == "" || strings.Contains(err.Error(), "private-discriminator") {
						t.Fatalf("policy %v admitted writable discriminator metadata: %v", policy, err)
					}
				}
			}
		})
	}
}
