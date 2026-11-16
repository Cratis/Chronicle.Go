// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

type binaryIdentityProtection struct {
	Payload []byte
	Source  events.SourceID
}

func TestProtectionRejectsBinaryBeforeSourceIdentity(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[binaryIdentityProtection]())
	if err != nil {
		t.Fatal(err)
	}
	_, err = plan.ProtectedSchema(compliance.For[binaryIdentityProtection](compliance.Classification{PII: true}))
	var detail *declarations.DeclarationError
	if !errors.As(err, &detail) {
		t.Fatalf("expected declaration error, got %v", err)
	}
	if detail.Message != "protected binary placements are not supported" || detail.Artifact != reflect.TypeFor[binaryIdentityProtection]().String() {
		t.Fatalf("unexpected first failure: %+v", detail)
	}
}

func TestProtectionRejectsDuplicatePropertyBeforeInvalidMetadata(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[struct{ Value string }]())
	if err != nil {
		t.Fatal(err)
	}
	_, err = plan.ProtectedSchema(
		compliance.Property("Value", compliance.Classification{PII: true}),
		compliance.Property("Value", compliance.Classification{PII: true, Encrypted: true}),
	)
	var detail *declarations.DeclarationError
	if !errors.As(err, &detail) {
		t.Fatalf("expected declaration error, got %v", err)
	}
	if detail.Message != "duplicate property classification" {
		t.Fatalf("unexpected first failure: %+v", detail)
	}
}
