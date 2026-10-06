// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/readmodels"
)

type modelKeyAliasFirst struct {
	Alias string `json:"data.payload" chronicle:"key"`
	Data  struct{ Payload []byte }
}
type modelKeyAliasLast struct {
	Data  struct{ Payload []byte }
	Alias string `json:"data.payload" chronicle:"key"`
}
type modelVariantAliasFirst struct {
	Alias string `json:"data.payload"`
	Data  struct{ Payload []byte }
}
type modelVariantAliasLast struct {
	Data  struct{ Payload []byte }
	Alias string `json:"data.payload"`
}

func TestBinaryModelKeysRefuseNewNamingAmbiguity(t *testing.T) {
	t.Run("key tag alias first", testBinaryModelKeyNaming[modelKeyAliasFirst])
	t.Run("key tag alias last", testBinaryModelKeyNaming[modelKeyAliasLast])
	t.Run("variant alias first", testBinaryModelKeyNaming[modelVariantAliasFirst])
	t.Run("variant alias last", testBinaryModelKeyNaming[modelVariantAliasLast])
}
func testBinaryModelKeyNaming[M any](t *testing.T) {
	t.Helper()
	// The model must fail before a key/variant projection can be authored,
	// rather than depending on a collision appearing during later rebinding.
	if _, err := readmodels.Define[M](); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("binary model names admitted: %v", err)
	}
}
