// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
)

type binaryMigrationLeaf struct{ Payload []byte }
type binaryMigrationNullable struct{ Payload *[]byte }
type binaryMigrationNested struct{ Data *binaryMigrationLeaf }
type binaryMigrationPlain struct{ Payload string }

func TestBinaryMigrationEndpointsRefuseBeforeCallbacks(t *testing.T) {
	t.Run("binary target", testBinaryMigrationEndpoints[binaryMigrationLeaf, binaryMigrationPlain])
	t.Run("binary source", testBinaryMigrationEndpoints[binaryMigrationPlain, binaryMigrationLeaf])
	t.Run("nullable target", testBinaryMigrationEndpoints[binaryMigrationNullable, binaryMigrationPlain])
	t.Run("nullable source", testBinaryMigrationEndpoints[binaryMigrationPlain, binaryMigrationNullable])
	t.Run("nested target", testBinaryMigrationEndpoints[binaryMigrationNested, binaryMigrationPlain])
	t.Run("nested source", testBinaryMigrationEndpoints[binaryMigrationPlain, binaryMigrationNested])
}

func testBinaryMigrationEndpoints[U, P any](t *testing.T) {
	t.Helper()
	upgrade, err := Define[U](WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := DefineGeneration[P](upgrade, 1)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := NewCatalog(upgrade.Descriptor(), previous.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	targetPath, sourcePath := "Payload", "Payload"
	for _, field := range upgrade.Descriptor().Fields() {
		if field.GoField == "Data.Payload" {
			targetPath = field.GoField
		}
	}
	for _, field := range previous.Descriptor().Fields() {
		if field.GoField == "Data.Payload" {
			sourcePath = field.GoField
		}
	}
	for _, operation := range []string{"identity", "$rename", "$defaultValue", "$split", "$combine", "$mapValues", "shared map"} {
		t.Run(operation, func(t *testing.T) {
			target, source := Property[U](targetPath), Property[P](sourcePath)
			called := false
			migration := Migration[U, P]{
				Upcast: func(b *MigrationBuilder[U, P]) {
					called = true
					switch operation {
					case "$rename":
						b.RenamedFrom(target, source)
					case "$defaultValue":
						b.DefaultValue(target, "AQ==")
					case "$split":
						b.Split(target, source, "=", 0)
					case "$combine":
						b.Combine(target, "", source)
					case "$mapValues":
						b.MapValues(target, source, ValueMapping{From: "AQ==", To: "AQ"})
					}
				},
				Downcast: func(*MigrationBuilder[P, U]) { called = true },
			}
			if operation == "shared map" {
				migration.MapValues = func(b *ValueMapBuilder[U, P]) {
					called = true
					b.For(target, source, ValueMapping{From: "AQ==", To: "AQ"})
				}
			}
			if _, err := DefineMigration(upgrade, previous, migration); !errors.Is(err, faults.ErrUnsupported) || called {
				t.Errorf("binary migration authoring admitted callbacks: %v, called=%v", err, called)
			}
			// Catalog admission must revalidate even a previously frozen declaration.
			declaration := MigrationDeclaration{upgrade: upgrade.Descriptor(), previous: previous.Descriptor()}
			if operation != "identity" {
				kind := operation
				if operation == "shared map" {
					kind = "$mapValues"
				}
				declaration.upcast = []migrationOperation{{kind: kind, target: targetPath, source: sourcePath, sources: []string{sourcePath}, value: []byte(`"AQ=="`), separator: "=", part: 0}}
			}
			if _, err := catalog.WithMigrations([]MigrationDeclaration{declaration}, true); !errors.Is(err, faults.ErrUnsupported) {
				t.Errorf("binary migration catalog admitted endpoints: %v", err)
			}
		})
	}
}
