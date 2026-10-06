// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

// WithMigrations compiles a detached catalog with frozen migration definitions.
// Paths, shared identities, adjacency and duplicates are always validated. When
// validateChain is true, every step from generation one to current is required.
// There are no placeholder codecs: both endpoint types must be in this catalog.
func (c *Catalog) WithMigrations(declarations []MigrationDeclaration, validateChain bool) (*Catalog, error) {
	result := *c
	result.migrations = nil
	seen := map[TypeRef]bool{}
	for _, declaration := range declarations {
		from, to := declaration.previous.Ref(), declaration.upgrade.Ref()
		previous, hasPrevious := c.LookupRef(from)
		upgrade, hasUpgrade := c.LookupRef(to)
		if !hasPrevious || !hasUpgrade || previous.typ != declaration.previous.typ || upgrade.typ != declaration.upgrade.typ {
			return nil, fmt.Errorf("%w: migration endpoints must be registered in this store", faults.ErrInvalidConfiguration)
		}
		if err := rejectEnumMigration(upgrade, previous); err != nil {
			return nil, err
		}
		if err := rejectBinaryMigration(upgrade, previous); err != nil {
			return nil, err
		}
		if from.ID != to.ID || from.Generation == 0 || to.Generation <= from.Generation || to.Generation-from.Generation != 1 {
			return nil, fmt.Errorf("%w: migration requires the same ID and adjacent increasing generations", faults.ErrInvalidConfiguration)
		}
		if seen[from] {
			return nil, fmt.Errorf("%w: duplicate migration for %s generation %d", faults.ErrInvalidConfiguration, from.ID, from.Generation)
		}
		seen[from] = true
		upcast, err := compileMigration(declaration.upcast, upgrade, previous)
		if err != nil {
			return nil, err
		}
		downcast, err := compileMigration(declaration.downcast, previous, upgrade)
		if err != nil {
			return nil, err
		}
		result.migrations = append(result.migrations, MigrationDefinition{from.ID, from.Generation, to.Generation, upcast, downcast})
	}
	slices.SortFunc(result.migrations, func(a, b MigrationDefinition) int {
		if order := cmp.Compare(a.EventType, b.EventType); order != 0 {
			return order
		}
		return cmp.Compare(a.From, b.From)
	})
	if validateChain {
		for _, descriptor := range c.ordered {
			if descriptor.IsHistorical() {
				continue
			}
			// Walking the actual definitions bounds validation by the input size, not
			// an untrusted uint32 generation number.
			expected := Generation(1)
			for _, migration := range result.migrations {
				if migration.EventType != descriptor.ref.ID {
					continue
				}
				if migration.From != expected {
					break
				}
				expected = migration.To
			}
			if expected != descriptor.ref.Generation {
				return nil, fmt.Errorf("%w: %s requires migration from generation %d", faults.ErrInvalidConfiguration, descriptor.ref.ID, expected)
			}
		}
	}
	return &result, nil
}

func compileMigration(operations []migrationOperation, target, source Descriptor) (string, error) {
	type propertyExpression struct {
		path  string
		value any
	}
	result := make([]propertyExpression, 0, len(operations))
	positions := map[string]int{}
	for _, operation := range operations {
		targetPath, err := migrationPath(target, operation.target)
		if err != nil {
			return "", err
		}
		sourcePath := ""
		if operation.kind == "$rename" || operation.kind == "$split" || operation.kind == "$mapValues" {
			sourcePath, err = migrationPath(source, operation.source)
			if err != nil {
				return "", err
			}
		}
		var expression any
		switch operation.kind {
		case "$rename":
			expression = sourcePath
		case "$defaultValue":
			expression = operation.value
		case "$split":
			expression = map[string]any{"source": sourcePath, "separator": operation.separator, "part": operation.part}
		case "$combine":
			sources := make([]string, len(operation.sources))
			for i, path := range operation.sources {
				sources[i], err = migrationPath(source, path)
				if err != nil {
					return "", err
				}
			}
			expression = map[string]any{"sources": sources, "separator": operation.separator}
		case "$mapValues":
			expression = map[string]any{"source": sourcePath, "mappings": operation.mappings}
		}
		value := map[string]any{operation.kind: expression}
		if position, ok := positions[targetPath]; ok {
			// C# dictionary assignment replaces the value without moving the
			// target's first insertion position.
			result[position].value = value
		} else {
			positions[targetPath] = len(result)
			result = append(result, propertyExpression{path: targetPath, value: value})
		}
	}
	// The kernel applies overlapping property writes in this order. Marshaling
	// a map would sort targets and change parent/child migration semantics.
	data := []byte{'{'}
	for i, property := range result {
		key, err := json.Marshal(property.path)
		if err != nil {
			return "", fmt.Errorf("%w: invalid migration JSON", faults.ErrInvalidConfiguration)
		}
		value, err := json.Marshal(property.value)
		if err != nil {
			return "", fmt.Errorf("%w: invalid migration JSON", faults.ErrInvalidConfiguration)
		}
		if i > 0 {
			data = append(data, ',')
		}
		data = append(data, key...)
		data = append(data, ':')
		data = append(data, value...)
	}
	return string(append(data, '}')), nil
}

func migrationPath(descriptor Descriptor, path string) (string, error) {
	fields := descriptor.Fields()
	segments := strings.Split(path, ".")
	names := make([]string, 0, len(segments))
	for _, segment := range segments {
		found := false
		for _, field := range serialization.RootFields(fields) {
			if field.GoField != segment {
				continue
			}
			if field.Collection || strings.Contains(field.Name, ".") || field.Name == "" {
				break
			}
			names = append(names, field.Name)
			fields = field.Fields()
			found = true
			break
		}
		if !found {
			return "", fmt.Errorf("%w: migration property %q does not address an object field on %s", faults.ErrInvalidConfiguration, path, descriptor.typ)
		}
	}
	// Fields() exposes collection-element fields for schema tooling; disallow
	// reaching them through a migration's scalar object path.
	resolved := strings.Join(names, ".")
	field, ok := serialization.FieldAt(descriptor.Fields(), resolved)
	if !ok || field.Collection {
		return "", fmt.Errorf("%w: migration cannot traverse a collection", faults.ErrInvalidConfiguration)
	}
	return resolved, nil
}
