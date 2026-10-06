// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/cratis/chronicle.go/internal/faults"
)

// Property is a dotted Go field path rooted in T, not a JSON path. NewClient
// resolves every segment through the serialization plan, honoring json tags and
// the client's naming policy. Collection traversal is not a property path.
// String literals are accepted directly by the typed builders.
type Property[T any] string

// Migration declares an adjacent event evolution. Both direction callbacks are
// required; an empty callback is an explicit identity transformation. Unmapped
// properties pass through in the kernel and are filtered by the target schema.
// MapValues, when supplied, runs before direction-specific operations, so the
// last directional operation on a property wins. No callback runs on payloads.
// Binary and declared serialization.Enum/Flags endpoints are not supported; authoring and
// catalog compilation reject them before generic JSON can bypass their codecs.
type Migration[Upgrade, Previous any] struct {
	Upcast    func(*MigrationBuilder[Upgrade, Previous])
	Downcast  func(*MigrationBuilder[Previous, Upgrade])
	MapValues func(*ValueMapBuilder[Upgrade, Previous])
}

// ValueMapping declares a previous/source value followed by its upgraded/target
// value. Values use their JSON representation (including ordinary named integers
// and Fundamentals concepts). Inputs are serialized and copied during registration.
// Closed enum codec endpoints are explicitly unsupported.
type ValueMapping struct{ From, To any }

type valueMapping struct {
	From json.RawMessage `json:"from"`
	To   json.RawMessage `json:"to"`
}
type migrationOperation struct {
	target, source, kind, separator string
	sources                         []string
	part                            int
	value                           json.RawMessage
	mappings                        []valueMapping
}
type migrationOperations struct {
	operations []migrationOperation
	err        error
}

// MigrationBuilder describes one direction. Use the builder passed to a Migration
// callback; it is not concurrency-safe. Repeated targets use the last operation.
type MigrationBuilder[Target, Source any] struct{ state migrationOperations }

// RenamedFrom copies a source property to its new target name.
func (b *MigrationBuilder[T, S]) RenamedFrom(target Property[T], source Property[S]) *MigrationBuilder[T, S] {
	b.state.operations = append(b.state.operations, migrationOperation{kind: "$rename", target: string(target), source: string(source)})
	return b
}

// DefaultValue supplies value only when the target is absent, not present-null.
// Object literals must already use their serialized JSON names, as in C#.
func (b *MigrationBuilder[T, S]) DefaultValue(target Property[T], value any) *MigrationBuilder[T, S] {
	data, err := json.Marshal(value)
	if err != nil {
		b.state.err = fmt.Errorf("%w: invalid migration default", faults.ErrInvalidConfiguration)
	}
	b.state.operations = append(b.state.operations, migrationOperation{kind: "$defaultValue", target: string(target), value: data})
	return b
}

// Split selects a zero-based part of a source string separated by separator.
func (b *MigrationBuilder[T, S]) Split(target Property[T], source Property[S], separator string, part int) *MigrationBuilder[T, S] {
	if part < 0 {
		b.state.err = fmt.Errorf("%w: split part must be nonnegative", faults.ErrInvalidConfiguration)
	}
	b.state.operations = append(b.state.operations, migrationOperation{kind: "$split", target: string(target), source: string(source), separator: separator, part: part})
	return b
}

// Combine joins source properties in the supplied order using separator.
func (b *MigrationBuilder[T, S]) Combine(target Property[T], separator string, sources ...Property[S]) *MigrationBuilder[T, S] {
	paths := make([]string, len(sources))
	for i, source := range sources {
		paths[i] = string(source)
	}
	b.state.operations = append(b.state.operations, migrationOperation{kind: "$combine", target: string(target), sources: paths, separator: separator})
	return b
}

// MapValues translates one direction. Unlisted values carry across unchanged.
// It overrides a shared map on the same target, even when declared with no pairs.
func (b *MigrationBuilder[T, S]) MapValues(target Property[T], source Property[S], mappings ...ValueMapping) *MigrationBuilder[T, S] {
	b.state.addMap(string(target), string(source), mappings)
	return b
}
func (s *migrationOperations) addMap(target, source string, mappings []ValueMapping) {
	pairs := make([]valueMapping, 0, len(mappings))
	for _, pair := range mappings {
		from, fromErr := json.Marshal(pair.From)
		to, toErr := json.Marshal(pair.To)
		if fromErr != nil || toErr != nil {
			s.err = fmt.Errorf("%w: invalid migration value map", faults.ErrInvalidConfiguration)
			return
		}
		pairs = append(pairs, valueMapping{from, to})
	}
	s.operations = append(s.operations, migrationOperation{kind: "$mapValues", target: target, source: source, mappings: pairs})
}

// ValueMapBuilder declares shared maps. Downcast inversion chooses the first
// previous value declared for each upgraded value, matching C# lossy inversion.
type ValueMapBuilder[Upgrade, Previous any] struct{ state migrationOperations }

// For declares a bidirectional map for the two generation properties.
func (b *ValueMapBuilder[U, P]) For(upgrade Property[U], previous Property[P], mappings ...ValueMapping) *ValueMapBuilder[U, P] {
	b.state.addMap(string(upgrade), string(previous), mappings)
	return b
}

// MigrationDeclaration is immutable authoring metadata. Build with DefineMigration
// or chronicle.RegisterEventMigration; its zero value is invalid.
type MigrationDeclaration struct {
	upgrade, previous Descriptor
	upcast, downcast  []migrationOperation
}

// Upgrade returns the exact declared target generation descriptor.
func (d MigrationDeclaration) Upgrade() Descriptor { return d.upgrade }

// Previous returns the exact declared source generation descriptor.
func (d MigrationDeclaration) Previous() Descriptor { return d.previous }

// DefineMigration snapshots callbacks and values without resolving catalog paths.
// Both callbacks are mandatory; registration errors never partially admit a map.
func DefineMigration[U, P any](upgrade Type[U], previous Type[P], migration Migration[U, P]) (MigrationDeclaration, error) {
	if migration.Upcast == nil || migration.Downcast == nil {
		return MigrationDeclaration{}, fmt.Errorf("%w: both migration directions are required", faults.ErrInvalidConfiguration)
	}
	if err := rejectEnumMigration(upgrade.Descriptor(), previous.Descriptor()); err != nil {
		return MigrationDeclaration{}, err
	}
	if err := rejectBinaryMigration(upgrade.Descriptor(), previous.Descriptor()); err != nil {
		return MigrationDeclaration{}, err
	}
	maps := &ValueMapBuilder[U, P]{}
	if migration.MapValues != nil {
		migration.MapValues(maps)
	}
	up := &MigrationBuilder[U, P]{}
	down := &MigrationBuilder[P, U]{}
	up.state.operations = slices.Clone(maps.state.operations)
	for _, op := range maps.state.operations {
		inverse := op
		inverse.target, inverse.source = op.source, op.target
		inverse.mappings = []valueMapping{}
		seen := map[string]bool{}
		for _, pair := range op.mappings {
			key := string(pair.To)
			if !seen[key] {
				inverse.mappings = append(inverse.mappings, valueMapping{pair.To, pair.From})
				seen[key] = true
			}
		}
		down.state.operations = append(down.state.operations, inverse)
	}
	migration.Upcast(up)
	migration.Downcast(down)
	for _, err := range []error{maps.state.err, up.state.err, down.state.err} {
		if err != nil {
			return MigrationDeclaration{}, err
		}
	}
	return MigrationDeclaration{upgrade.Descriptor(), previous.Descriptor(), slices.Clone(up.state.operations), slices.Clone(down.state.operations)}, nil
}

// MigrationDefinition is a compiled kernel transformation, not executable Go.
// Strings are immutable JSON in the same vocabulary as C# EventMigrationBuilder.
type MigrationDefinition struct {
	EventType                TypeID
	From, To                 Generation
	UpcastJSON, DowncastJSON string
}

// Migrations returns detached definitions in event ID and generation order.
func (c *Catalog) Migrations() []MigrationDefinition { return slices.Clone(c.migrations) }
