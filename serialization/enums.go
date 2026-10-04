// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"cmp"
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/fundamentals.go/concepts"
)

// EnumMember declares one case-sensitive wire name and its exact Int32 value.
// Names must contain only ASCII letters. Aliases and case-insensitive name
// collisions are rejected by NewCodecs. No zero member is synthesized.
type EnumMember[T ~int32] struct {
	Name  string // Name is the unchanged C# enum member name, not a property name.
	Value T      // Value is the declared numeric value, including explicit zero or -1.
}

// Enum declares a closed Int32 enum for a caller-defined named Go type. It copies
// members immediately; NewCodecs validates and owns a second immutable copy.
// Built-in int32 (including aliases to it), empty tables and aliases are invalid.
// Writes are numeric; reads accept declared numbers and fixture-qualified names.
// Only ordinary root DTO members T, *T and []T are supported. Custom JSON/text
// codecs and String methods are never invoked. See Documentation/enum-codecs.md.
func Enum[T ~int32](members ...EnumMember[T]) Codec {
	return declareEnum(false, members)
}

// Flags declares the same closed Int32 profile as Enum, marked as flags for
// representation identity. Comma-separated names must resolve to a declared
// value. All=-1 does not authorize undeclared combinations or unknown bits.
func Flags[T ~int32](members ...EnumMember[T]) Codec {
	return declareEnum(true, members)
}

type enumMember struct {
	name  string
	value int32
}

type enumDefinition struct {
	flags   bool
	members []enumMember
	values  map[int32]bool
	names   map[string]int32
}

func declareEnum[T ~int32](flags bool, members []EnumMember[T]) Codec {
	definition := &enumDefinition{flags: flags, members: make([]enumMember, len(members))}
	for i, member := range members {
		definition.members[i] = enumMember{member.Name, int32(member.Value)}
	}
	return Codec{kind: enumCodec, concrete: reflect.TypeFor[T](), enum: definition}
}

func compileEnum(r Codec) (*enumDefinition, error) {
	if r.concrete == nil || r.concrete.Kind() != reflect.Int32 || r.concrete.PkgPath() == "" || r.enum == nil || len(r.enum.members) == 0 {
		return nil, codecError(r, "", "named Int32 type and nonempty enum members required")
	}
	if _, concept, err := concepts.Underlying(r.concrete); concept || err != nil {
		return nil, codecError(r, "", "enum concepts are not supported")
	}
	definition := &enumDefinition{flags: r.enum.flags, members: slices.Clone(r.enum.members), values: map[int32]bool{}, names: map[string]int32{}}
	for _, member := range definition.members {
		if !enumName(member.name) {
			return nil, codecError(r, "", "enum names must contain only ASCII letters")
		}
		key := strings.ToLower(member.name)
		if _, exists := definition.names[key]; exists || definition.values[member.value] {
			return nil, codecError(r, "", "duplicate enum name or value")
		}
		definition.names[key], definition.values[member.value] = member.value, true
	}
	// Enum.GetNames/GetValues order Int32 values by their unsigned magnitude.
	slices.SortFunc(definition.members, func(a, b enumMember) int { return cmp.Compare(uint32(a.value), uint32(b.value)) })
	return definition, nil
}

func enumName(name string) bool {
	if name == "" {
		return false
	}
	for _, c := range name {
		if c < 'A' || c > 'Z' {
			if c < 'a' || c > 'z' {
				return false
			}
		}
	}
	return true
}

func (c *Codecs) enumeration(typ reflect.Type) *enumDefinition {
	if c == nil {
		return nil
	}
	return c.enums[typ]
}

func (e *enumDefinition) schema() map[string]any {
	values, names := make([]int32, len(e.members)), make([]string, len(e.members))
	for i, member := range e.members {
		values[i], names[i] = member.value, member.name
	}
	return map[string]any{"type": "integer", "enum": values, "x-enumNames": names}
}

func sameEnum(a, b *enumDefinition) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.flags == b.flags && slices.Equal(a.members, b.members)
}

// IsEnum reports whether this field is a declared Int32 enum, nullable enum or
// enum array. It consults the frozen codec, not the underlying Go integer kind.
func (f Field) IsEnum() bool { return hasEnum(f.plan) }

func hasEnum(n *node) bool {
	found := false
	if n != nil {
		_ = visitNodes(n, map[*node]bool{}, func(child *node) error {
			found = found || child.enum != nil
			return nil
		})
	}
	return found
}

// Admission is deliberately root-member-only. Walking every graph edge also
// catches registered enums in unused derivatives and nested collection shapes.
func validateEnumPlacement(root *node) error {
	for _, f := range root.fields {
		if !hasEnum(f.value) {
			continue
		}
		n := f.value
		if n.typ.Kind() == reflect.Pointer || n.typ.Kind() == reflect.Slice {
			n = n.item
		}
		if n == nil || n.enum == nil || len(f.index) != 1 || f.omitEmpty || f.omitZero {
			return unsupported(f.value.typ, "enum profile requires direct root T, *T or []T members without omission tags")
		}
	}
	return nil
}
