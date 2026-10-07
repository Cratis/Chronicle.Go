// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"errors"
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
)

// Scalar classifies the serialized representation, not the declared Go type.
type Scalar uint8

const (
	NotScalar Scalar = iota // NotScalar describes an object or collection.
	String                  // String includes UUID and timestamp formats.
	Boolean                 // Boolean is a JSON boolean.
	Integer                 // Integer is a JSON integer.
	Number                  // Number is a JSON floating-point number.
	Binary                  // Binary is a base64 leaf, not a general-purpose string.
)

// IsPrimitive reports whether a scalar supports ordinary primitive operations.
// Binary requires an explicitly qualified codec path and is excluded by default.
// Unknown future classifications are excluded too.
func (s Scalar) IsPrimitive() bool {
	switch s {
	case String, Boolean, Integer, Number:
		return true
	default:
		return false
	}
}

// Field is a detached snapshot of one serialized field. Type preserves the exact
// declared type for identity conventions. Index is an owned Go field-index path;
// Collection indicates that the path crosses an array, slice or map boundary.
// Nullable describes the field itself, not an optional ancestor. Tag is decoded
// chronicle metadata; consumers must not log it because it may contain literals.
type Field struct {
	GoField    string
	Index      []int
	Name       string
	Path       string
	Type       reflect.Type
	Nullable   bool
	Scalar     Scalar
	Format     string
	Collection bool
	Tag        string
	plan       *node
}

// Fields returns owned metadata in declaration/traversal order. Names come from
// the exact same compiled plan as Schema and Marshal, never a second naming policy.
func (p *Plan) Fields() []Field {
	if p == nil {
		return nil
	}
	var fields []Field
	collectFields(p.root, "", "", nil, false, &fields, map[*node]bool{})
	return fields
}

// ContainsBinary reports whether this field or any compiled descendant requires
// the binary codec. Capability consumers must explicitly qualify such fields;
// an object's primitive-looking JSON members do not make it binary-safe.
func (f Field) ContainsBinary() bool {
	if f.plan == nil {
		return false
	}
	n := f.plan
	if n.reference != nil {
		n = n.reference
	}
	return n.containsBinary
}

// Fields returns the object's (or collection element's) local metadata. Recursive
// edges are bounded per traversal path, not globally; siblings remain independent.
func (f Field) Fields() []Field {
	if f.plan == nil {
		return nil
	}
	n := f.plan
	for n.item != nil {
		n = n.item
	}
	if n.reference != nil {
		n = n.reference
	}
	var fields []Field
	collectFields(n, "", f.GoField, nil, false, &fields, map[*node]bool{})
	return fields
}

func collectFields(n *node, path, goPath string, index []int, collection bool, result *[]Field, active map[*node]bool) {
	if n.reference != nil {
		n = n.reference
	}
	if active[n] {
		return
	}
	active[n] = true
	defer delete(active, n)
	if n.item != nil {
		collectFields(n.item, path, goPath, index, collection || n.typ.Kind() != reflect.Pointer, result, active)
		return
	}
	for _, f := range n.fields {
		fieldPath, fieldName := f.name, f.goName
		if path != "" {
			fieldPath = path + "." + f.name
			fieldName = goPath + "." + f.goName
		}
		indices := append(append([]int(nil), index...), f.index...)
		scalar, format := classify(f.value)
		*result = append(*result, Field{GoField: fieldName, Index: indices, Name: f.name, Path: fieldPath, Type: f.value.typ, Nullable: f.value.typ.Kind() == reflect.Pointer || f.value.typ.Kind() == reflect.Interface, Scalar: scalar, Format: format, Collection: collection, Tag: f.tag, plan: f.value})
		collectFields(f.value, fieldPath, fieldName, indices, collection, result, active)
	}
}
func classify(n *node) (Scalar, string) {
	if n.typ.Kind() == reflect.Pointer {
		return classify(n.item)
	}
	format, _ := n.schema["format"].(string)
	if n.binary {
		return Binary, format
	}
	switch n.schema["type"] {
	case "string":
		return String, format
	case "boolean":
		return Boolean, format
	case "integer":
		return Integer, format
	case "number":
		return Number, format
	default:
		return NotScalar, ""
	}
}

// ValidateRole rejects declarations that cannot be honored by the artifact role.
// Compile already validates syntax and supported model directives.
func (p *Plan) ValidateRole(role declarations.Role) error {
	return p.visitAll(func(n *node) error {
		for _, f := range n.fields {
			if err := validateTag(f.tag, role, p.typ.String(), f.goName, f.name); err != nil {
				return err
			}
		}
		for _, derivative := range n.derivatives {
			if err := visitNodes(derivative.node, map[*node]bool{}, func(child *node) error {
				for _, f := range child.fields {
					if role == declarations.Model {
						switch strings.ToLower(f.name) {
						case "_subject", "__subject", "__subjects":
							return codecError(derivative.registration, f.goName, "reserved compliance property")
						}
					}
					directives, err := declarations.Parse(declarations.V1, f.tag)
					if err != nil {
						return err
					}
					for _, directive := range directives {
						if directive.Name == "pii" || directive.Name == "encrypted" || directive.Name == "compliance-details" {
							continue // Protection audits and rejects classified variants separately.
						}
						// Derived children consume projection directives on their
						// derivative. Subjects, indexes and event roles have no
						// variant-qualified consumer and remain refused.
						if role != declarations.Model || directive.Name == "subject" || directive.Name == "index" {
							return codecError(derivative.registration, f.goName, "artifact role declarations within derivatives are not supported")
						}
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
}
func validateTag(tag string, role declarations.Role, artifact, field, path string) error {
	parsed, err := declarations.Parse(declarations.V1, tag)
	if err == nil {
		err = declarations.Validate(role, parsed)
	}
	var declaration *declarations.DeclarationError
	if errors.As(err, &declaration) {
		declaration.Artifact, declaration.GoField, declaration.Path = artifact, field, path
	}
	return err
}

// FieldAt resolves an exact serialized object-property path. Collection paths are
// present in Fields for schema tooling but cannot be used as scalar object paths.
func FieldAt(fields []Field, path string) (Field, bool) {
	for _, f := range fields {
		if f.Path == path {
			f.Index = append([]int(nil), f.Index...)
			return f, true
		}
	}
	// Resolve a finite caller-supplied path beyond a recursive metadata edge.
	// Every step consumes a property segment, so cycles cannot loop indefinitely.
	for _, parent := range RootFields(fields) {
		if !strings.HasPrefix(path, parent.Path+".") {
			continue
		}
		field, ok := FieldAt(parent.Fields(), strings.TrimPrefix(path, parent.Path+"."))
		if !ok {
			return Field{}, false
		}
		field.Path = parent.Path + "." + field.Path
		field.GoField = parent.GoField + "." + field.GoField
		field.Index = append(append([]int(nil), parent.Index...), field.Index...)
		typ := parent.Type
		for typ.Kind() == reflect.Pointer {
			typ = typ.Elem()
		}
		field.Collection = field.Collection || parent.Collection || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array || typ.Kind() == reflect.Map
		return field, true
	}
	return Field{}, false
}

// FieldAtWithCapability resolves a path while preferring any candidate with the
// supplied capability. Literal dotted JSON names may overlap segment-resolved
// paths; capability guards must not let the first ordinary field hide a sensitive
// candidate. Recursive paths are resolved finitely, consuming a prefix per step.
// If no candidate has the capability (or capability is nil), FieldAt is used.
func FieldAtWithCapability(fields []Field, path string, capability func(Field) bool) (Field, bool) {
	if capability != nil {
		for _, field := range fields {
			if field.Path == path && capability(field) {
				field.Index = slices.Clone(field.Index)
				return field, true
			}
		}
		for _, parent := range EmittedRootFields(fields) {
			if !strings.HasPrefix(path, parent.Path+".") {
				continue
			}
			field, ok := FieldAtWithCapability(parent.Fields(), strings.TrimPrefix(path, parent.Path+"."), capability)
			if !ok || !capability(field) {
				continue
			}
			field.Path = parent.Path + "." + field.Path
			field.GoField = parent.GoField + "." + field.GoField
			field.Index = append(slices.Clone(parent.Index), field.Index...)
			typ := parent.Type
			for typ.Kind() == reflect.Pointer {
				typ = typ.Elem()
			}
			field.Collection = field.Collection || parent.Collection || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array || typ.Kind() == reflect.Map
			return field, true
		}
	}
	return FieldAt(fields, path)
}

// EmittedRootFields returns emitted top-level fields by Go index ownership,
// including literal dots in JSON names and flattened embeddings. Use this when
// matching raw JSON properties rather than segment-oriented paths.
func EmittedRootFields(fields []Field) []Field {
	var result []Field
	for _, f := range fields {
		if !slices.ContainsFunc(fields, func(owner Field) bool {
			return len(owner.Index) > 0 && len(owner.Index) < len(f.Index) && slices.Equal(owner.Index, f.Index[:len(owner.Index)])
		}) {
			f.Index = append([]int(nil), f.Index...)
			result = append(result, f)
		}
	}
	return result
}

// RootFields returns top-level segment-oriented paths from a metadata snapshot.
// Literal dotted property names are excluded; use EmittedRootFields for raw JSON.
func RootFields(fields []Field) []Field {
	var result []Field
	for _, field := range fields {
		if !strings.Contains(field.Path, ".") {
			field.Index = slices.Clone(field.Index)
			result = append(result, field)
		}
	}
	return result
}
