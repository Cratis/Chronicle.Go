// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"encoding/json"
	"reflect"
	"regexp"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/serialization"
)

type expressionKind uint8

const (
	emptyExpression expressionKind = iota
	pathExpression
	contextExpression
	sourceExpression
	literalExpression
	nullExpression
	invalidExpression
)

type expression struct {
	kind        expressionKind
	text        string
	literalKind declarations.Kind
}

func literalValue(v declarations.Value) expression {
	if v.Kind == declarations.Null {
		return expression{kind: nullExpression}
	}
	return expression{kind: literalExpression, text: v.Text, literalKind: v.Kind}
}
func (e expression) encode() string {
	switch e.kind {
	case pathExpression:
		return e.text
	case contextExpression:
		return "$eventContext(" + e.text + ")"
	case sourceExpression:
		return "$eventSourceId"
	case literalExpression:
		text := e.text
		if e.literalKind == declarations.Boolean {
			if text == "true" {
				text = "True"
			} else {
				text = "False"
			}
		}
		return "$value(" + text + ")"
	case nullExpression:
		return "$null"
	default:
		return ""
	}
}

// The kernel's ValueExpressionResolver restricts literals to this character set.
// Anchor it here: the kernel regex is unanchored and must never accept a partial
// match or nested expression accidentally. JSON quoting is not kernel escaping.
var kernelLiteral = regexp.MustCompile(`^[\p{L}\p{Mn}\p{Nd}\p{Pc} ._/:*+\-]*$`)

func validateLiteral(e expression, target serialization.Field) error {
	if e.kind == nullExpression {
		if !target.Nullable || target.Scalar == serialization.NotScalar {
			return invalid("null requires a nullable scalar pointer")
		}
		return nil
	}
	if !representableLiteral(e.text) {
		return invalid("literal cannot be represented by the kernel expression grammar")
	}
	if target.Scalar == serialization.NotScalar {
		return invalid("literal requires a scalar field")
	}
	var data []byte
	switch e.literalKind {
	case declarations.String:
		if target.Scalar != serialization.String {
			return invalid("string literal does not match target scalar")
		}
		data, _ = json.Marshal(e.text)
	case declarations.Boolean:
		if target.Scalar != serialization.Boolean {
			return invalid("boolean literal does not match target scalar")
		}
		data = []byte(e.text)
	case declarations.Number:
		if target.Scalar != serialization.Integer && target.Scalar != serialization.Number {
			return invalid("numeric literal does not match target scalar")
		}
		data = []byte(e.text)
	default:
		return invalid("invalid scalar literal")
	}
	// The plan has already rejected custom codecs. Decode only to validate scalar
	// ranges/formats; never call a concept accessor on a fabricated value.
	value := reflect.New(target.Type)
	if err := json.Unmarshal(data, value.Interface()); err != nil {
		return invalid("literal does not fit target scalar range or format")
	}
	scalar := value.Elem()
	for scalar.Kind() == reflect.Pointer {
		scalar = scalar.Elem()
	}
	if scalar.Kind() == reflect.Uint64 || scalar.Kind() == reflect.Uint {
		if scalar.Uint() > 1<<63-1 {
			return invalid("unsigned literal exceeds kernel integer range")
		}
	}
	return nil
}

// scalarCompatible is the single representation-compatibility seam. Named
// primitives use their reflected kind today; the concepts slice can substitute
// concepts.Underlying here without changing either projection front end.
func scalarCompatible(target, source serialization.Field) bool {
	if target.Scalar == serialization.NotScalar || source.Scalar == serialization.NotScalar {
		return target.Type == source.Type
	}
	return target.Scalar == source.Scalar && target.Format == source.Format && (!source.Nullable || target.Nullable)
}
func validateExpression(e expression, target serialization.Field, eventFields []serialization.Field, sourceType reflect.Type) error {
	switch e.kind {
	case pathExpression:
		source, ok := serialization.FieldAt(eventFields, e.text)
		if !eventPropertyPath(e.text) || !ok || source.Collection {
			return invalid("unknown or unsupported event property path")
		}
		if sourceType != nil && source.Type != sourceType {
			return invalid("event field descriptor has the wrong Go value type")
		}
		if !scalarCompatible(target, source) {
			return invalid("incompatible event and model property representations")
		}
	case sourceExpression:
		if target.Scalar != serialization.String || target.Format != "" && target.Format != "uuid" {
			return invalid("event source identity requires a string or UUID field")
		}
	case contextExpression:
		if !declarations.Path(e.text) {
			return invalid("invalid context path")
		}
		if err := validateContext(e.text, target); err != nil {
			return err
		}
	case literalExpression, nullExpression:
		return validateLiteral(e, target)
	default:
		return invalid("incomplete or unsupported expression")
	}
	return nil
}
func validateContext(path string, target serialization.Field) error {
	// Names are the serialized kernel EventContext contract, not Go's differently
	// named Context fields. Complex collections/objects require the later node slice.
	switch path {
	case "occurred":
		if target.Scalar == serialization.String && target.Format == "date-time" {
			return nil
		}
	case "eventSourceId", "correlationId":
		if target.Scalar == serialization.String && (target.Format == "" || target.Format == "uuid") {
			return nil
		}
	case "eventSourceType", "eventStreamType", "eventStreamId", "eventStore", "namespace", "subject", "hash", "eventType.id":
		if target.Scalar == serialization.String && target.Format == "" {
			return nil
		}
	case "sequenceNumber", "eventType.generation":
		if target.Scalar == serialization.Integer {
			return nil
		}
	case "observationState":
		if target.Scalar == serialization.Integer {
			return nil
		}
	default:
		return invalid("unknown or not yet supported scalar context property")
	}
	return invalid("context property does not match target scalar")
}

func validateTarget(field serialization.Field, expected reflect.Type) error {
	if field.Collection || !declarations.Path(field.Path) {
		return invalid("unsupported model property path")
	}
	if expected != nil && field.Type != expected {
		return invalid("model field descriptor has the wrong Go value type")
	}
	return nil
}
func validateKey(e expression, expected reflect.Type, fields []serialization.Field, optional bool) error {
	switch e.kind {
	case emptyExpression:
		if optional {
			return nil
		}
	case sourceExpression:
		return nil
	case literalExpression:
		if e.literalKind == declarations.String && e.text != "" && representableLiteral(e.text) {
			return nil
		}
	case pathExpression:
		field, ok := serialization.FieldAt(fields, e.text)
		if ok && !field.Collection && field.Scalar != serialization.NotScalar && !field.Nullable && eventPropertyPath(e.text) && (expected == nil || expected == field.Type) {
			return nil
		}
	}
	return invalid("invalid or unrepresentable correlation key")
}
func representableLiteral(value string) bool {
	// .NET's regex matches UTF-16 code units, not supplementary-plane runes.
	for _, r := range value {
		if r > 0xffff {
			return false
		}
	}
	return kernelLiteral.MatchString(value)
}

func eventPropertyPath(path string) bool {
	// LiteralExpressionResolver takes precedence over event content in the kernel.
	switch path {
	case "true", "True", "false", "False":
		return false
	default:
		return declarations.Path(path)
	}
}

func blank(value string) bool { return strings.TrimSpace(value) == "" }
