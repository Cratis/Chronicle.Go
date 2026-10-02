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
	"github.com/cratis/fundamentals.go/concepts"
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
	typ := target.Type
	if representation, ok, err := concepts.Underlying(typ); err != nil {
		return invalid("invalid concept representation")
	} else if ok {
		typ = representation.Type
	}
	value := reflect.New(typ)
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

// scalarCompatible checks wire conversion, not exact Go width or nullability.
// Typed field/key validation separately preserves declared domain identity.
func scalarCompatible(target, source serialization.Field, targetFields, sourceFields []serialization.Field) bool {
	targetScalar, targetOK := scalarRepresentation(target)
	sourceScalar, sourceOK := scalarRepresentation(source)
	if !targetOK || !sourceOK {
		return false
	}
	target.Scalar, source.Scalar = targetScalar, sourceScalar
	if target.Scalar == serialization.NotScalar || source.Scalar == serialization.NotScalar {
		if target.Scalar != source.Scalar {
			return false
		}
		return objectCompatible(target, source, targetFields, sourceFields)
	}
	if target.Scalar == serialization.String && source.Scalar == serialization.String {
		return target.Format == "" || source.Format == "" || stringFormat(target.Format) == stringFormat(source.Format)
	}
	return target.Scalar == source.Scalar || target.Scalar == serialization.Number && source.Scalar == serialization.Integer
}

func stringFormat(format string) string {
	if format == "guid" {
		return "uuid"
	}
	return format
}

func objectCompatible(target, source serialization.Field, targetFields, sourceFields []serialization.Field) bool {
	targetType, sourceType := indirectType(target.Type), indirectType(source.Type)
	switch targetType.Kind() {
	case reflect.Slice, reflect.Array, reflect.Map:
		if targetType.Kind() == reflect.Map {
			if sourceType.Kind() != reflect.Map {
				return false
			}
		} else if sourceType.Kind() != reflect.Slice && sourceType.Kind() != reflect.Array {
			return false
		}
		targetElement, targetOK := elementField(target, targetType.Elem())
		sourceElement, sourceOK := elementField(source, sourceType.Elem())
		return targetOK && sourceOK && scalarCompatible(targetElement, sourceElement, targetFields, sourceFields)
	case reflect.Struct:
		if sourceType.Kind() != reflect.Struct {
			return false
		}
		for _, property := range childFields(targetFields, target.Path) {
			if corresponding, ok := serialization.FieldAt(sourceFields, source.Path+"."+property.Name); ok &&
				!scalarCompatible(property, corresponding, targetFields, sourceFields) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func childFields(fields []serialization.Field, path string) []serialization.Field {
	var children []serialization.Field
	for _, field := range fields {
		if strings.TrimSuffix(field.Path, "."+field.Name) == path {
			children = append(children, field)
		}
	}
	return children
}

func elementField(parent serialization.Field, typ reflect.Type) (serialization.Field, bool) {
	// Collection metadata describes the container, so compile only the element's
	// scalar classification. Descendant names still come from the original plans.
	plan, err := serialization.Compile(reflect.StructOf([]reflect.StructField{{Name: "Value", Type: typ}}))
	if err != nil {
		return serialization.Field{}, false
	}
	field := plan.Fields()[0]
	field.Path = parent.Path
	return field, true
}

func scalarRepresentation(field serialization.Field) (serialization.Scalar, bool) {
	r, ok, err := concepts.Underlying(field.Type)
	if err != nil {
		return serialization.NotScalar, false
	}
	if !ok {
		return field.Scalar, true
	}
	switch r.Kind {
	case concepts.KindUUID, concepts.KindDateOnly, concepts.KindTimeOnly, concepts.KindTimeSpan, concepts.KindString:
		return serialization.String, true
	case concepts.KindBool:
		return serialization.Boolean, true
	case concepts.KindFloat32, concepts.KindFloat64:
		return serialization.Number, true
	default:
		return serialization.Integer, true
	}
}

func indirectType(typ reflect.Type) reflect.Type {
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	return typ
}
func validateExpression(e expression, target serialization.Field, modelFields, eventFields []serialization.Field, sourceType reflect.Type) error {
	switch e.kind {
	case pathExpression:
		source, ok := serialization.FieldAt(eventFields, e.text)
		if !eventPropertyPath(e.text) || !ok || source.Collection {
			return invalid("unknown or unsupported event property path")
		}
		if sourceType != nil && source.Type != sourceType {
			return invalid("event field descriptor has the wrong Go value type")
		}
		if !scalarCompatible(target, source, modelFields, eventFields) {
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
