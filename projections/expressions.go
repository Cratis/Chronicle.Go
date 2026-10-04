// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
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
	addExpression
	subtractExpression
	incrementExpression
	decrementExpression
	countExpression
	compositeExpression
)

type expression struct {
	kind        expressionKind
	text        string
	literalKind declarations.Kind
	parts       []keyPart
}

type keyPart struct {
	name       string
	expression expression
	sourceType reflect.Type
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
	case addExpression:
		return "$add(" + e.text + ")"
	case subtractExpression:
		return "$subtract(" + e.text + ")"
	case incrementExpression:
		return "$increment"
	case decrementExpression:
		return "$decrement"
	case countExpression:
		return "$count"
	case compositeExpression:
		parts := make([]string, len(e.parts))
		for i, part := range e.parts {
			parts[i] = part.name + "=" + part.expression.encode()
		}
		return "$composite(" + strings.Join(parts, ",") + ")"
	default:
		return ""
	}
}

// The kernel's ValueExpressionResolver restricts literals to this character set.
// Anchor it here: the kernel regex is unanchored and must never accept a partial
// match or nested expression accidentally. JSON quoting is not kernel escaping.
var kernelLiteral = regexp.MustCompile(`^[\p{L}\p{Mn}\p{Nd}\p{Pc} ._/:*+\-]*$`)

// Add/Subtract embed the event provider in a stricter ASCII kernel regex.
// Even an otherwise valid JSON path (e.g. amount_delta) may not fit it.
var kernelArithmeticPath = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9.]*$`)

func validateLiteral(e expression, target serialization.Field) error {
	if e.kind == nullExpression {
		if !nullableAssignment(target) {
			return invalid("null requires a supported nullable pointer")
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
	if target.IsEnum() {
		return validateEnumLiteral(data, target)
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
	if target.IsEnum() || source.IsEnum() {
		return target.SameRepresentation(source)
	}
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
	case reflect.Interface:
		return sourceType.Kind() == reflect.Interface && target.SameRepresentation(source)
	case reflect.Slice, reflect.Array, reflect.Map:
		if targetType.Kind() == reflect.Map {
			if sourceType.Kind() != reflect.Map {
				return false
			}
		} else if sourceType.Kind() != reflect.Slice && sourceType.Kind() != reflect.Array {
			return false
		}
		targetElement, targetOK := target.Element()
		sourceElement, sourceOK := source.Element()
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
	case addExpression, subtractExpression:
		if !kernelArithmeticPath.MatchString(e.text) {
			return invalid("arithmetic path cannot be represented by the kernel expression grammar")
		}
		if !numeric(target) {
			return invalid("arithmetic requires a numeric target")
		}
		source, ok := serialization.FieldAt(eventFields, e.text)
		if !ok || !numeric(source) {
			return invalid("arithmetic requires a numeric event field")
		}
		e.kind = pathExpression
		return validateExpression(e, target, modelFields, eventFields, sourceType)
	case incrementExpression, decrementExpression, countExpression:
		if !numeric(target) {
			return invalid("arithmetic requires a numeric target")
		}
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

// nullableAssignment uses the frozen field representation. Collection elements,
// fixed arrays, interfaces and objects are not direct nullable assignments.
func nullableAssignment(target serialization.Field) bool {
	if !target.Nullable || target.Collection || target.Type.Kind() != reflect.Pointer {
		return false
	}
	if target.Scalar != serialization.NotScalar {
		return true
	}
	typ := indirectType(target.Type)
	if typ.Kind() != reflect.Slice && (typ.Kind() != reflect.Map || typ.Key().Kind() != reflect.String) {
		return false
	}
	_, compiled := target.Element()
	return compiled
}

// resolveContext describes the kernel contract, not Go events.Context's names.
// Keep existing scalar spellings and conversions; whole Tags is the first
// admitted collection. Other known complex properties fail explicitly.
func resolveContext(path string) (serialization.Field, error) {
	field := serialization.Field{Type: reflect.TypeFor[string](), Scalar: serialization.String}
	switch path {
	case "occurred":
		field.Type, field.Format = reflect.TypeFor[time.Time](), "date-time"
	case "eventSourceId", "EventSourceId", "correlationId":
		field.Format = "uuid"
	case "eventSourceType", "eventStreamType", "eventStreamId", "eventStore", "namespace", "subject", "hash", "eventType.id":
	case "sequenceNumber", "eventType.generation", "observationState":
		field.Type, field.Scalar = reflect.TypeFor[int64](), serialization.Integer
	case "Tags", "tags":
		field.Type, field.Scalar = reflect.TypeFor[[]events.Tag](), serialization.NotScalar
	case "CausedBy", "causedBy", "EventType", "eventType", "NamedTags", "namedTags", "Causation", "causation":
		return serialization.Field{}, fmt.Errorf("%w: complex context property is not supported", faults.ErrUnsupported)
	default:
		return serialization.Field{}, invalid("unknown or not yet supported scalar context property")
	}
	return field, nil
}

func validateContext(path string, target serialization.Field) error {
	if target.IsEnum() {
		return invalid("context cannot supply a declared enum")
	}
	source, err := resolveContext(path)
	if err != nil {
		return err
	}
	if source.Scalar == serialization.NotScalar {
		if indirectType(target.Type).Kind() == reflect.Slice {
			element, ok := target.Element()
			if ok && !element.Nullable && element.Scalar == serialization.String && element.Format == "" {
				return nil
			}
		}
		return invalid("context Tags requires a slice of non-nullable unformatted strings")
	}
	if target.Scalar == source.Scalar {
		switch source.Format {
		case "date-time":
			if target.Format == "date-time" {
				return nil
			}
		case "uuid":
			if target.Format == "" || target.Format == "uuid" {
				return nil
			}
		default:
			if target.Scalar != serialization.String || target.Format == "" {
				return nil
			}
		}
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
	case contextExpression:
		if _, ok := contextField(e.text); ok {
			return nil
		}
	case compositeExpression:
		if len(e.parts) == 0 {
			break
		}
		seen := map[string]bool{}
		for _, part := range e.parts {
			// The kernel composite parser splits commas without balancing nested
			// expressions; nested composites cannot be represented faithfully.
			if !declarations.Path(part.name) || seen[part.name] || part.expression.kind == compositeExpression {
				return invalid("invalid or duplicate composite key part")
			}
			seen[part.name] = true
			if err := validateKey(part.expression, part.sourceType, fields, false); err != nil {
				return err
			}
		}
		return nil
	case literalExpression:
		if e.literalKind == declarations.String && e.text != "" && representableLiteral(e.text) {
			return nil
		}
		if e.literalKind == declarations.Boolean {
			return validateLiteral(e, serialization.Field{Type: reflect.TypeFor[bool](), Scalar: serialization.Boolean})
		}
		if e.literalKind == declarations.Number {
			return validateLiteral(e, serialization.Field{Type: reflect.TypeFor[int64](), Scalar: serialization.Integer})
		}
	case pathExpression:
		field, ok := serialization.FieldAt(fields, e.text)
		if ok && !field.IsEnum() && !field.Collection && field.Scalar != serialization.NotScalar && !field.Nullable && eventPropertyPath(e.text) && (expected == nil || expected == field.Type) {
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
