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
	causedByExpression
	removeExpression
)

type expression struct {
	kind        expressionKind
	text        string
	literalKind declarations.Kind
	parts       []keyPart
	// once wraps a set-family value in the kernel's $setOnce(...) expression.
	once bool
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
	if e.once {
		inner := e
		inner.once = false
		return "$setOnce(" + inner.encode() + ")"
	}
	return e.encodeUnwrapped()
}

func (e expression) encodeUnwrapped() string {
	switch e.kind {
	case pathExpression, contextExpression, sourceExpression, literalExpression, nullExpression:
		return e.encodeValue()
	case addExpression, subtractExpression, incrementExpression, decrementExpression, countExpression:
		return e.encodeArithmetic()
	case causedByExpression:
		return "$causedBy(" + e.text + ")"
	case removeExpression:
		return "$remove"
	case compositeExpression:
		return e.encodeComposite()
	default:
		return ""
	}
}

func (e expression) encodeValue() string {
	switch e.kind {
	case pathExpression:
		return e.text
	case contextExpression:
		return "$eventContext(" + e.text + ")"
	case sourceExpression:
		return "$eventSourceId"
	case literalExpression:
		return "$value(" + e.literalText() + ")"
	default:
		return "$null"
	}
}

func (e expression) literalText() string {
	if e.literalKind != declarations.Boolean {
		return e.text
	}
	if e.text == "true" {
		return "True"
	}
	return "False"
}

func (e expression) encodeArithmetic() string {
	switch e.kind {
	case addExpression:
		return "$add(" + e.text + ")"
	case subtractExpression:
		return "$subtract(" + e.text + ")"
	case incrementExpression:
		return "$increment"
	case decrementExpression:
		return "$decrement"
	default:
		return "$count"
	}
}

func (e expression) encodeComposite() string {
	parts := make([]string, len(e.parts))
	for i, part := range e.parts {
		parts[i] = part.name + "=" + part.expression.encode()
	}
	return "$composite(" + strings.Join(parts, ",") + ")"
}

// The kernel's ValueExpressionResolver restricts literals to this character set.
// Anchor it here: the kernel regex is unanchored and must never accept a partial
// match or nested expression accidentally. JSON quoting is not kernel escaping.
var kernelLiteral = regexp.MustCompile(`^[\p{L}\p{Mn}\p{Nd}\p{Pc} ._/:*+\-]*$`)

// Add/Subtract embed the event provider in a stricter ASCII kernel regex.
// Even an otherwise valid JSON path (e.g. amount_delta) may not fit it.
var kernelArithmeticPath = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9.]*$`)

func validateLiteral(e expression, target serialization.Field) error {
	if binaryField(target) {
		return binaryUnsupported("binary literals are not supported")
	}
	if e.kind == nullExpression {
		return validateNullLiteral(target)
	}
	if !representableLiteral(e.text) {
		return invalid("literal cannot be represented by the kernel expression grammar")
	}
	if !target.Scalar.IsPrimitive() {
		return invalid("literal requires a scalar field")
	}
	data, err := literalJSON(e, target)
	if err != nil {
		return err
	}
	if target.IsEnum() {
		return validateEnumLiteral(data, target)
	}
	return validateScalarLiteral(data, target)
}

func validateNullLiteral(target serialization.Field) error {
	if !nullableAssignment(target) {
		return invalid("null requires a supported nullable pointer")
	}
	return nil
}

func literalJSON(e expression, target serialization.Field) ([]byte, error) {
	switch e.literalKind {
	case declarations.String:
		if target.Scalar != serialization.String {
			return nil, invalid("string literal does not match target scalar")
		}
		data, _ := json.Marshal(e.text)
		return data, nil
	case declarations.Boolean:
		return unquotedLiteralJSON(e.text, target.Scalar == serialization.Boolean, "boolean literal does not match target scalar")
	case declarations.Number:
		return unquotedLiteralJSON(e.text, numericScalar(target.Scalar), "numeric literal does not match target scalar")
	default:
		return nil, invalid("invalid scalar literal")
	}
}

func unquotedLiteralJSON(text string, matches bool, mismatch string) ([]byte, error) {
	if !matches {
		return nil, invalid(mismatch)
	}
	return []byte(text), nil
}

func numericScalar(scalar serialization.Scalar) bool {
	return scalar == serialization.Integer || scalar == serialization.Number
}

func validateScalarLiteral(data []byte, target serialization.Field) error {
	// The plan has already rejected custom codecs. Decode only to validate scalar
	// ranges/formats; never call a concept accessor on a fabricated value.
	typ, err := literalRepresentationType(target.Type)
	if err != nil {
		return err
	}
	value := reflect.New(typ)
	if err := json.Unmarshal(data, value.Interface()); err != nil {
		return invalid("literal does not fit target scalar range or format")
	}
	return validateLiteralIntegerRange(value.Elem())
}

func literalRepresentationType(typ reflect.Type) (reflect.Type, error) {
	representation, ok, err := concepts.Underlying(typ)
	if err != nil {
		return nil, invalid("invalid concept representation")
	}
	if ok {
		return representation.Type, nil
	}
	return typ, nil
}

func validateLiteralIntegerRange(scalar reflect.Value) error {
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
	if binaryField(target) || binaryField(source) {
		// Only the final, qualified AutoMap path admits binary copies.
		return false
	}
	if target.IsEnum() || source.IsEnum() {
		return target.SameRepresentation(source)
	}
	targetScalar, targetOK := scalarRepresentation(target)
	sourceScalar, sourceOK := scalarRepresentation(source)
	if !targetOK || !sourceOK {
		return false
	}
	target.Scalar, source.Scalar = targetScalar, sourceScalar
	return representationCompatible(target, source, targetFields, sourceFields)
}

func representationCompatible(target, source serialization.Field, targetFields, sourceFields []serialization.Field) bool {
	if target.Scalar == serialization.NotScalar || source.Scalar == serialization.NotScalar {
		if target.Scalar != source.Scalar {
			return false
		}
		return objectCompatible(target, source, targetFields, sourceFields)
	}
	return primitiveCompatible(target, source)
}

func primitiveCompatible(target, source serialization.Field) bool {
	if !target.Scalar.IsPrimitive() || !source.Scalar.IsPrimitive() {
		return false
	}
	if target.Scalar == serialization.String && source.Scalar == serialization.String {
		return stringFormatsCompatible(target.Format, source.Format)
	}
	return target.Scalar == source.Scalar || target.Scalar == serialization.Number && source.Scalar == serialization.Integer
}

func stringFormatsCompatible(target, source string) bool {
	return target == "" || source == "" || stringFormat(target) == stringFormat(source)
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
		return collectionCompatible(target, source, targetFields, sourceFields)
	case reflect.Struct:
		return structCompatible(target, source, targetFields, sourceFields)
	default:
		return false
	}
}

func collectionCompatible(target, source serialization.Field, targetFields, sourceFields []serialization.Field) bool {
	if !collectionKindsCompatible(indirectType(target.Type), indirectType(source.Type)) {
		return false
	}
	targetElement, targetOK := target.Element()
	sourceElement, sourceOK := source.Element()
	return targetOK && sourceOK && scalarCompatible(targetElement, sourceElement, targetFields, sourceFields)
}

func collectionKindsCompatible(target, source reflect.Type) bool {
	if target.Kind() == reflect.Map {
		return source.Kind() == reflect.Map
	}
	return source.Kind() == reflect.Slice || source.Kind() == reflect.Array
}

func structCompatible(target, source serialization.Field, targetFields, sourceFields []serialization.Field) bool {
	if indirectType(source.Type).Kind() != reflect.Struct {
		return false
	}
	for _, property := range childFields(targetFields, target.Path) {
		if corresponding, ok := serialization.FieldAt(sourceFields, source.Path+"."+property.Name); ok &&
			!scalarCompatible(property, corresponding, targetFields, sourceFields) {
			return false
		}
	}
	return true
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
	validation := expressionValidation{target: target, modelFields: modelFields, eventFields: eventFields, sourceType: sourceType}
	return validation.validate(e)
}

type expressionValidation struct {
	target      serialization.Field
	modelFields []serialization.Field
	eventFields []serialization.Field
	sourceType  reflect.Type
}

func (v expressionValidation) validate(e expression) error {
	if e.once {
		if !settableOnce(e) {
			return invalid("set once requires an event property, context, event source, caused-by or non-null literal value")
		}
		e.once = false
	}
	if err := v.validateBinary(e); err != nil {
		return err
	}
	switch e.kind {
	case addExpression, subtractExpression:
		return v.validateArithmetic(e)
	case incrementExpression, decrementExpression, countExpression:
		return validateNumericTarget(v.target)
	default:
		return v.validateValue(e)
	}
}

func (v expressionValidation) validateBinary(e expression) error {
	if binaryField(v.target) {
		return binaryUnsupported("binary is only qualified for same-representation AutoMap")
	}
	switch e.kind {
	case pathExpression, addExpression, subtractExpression:
		if source, ok := binaryMappingField(v.eventFields, e.text); ok && binaryField(source) {
			return binaryUnsupported("binary is only qualified for same-representation AutoMap")
		}
	}
	return nil
}

func (v expressionValidation) validateArithmetic(e expression) error {
	if !kernelArithmeticPath.MatchString(e.text) {
		return invalid("arithmetic path cannot be represented by the kernel expression grammar")
	}
	if err := validateNumericTarget(v.target); err != nil {
		return err
	}
	source, ok := binaryMappingField(v.eventFields, e.text)
	if !ok || !numeric(source) {
		return invalid("arithmetic requires a numeric event field")
	}
	e.kind = pathExpression
	return v.validate(e)
}

func validateNumericTarget(target serialization.Field) error {
	if !numeric(target) {
		return invalid("arithmetic requires a numeric target")
	}
	return nil
}

func (v expressionValidation) validateValue(e expression) error {
	switch e.kind {
	case pathExpression:
		return v.validatePath(e.text)
	case sourceExpression:
		return validateSourceTarget(v.target)
	case contextExpression:
		return validateContextExpression(e.text, v.target)
	case literalExpression, nullExpression:
		return validateLiteral(e, v.target)
	case causedByExpression:
		return validateCausedBy(e.text, v.target)
	case removeExpression:
		return invalid("remove requires a dictionary entry target")
	default:
		return invalid("incomplete or unsupported expression")
	}
}

func (v expressionValidation) validatePath(path string) error {
	source, ok := binaryMappingField(v.eventFields, path)
	if !validEventPathSource(path, source, ok) {
		return invalid("unknown or unsupported event property path")
	}
	if v.sourceType != nil && source.Type != v.sourceType {
		return invalid("event field descriptor has the wrong Go value type")
	}
	if !scalarCompatible(v.target, source, v.modelFields, v.eventFields) {
		return invalid("incompatible event and model property representations")
	}
	return nil
}

func validEventPathSource(path string, source serialization.Field, found bool) bool {
	return eventPropertyPath(path) && found && !source.Collection
}

func validateSourceTarget(target serialization.Field) error {
	if !sourceTargetCompatible(target) {
		return invalid("event source identity requires a string or UUID field")
	}
	return nil
}

func sourceTargetCompatible(target serialization.Field) bool {
	return target.Scalar == serialization.String && (target.Format == "" || target.Format == "uuid")
}

func validateContextExpression(path string, target serialization.Field) error {
	if !declarations.Path(path) {
		return invalid("invalid context path")
	}
	return validateContext(path, target)
}

// nullableAssignment uses the frozen field representation. Collection elements,
// fixed arrays, interfaces and objects are not direct nullable assignments.
func nullableAssignment(target serialization.Field) bool {
	if !directNullablePointer(target) {
		return false
	}
	if target.Scalar.IsPrimitive() {
		return true
	}
	if target.ContainsBinary() {
		return false
	}
	typ := indirectType(target.Type)
	if !nullableCollectionType(typ) {
		return false
	}
	_, compiled := target.Element()
	return compiled
}

func directNullablePointer(target serialization.Field) bool {
	return target.Nullable && !target.Collection && target.Type.Kind() == reflect.Pointer
}

func nullableCollectionType(typ reflect.Type) bool {
	return typ.Kind() == reflect.Slice || typ.Kind() == reflect.Map && typ.Key().Kind() == reflect.String
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
		return validateContextTags(target)
	}
	if contextScalarCompatible(target, source) {
		return nil
	}
	return invalid("context property does not match target scalar")
}

func validateContextTags(target serialization.Field) error {
	if indirectType(target.Type).Kind() == reflect.Slice {
		element, ok := target.Element()
		if contextTagElement(element, ok) {
			return nil
		}
	}
	return invalid("context Tags requires a slice of non-nullable unformatted strings")
}

func contextTagElement(element serialization.Field, found bool) bool {
	return found && !element.Nullable && element.Scalar == serialization.String && element.Format == ""
}

func contextScalarCompatible(target, source serialization.Field) bool {
	if target.Scalar != source.Scalar {
		return false
	}
	switch source.Format {
	case "date-time":
		return target.Format == "date-time"
	case "uuid":
		return target.Format == "" || target.Format == "uuid"
	default:
		return target.Scalar != serialization.String || target.Format == ""
	}
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
	case compositeExpression:
		return validateCompositeKey(e.parts, fields)
	case literalExpression:
		return validateLiteralKey(e)
	case pathExpression:
		return validatePathKey(e.text, expected, fields)
	default:
		return validateSimpleKey(e, optional)
	}
}

func validateSimpleKey(e expression, optional bool) error {
	if e.kind == emptyExpression && optional {
		return nil
	}
	if e.kind == sourceExpression {
		return nil
	}
	if e.kind == contextExpression {
		if _, ok := contextField(e.text); ok {
			return nil
		}
	}
	return invalid("invalid or unrepresentable correlation key")
}

func validateCompositeKey(parts []keyPart, fields []serialization.Field) error {
	if len(parts) == 0 {
		return invalid("invalid or unrepresentable correlation key")
	}
	seen := map[string]bool{}
	for _, part := range parts {
		// The kernel composite parser splits commas without balancing nested
		// expressions; nested composites cannot be represented faithfully.
		if !validCompositeKeyPart(part, seen) {
			return invalid("invalid or duplicate composite key part")
		}
		seen[part.name] = true
		if err := validateKey(part.expression, part.sourceType, fields, false); err != nil {
			return err
		}
	}
	return nil
}

func validCompositeKeyPart(part keyPart, seen map[string]bool) bool {
	return declarations.Path(part.name) && !seen[part.name] && part.expression.kind != compositeExpression
}

func validateLiteralKey(e expression) error {
	if validStringKey(e) {
		return nil
	}
	switch e.literalKind {
	case declarations.Boolean:
		return validateLiteral(e, serialization.Field{Type: reflect.TypeFor[bool](), Scalar: serialization.Boolean})
	case declarations.Number:
		return validateLiteral(e, serialization.Field{Type: reflect.TypeFor[int64](), Scalar: serialization.Integer})
	default:
		return invalid("invalid or unrepresentable correlation key")
	}
}

func validStringKey(e expression) bool {
	return e.literalKind == declarations.String && e.text != "" && representableLiteral(e.text)
}

func validatePathKey(path string, expected reflect.Type, fields []serialization.Field) error {
	field, ok := binaryMappingField(fields, path)
	if ok && binaryField(field) {
		return binaryUnsupported("binary correlation keys are not supported")
	}
	if validCorrelationPath(field, ok, path, expected) {
		return nil
	}
	return invalid("invalid or unrepresentable correlation key")
}

func validCorrelationPath(field serialization.Field, found bool, path string, expected reflect.Type) bool {
	return correlationPathField(field, found) && eventPropertyPath(path) && matchingKeyType(expected, field.Type)
}

func correlationPathField(field serialization.Field, found bool) bool {
	return found && !field.IsEnum() && !field.Collection && field.Scalar.IsPrimitive() && !field.Nullable
}

func matchingKeyType(expected, actual reflect.Type) bool {
	return expected == nil || expected == actual
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
