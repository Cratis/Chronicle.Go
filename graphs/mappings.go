// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package graphs

import (
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"time"
)

type mappingKind uint8

const (
	setFromEvent mappingKind = iota
	setFromContext
	setFromEventSourceID
	setLiteral
	addFromEvent
	subtractFromEvent
	increment
	decrement
	count
	clearProperty
)

// mapping is one property assignment. source is an event or context path; the
// literal is already encoded, or literalErr explains why it cannot be.
type mapping struct {
	property   string
	kind       mappingKind
	source     string
	literal    string
	literalErr error
}

func (m mapping) applyNodeFrom(f *nodeFrom) { f.mappings = append(f.mappings, m) }
func (m mapping) applyEdgeFrom(f *edgeFrom) { f.mappings = append(f.mappings, m) }

// Set assigns the property of the node or edge from an event property. The
// event property is a serialized event path of a primitive value. Properties of
// the node or edge that the event has under the same name are mapped without it.
func Set(property, eventProperty string) MappingOption {
	return mapping{property: property, kind: setFromEvent, source: eventProperty}
}

// SetFromContext assigns the property from the event context, for example
// "occurred", "eventSourceId", "sequenceNumber" or "correlationId".
func SetFromContext(property, contextProperty string) MappingOption {
	return mapping{property: property, kind: setFromContext, source: contextProperty}
}

// SetFromEventSourceID assigns the property from the event source id.
func SetFromEventSourceID(property string) MappingOption {
	return mapping{property: property, kind: setFromEventSourceID}
}

// SetValue assigns a constant. Supported values are strings, booleans, integers,
// floating point numbers and time.Time values; nil clears the property. The
// kernel's literal grammar cannot represent every string: commas, semicolons,
// parentheses, quotes and line breaks make the declaration invalid.
func SetValue(property string, value any) MappingOption {
	text, err := literal(value)
	return mapping{property: property, kind: setLiteral, literal: text, literalErr: err}
}

// Add adds an event property to the property, as a running total.
func Add(property, eventProperty string) MappingOption {
	return mapping{property: property, kind: addFromEvent, source: eventProperty}
}

// Subtract subtracts an event property from the property, as a running total.
func Subtract(property, eventProperty string) MappingOption {
	return mapping{property: property, kind: subtractFromEvent, source: eventProperty}
}

// Increment increments the numeric property by one for every event.
func Increment(property string) MappingOption { return mapping{property: property, kind: increment} }

// Decrement decrements the numeric property by one for every event.
func Decrement(property string) MappingOption { return mapping{property: property, kind: decrement} }

// Count counts the events on the numeric property.
func Count(property string) MappingOption { return mapping{property: property, kind: count} }

// Clear sets the property back to no value.
func Clear(property string) MappingOption { return mapping{property: property, kind: clearProperty} }

// The kernel's literal grammar admits letters, digits, connector punctuation,
// spaces and ._/:*+- only. The expression is unanchored in the kernel, so it is
// anchored here to refuse any partial match.
var kernelLiteral = regexp.MustCompile(`^[\p{L}\p{Mn}\p{Nd}\p{Pc} ._/:*+\-]*$`)

func literal(value any) (string, error) {
	if value == nil {
		return "$null", nil
	}
	if at, ok := value.(time.Time); ok {
		return checked(at.UTC().Format(time.RFC3339Nano))
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.String:
		return checked(v.String())
	case reflect.Bool:
		if v.Bool() {
			return "$value(True)", nil
		}
		return "$value(False)", nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "$value(" + strconv.FormatInt(v.Int(), 10) + ")", nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "$value(" + strconv.FormatUint(v.Uint(), 10) + ")", nil
	case reflect.Float32, reflect.Float64:
		number := v.Float()
		if math.IsNaN(number) || math.IsInf(number, 0) {
			return "", fmt.Errorf("the literal is not a finite number")
		}
		return checked(strconv.FormatFloat(number, 'f', -1, v.Type().Bits()))
	default:
		return "", fmt.Errorf("the literal type %s is not supported", v.Type())
	}
}

func checked(text string) (string, error) {
	for _, r := range text {
		if r > 0xffff {
			return "", fmt.Errorf("the literal cannot be represented by the kernel expression grammar")
		}
	}
	if !kernelLiteral.MatchString(text) {
		return "", fmt.Errorf("the literal cannot be represented by the kernel expression grammar")
	}
	return "$value(" + text + ")", nil
}

// The kernel embeds the event path of an arithmetic expression in a stricter
// ASCII grammar than the one for plain paths.
var arithmeticPath = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9.]*$`)
