// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package discovery shares C# handler precedence and catalog-scoped event families.
package discovery

import (
	"reflect"
	"slices"
	"unicode/utf16"

	"github.com/cratis/chronicle.go/events"
)

// Methods returns exported methods richest-signature first, then ordinal name.
func Methods(typ reflect.Type) []reflect.Method {
	methods := make([]reflect.Method, typ.NumMethod())
	for i := range methods {
		methods[i] = typ.Method(i)
	}
	slices.SortFunc(methods, func(a, b reflect.Method) int {
		if a.Type.NumIn() != b.Type.NumIn() {
			return b.Type.NumIn() - a.Type.NumIn()
		}
		return slices.Compare(utf16.Encode([]rune(a.Name)), utf16.Encode([]rune(b.Name)))
	})
	return methods
}

// Events selects registered concrete members of an event type or interface family.
func Events(typ reflect.Type, catalog *events.Catalog) []events.Descriptor {
	if typ.Kind() == reflect.Interface && typ.NumMethod() == 0 {
		return nil
	}
	var result []events.Descriptor
	for _, d := range catalog.Descriptors() {
		if d.GoType().AssignableTo(typ) || reflect.PointerTo(d.GoType()).AssignableTo(typ) {
			result = append(result, d)
		}
	}
	return result
}

// EventArgument adapts E/*E or an event family without constructing a new event
// except when a pointer receiver is required for a value supplied by a caller.
func EventArgument(content any, typ reflect.Type) (reflect.Value, bool) {
	value := reflect.ValueOf(content)
	if !value.IsValid() {
		return value, false
	}
	if !value.Type().AssignableTo(typ) {
		if value.Kind() == reflect.Pointer && !value.IsNil() {
			value = value.Elem()
		} else if reflect.PointerTo(value.Type()).AssignableTo(typ) {
			pointer := reflect.New(value.Type())
			pointer.Elem().Set(value)
			value = pointer
		}
	}
	return value, value.Type().AssignableTo(typ) && (value.Kind() != reflect.Pointer || !value.IsNil())
}
