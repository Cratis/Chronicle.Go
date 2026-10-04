// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"encoding/json"
	"reflect"
	"slices"
	"sync"
)

// EventContent is an expiring, schema-bound editor for outgoing event content.
// Applications receive it through events.EventEnricher; its zero value is invalid.
// Names are exact top-level serialized JSON names, never Go names or paths.
// Each callback receives a new handle over the same ordered document. Handles
// expire on callback return, including failure/panic. Methods are concurrency-safe;
// a setter already encoding when the handle expires cannot publish its result.
// Values passed to Set must not be mutated concurrently with that call.
type EventContent struct{ editor *contentEditor }

type contentEditor struct {
	mu         sync.Mutex
	active     bool
	failure    error
	fields     []field
	properties []contentProperty
	readOnly   bool
	immutable  map[string]bool
}

type contentProperty struct {
	name string
	data json.RawMessage
}

type contentFailure struct{ panicked bool }

func (*contentFailure) Error() string { return "chronicle: invalid or expired event content operation" }

var errContent = &contentFailure{}

// Get returns an owned copy, or false for an omitted declared field. Unknown
// names and expired handles return an error. Reading never mutates the document.
func (handle *EventContent) Get(name string) (json.RawMessage, bool, error) {
	if handle == nil || handle.editor == nil {
		return nil, false, errContent
	}
	c := handle.editor
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.active || c.field(name) == nil {
		return nil, false, errContent
	}
	for _, property := range c.properties {
		if property.name == name {
			return slices.Clone(property.data), true, nil
		}
	}
	return nil, false, nil
}

// Set encodes value immediately using the field's exact declared Go type and
// frozen codec plan. Untyped nil and raw JSON substitutions are not accepted.
// A typed nil follows ordinary object omission. Replacement retains position;
// adding an omitted field appends it. Errors latch even when the caller ignores
// them. Tagged subject fields and opaque subject dependencies cannot be changed.
func (handle *EventContent) Set(name string, value any) error {
	if handle == nil || handle.editor == nil {
		return errContent
	}
	c := handle.editor
	c.mu.Lock()
	f, err := c.mutableField(name)
	c.mu.Unlock()
	if err != nil {
		return err
	}
	// User concept/IsZero hooks must never execute under the publication lock.
	data, omit, err := encodeContentField(*f, value)
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.active {
		return errContent
	}
	if err != nil {
		c.failure = err
		return err
	}
	for i, property := range c.properties {
		if property.name != name {
			continue
		}
		if omit {
			c.properties = slices.Delete(c.properties, i, i+1)
		} else {
			c.properties[i].data = data
		}
		return nil
	}
	if !omit {
		c.properties = append(c.properties, contentProperty{name, data})
	}
	return nil
}

// Remove omits a field only when its compiled Go encoder can omit it. A nullable
// schema alone is insufficient. Removing an absent optional field is a no-op.
func (handle *EventContent) Remove(name string) error {
	if handle == nil || handle.editor == nil {
		return errContent
	}
	c := handle.editor
	c.mu.Lock()
	defer c.mu.Unlock()
	f, err := c.mutableField(name)
	if err != nil {
		return err
	}
	if !canOmit(*f) {
		c.failure = errContent
		return errContent
	}
	for i, property := range c.properties {
		if property.name == name {
			c.properties = slices.Delete(c.properties, i, i+1)
			break
		}
	}
	return nil
}

func (c *contentEditor) field(name string) *field {
	for i := range c.fields {
		if c.fields[i].name == name {
			return &c.fields[i]
		}
	}
	return nil
}
func (c *contentEditor) mutableField(name string) (*field, error) {
	if !c.active {
		return nil, errContent
	}
	f := c.field(name)
	if f == nil || c.readOnly || c.immutable[name] || name == derivedTypeID {
		c.failure = errContent
		return nil, errContent
	}
	return f, nil
}
func canOmit(f field) bool {
	if f.omitEmpty || f.omitZero || f.optionalAncestor {
		return true
	}
	switch f.value.typ.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Interface:
		return true
	}
	return false
}
func encodeContentField(f field, value any) (data json.RawMessage, omit bool, err error) {
	defer func() {
		if recover() != nil {
			data, err = nil, &contentFailure{panicked: true}
		}
	}()
	v := reflect.ValueOf(value)
	if !v.IsValid() {
		return nil, false, errContent
	}
	if f.value.typ.Kind() == reflect.Interface && v.Type().Implements(f.value.typ) {
		declared := reflect.New(f.value.typ).Elem()
		declared.Set(v)
		v = declared
	}
	if v.Type() != f.value.typ {
		return nil, false, errContent
	}
	if err := f.value.validateFamilyValue(v); err != nil {
		return nil, false, errContent
	}
	if (f.omitEmpty && empty(v)) || (f.omitZero && f.isZero(v)) {
		return nil, true, nil
	}
	state := &encodeState{active: map[valueVisit]bool{}, privateCallbacks: true}
	encoded, err := f.value.encode(v, false, state, 0)
	if err != nil {
		return nil, false, &contentFailure{panicked: state.panicked}
	}
	if encoded == nil {
		return nil, true, nil
	}
	data, err = json.Marshal(encoded)
	if err != nil {
		return nil, false, errContent
	}
	data, err = escapeJSONStrings(data)
	if err != nil {
		return nil, false, errContent
	}
	return data, false, nil
}
