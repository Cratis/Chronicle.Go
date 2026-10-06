// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

// WithInitialValues snapshots a whole model through its registered codec when
// the option is applied. Repeated calls replace the state. Omitted state is {};
// nil properties follow the model serializer's omission rules. Use
// WithInitialValue for an explicit scalar null or zero on an omitted property.
// Initial values do not create an instance before kernel materialization.
// Protected roots cannot be initialized: definition state bypasses encryption.
func WithInitialValues[M any](value M) Option {
	return func(d *declaration) {
		if d.model.GoType() != reflect.TypeFor[M]() {
			d.err = invalid("initial values must match the model type")
			return
		}
		data, err := d.model.Marshal(value)
		if err != nil {
			d.err = invalid("initial values cannot be serialized")
			return
		}
		state, err := prepareInitialState(d.model, string(data))
		if err != nil {
			d.err = err
			return
		}
		d.initialState = state
	}
}

// WithInitialValue snapshots a typed scalar at an exact serialized model path.
// A nil scalar pointer is explicit JSON null. Paths cannot cross collections.
// Duplicate properties and ancestor/descendant collisions fail rather than
// silently overwriting. This Go-specific presence option shares the whole-model
// codec and canonical definition; it is not an event mapping or event filter.
func WithInitialValue[M, V any](target Field[M, V], value V) Option {
	return func(d *declaration) {
		field, ok := binaryMappingField(d.model.Fields(), target.path)
		if ok && binaryField(field) {
			d.err = binaryUnsupported("initial state cannot initialize a binary model root")
			return
		}
		if target.owner != d.model.GoType() || !ok || field.Type != reflect.TypeFor[V]() || field.Collection || !field.Scalar.IsPrimitive() {
			d.err = invalid("initial value requires a matching scalar model path")
			return
		}
		data, err := field.Marshal(value)
		if err != nil {
			d.err = invalid("initial value cannot be serialized")
			return
		}
		state := d.initialState
		if state == "" {
			state = "{}"
		}
		var object map[string]json.RawMessage
		if err := json.Unmarshal([]byte(state), &object); err != nil || object == nil {
			d.err = invalid("initial state requires an object")
			return
		}
		if err := insertInitialValue(object, strings.Split(target.path, "."), data); err != nil {
			d.err = err
			return
		}
		encoded, err := json.Marshal(object)
		if err != nil {
			d.err = invalid("initial state cannot be serialized")
			return
		}
		state, err = prepareInitialState(d.model, string(encoded))
		if err != nil {
			d.err = err
			return
		}
		d.initialState = state
	}
}

func insertInitialValue(object map[string]json.RawMessage, path []string, value json.RawMessage) error {
	if len(path) == 1 {
		if _, exists := object[path[0]]; exists {
			return invalid("duplicate or overlapping initial value")
		}
		object[path[0]] = value
		return nil
	}
	child := map[string]json.RawMessage{}
	if previous, exists := object[path[0]]; exists {
		if err := json.Unmarshal(previous, &child); err != nil || child == nil {
			return invalid("overlapping initial value")
		}
	}
	if err := insertInitialValue(child, path[1:], value); err != nil {
		return err
	}
	encoded, err := json.Marshal(child)
	if err != nil {
		return err
	}
	object[path[0]] = encoded
	return nil
}

// WithLabels appends artifact labels, deduplicating exact strings in first-seen
// order (C# GetTags/Distinct semantics). Labels never filter events. Input is
// copied; blank, invalid UTF-8 and control-containing labels fail preparation.
// Case and nonblank whitespace are preserved, not normalized.
func WithLabels(labels ...string) Option {
	owned := slices.Clone(labels)
	return func(d *declaration) {
		for _, label := range owned {
			if !utf8.ValidString(label) || strings.TrimSpace(label) == "" || strings.ContainsFunc(label, unicode.IsControl) {
				d.err = invalid("artifact labels must be nonblank UTF-8 without controls")
				return
			}
			if !slices.Contains(d.labels, label) {
				d.labels = append(d.labels, label)
			}
		}
	}
}

func prepareInitialState(model readmodels.Descriptor, state string) (string, error) {
	if state == "" {
		return "{}", nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(state), &object); err != nil || object == nil {
		return "", invalid("initial state requires a non-null object")
	}
	protected, err := serialization.ProtectionRoots(model.Schema())
	if err != nil {
		return "", err
	}
	for root := range protected {
		if _, exists := object[root]; exists {
			return "", invalid("initial state cannot initialize a protected model root")
		}
	}
	for _, field := range enumRootFields(model.Fields()) {
		if _, present := object[field.Name]; present && field.ContainsBinary() {
			return "", binaryUnsupported("initial state cannot initialize a binary model root")
		}
	}
	// Canonical JSON detaches all values and stabilizes definition identity.
	data, err := model.RebindJSON([]byte(state), model)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
