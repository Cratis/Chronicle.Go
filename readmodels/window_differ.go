// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strconv"
)

// Unmarshal decodes a present model document into a pointer to its registered Go
// type, normalizing declared collections like Reader.Get. The result is owned.
// Runtime adapters can use this with a catalog; ordinary callers should use Reader.
func (d Descriptor) Unmarshal(data json.RawMessage) (any, error) {
	if d.GoType() == nil {
		return nil, notRegistered()
	}
	if !validDocument(bytes.TrimSpace(data)) {
		return nil, protocol("invalid model document")
	}
	data, err := normalizeID(data, d)
	if err != nil {
		return nil, err
	}
	value := reflect.New(d.GoType())
	if err := d.definition.plan.Unmarshal(data, value.Interface()); err != nil {
		return nil, err
	}
	return value.Interface(), nil
}

// WindowDiffer compares released, normalized model JSON by key. It is single-
// consumer state, scoped to one subscription. A removed item has no Value and
// only means it left this window, not that it was deleted from the sink.
// Unlike C#'s silent skip/overwrite, missing or duplicate keys fail explicitly.
// The zero value is usable; Reset is achieved by constructing a new differ.
type WindowDiffer struct {
	previous map[Key]string
	order    []Key
}

// Diff emits additions/modifications in current window order, then removals in
// previous order. Invalid windows leave the prior state intact. The registered
// key property takes precedence over C# id/_id/Id fallbacks.
func (d *WindowDiffer) Diff(model Descriptor, window []json.RawMessage) ([]Change[json.RawMessage], error) {
	current := make(map[Key]string, len(window))
	order := make([]Key, 0, len(window))
	changes := make([]Change[json.RawMessage], 0)
	for _, data := range window {
		value, err := model.Unmarshal(data)
		if err != nil {
			return nil, err
		}
		canonical, err := model.Marshal(value)
		if err != nil {
			return nil, err
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(canonical, &fields); err != nil {
			return nil, protocol("invalid window document")
		}
		key := ""
		for _, name := range []string{model.KeyProperty(), "id", "_id", "Id"} {
			if name != "" {
				key = windowKey(fields[name])
				if key != "" {
					break
				}
			}
		}
		if key == "" {
			return nil, protocol("materialized window requires a scalar key")
		}
		if _, exists := current[Key(key)]; exists {
			return nil, protocol("duplicate materialized window key")
		}
		current[Key(key)] = string(canonical)
		order = append(order, Key(key))
		previous, exists := d.previous[Key(key)]
		if !exists || previous != string(canonical) {
			kind := Modified
			if !exists {
				kind = Added
			}
			changes = append(changes, Change[json.RawMessage]{Type: kind, Key: Key(key), Value: canonical, HasValue: true})
		}
	}
	for _, key := range d.order {
		if _, exists := current[key]; !exists {
			changes = append(changes, Change[json.RawMessage]{Type: Removed, Key: key})
		}
	}
	d.previous, d.order = current, order
	return changes, nil
}

// windowKey matches JsonNode.ToString for admitted projection key scalars.
// Unlike a compliance subject, a projection key may be a boolean.
func windowKey(data json.RawMessage) string {
	var value any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return ""
	}
	switch value := value.(type) {
	case string:
		return value
	case json.Number:
		return value.String()
	case bool:
		return strconv.FormatBool(value)
	default:
		return ""
	}
}
