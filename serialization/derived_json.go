// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"

	"github.com/cratis/chronicle.go/internal/faults"
)

// Check the dynamic family identity before an omitzero callback can hide an
// unregistered implementation. Ordinary composite omission is still caller policy.
func (n *node) validateFamilyValue(value reflect.Value) error {
	if n.reference != nil {
		return n.reference.validateFamilyValue(value)
	}
	if n.typ.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil
		}
		return n.item.validateFamilyValue(value.Elem())
	}
	if !n.family || value.IsNil() {
		return nil
	}
	concrete := value.Elem()
	if concrete.Kind() == reflect.Pointer && concrete.IsNil() {
		return unsupported(n.typ, "typed-nil derivative")
	}
	for _, derivative := range n.derivatives {
		if derivative.registration.concrete == concrete.Type() {
			return nil
		}
	}
	return unsupported(n.typ, "unregistered dynamic derivative")
}

// Inspect the exact member before any map decoding can discard duplicate keys.
func discriminator(data []byte) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	if err != nil {
		return "", err
	}
	if token != json.Delim('{') {
		return "", faults.ErrProtocol
	}
	found, id := false, ""
	for decoder.More() {
		token, err = decoder.Token()
		if err != nil {
			return "", err
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return "", err
		}
		if token != derivedTypeID {
			continue
		}
		if found || len(raw) == 0 || raw[0] != '"' {
			return "", faults.ErrProtocol
		}
		found = true
		if err := json.Unmarshal(raw, &id); err != nil {
			return "", err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return "", err
	}
	if _, err := decoder.Token(); err != io.EOF || !found {
		return "", faults.ErrProtocol
	}
	return id, nil
}

func (n *node) decodeFamily(data []byte, value reflect.Value, depth int) error {
	id, err := discriminator(data)
	if err != nil {
		return err
	}
	for _, derivative := range n.derivatives {
		if derivative.registration.id != id {
			continue
		}
		concrete := reflect.New(derivative.registration.concrete).Elem()
		if err := derivative.node.decodeContext(data, concrete, depth+1, true); err != nil {
			return err
		}
		value.Set(concrete)
		return nil
	}
	return faults.ErrProtocol
}
