// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"

	"github.com/cratis/chronicle.go/internal/faults"
)

func (n *node) encodeEnum(value reflect.Value) (any, error) {
	integer := int32(value.Int())
	if !n.enum.values[integer] {
		return nil, unsupported(n.typ, "undeclared enum value")
	}
	// Never return the named application value to encoding/json: it may have
	// MarshalJSON/MarshalText methods, which are not part of this profile.
	return integer, nil
}

func (n *node) decodeEnum(data []byte, value reflect.Value) error {
	text := string(bytes.TrimSpace(data))
	var integer int32
	var ok bool
	if len(text) > 0 && text[0] == '"' {
		var name string
		if json.Unmarshal(data, &name) != nil {
			return faults.ErrProtocol
		}
		integer, ok = n.enum.readString(name)
	} else {
		integer, ok = enumInteger(text, false)
	}
	if !ok || !n.enum.values[integer] {
		return faults.ErrProtocol
	}
	value.SetInt(int64(integer))
	return nil
}

func enumInteger(text string, stringToken bool) (int32, bool) {
	if text == "" {
		return 0, false
	}
	digits := text
	if text[0] == '-' || stringToken && text[0] == '+' {
		digits = text[1:]
	}
	if digits == "" {
		return 0, false
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	if !stringToken && len(digits) > 1 && digits[0] == '0' {
		return 0, false
	}
	integer, err := strconv.ParseInt(text, 10, 32)
	return int32(integer), err == nil
}

func (e *enumDefinition) readString(text string) (int32, bool) {
	// Deliberately qualify ASCII spaces only, not every Unicode whitespace
	// accepted by Enum.TryParse. JSON escape decoding happens before this step.
	text = strings.Trim(text, " ")
	if integer, ok := enumInteger(text, true); ok {
		return integer, true
	}
	var combined int32
	for _, part := range strings.Split(text, ",") {
		name := strings.Trim(part, " ")
		if !enumName(name) {
			return 0, false
		}
		integer, ok := e.names[strings.ToLower(name)]
		if !ok {
			return 0, false
		}
		combined |= integer
	}
	return combined, true
}

func (n *node) decodeMissingEnum(value reflect.Value) error {
	if n.enum != nil && !n.enum.values[0] {
		return faults.ErrProtocol
	}
	if n.typ.Kind() == reflect.Slice && n.item.enum != nil {
		value.Set(reflect.MakeSlice(n.typ, 0, 0))
	}
	return nil
}
