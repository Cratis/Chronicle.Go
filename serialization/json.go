// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"encoding/json"

	"github.com/cratis/chronicle.go/internal/jsonescape"
)

// Structs retain the plan's field order; dictionaries still use encoding/json's
// deterministic key ordering. Keep the standard encoder's scalar/error handling.
type objectProperty struct {
	name  string
	value any
}
type orderedObject []objectProperty

func (object orderedObject) MarshalJSON() ([]byte, error) {
	data := []byte{'{'}
	for i, property := range object {
		if i != 0 {
			data = append(data, ',')
		}
		name, err := json.Marshal(property.name)
		if err != nil {
			return nil, err
		}
		value, err := json.Marshal(property.value)
		if err != nil {
			return nil, err
		}
		data = append(data, name...)
		data = append(data, ':')
		data = append(data, value...)
	}
	return append(data, '}'), nil
}

// escapeJSONStrings changes only string tokens in already-valid JSON. This also
// normalizes supported scalar/concept codecs without calling them a second time
// or decoding numbers (which would lose precision). Chronicle's default options
// leave Encoder unset: System.Text.Json uses JavaScriptEncoder.Default.
func escapeJSONStrings(data []byte) ([]byte, error) {
	return jsonescape.Strings(data)
}

func appendJSONString(data []byte, value string) []byte {
	return jsonescape.AppendString(data, value)
}
