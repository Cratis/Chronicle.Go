// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package jsonescape matches System.Text.Json's JavaScriptEncoder.Default.
package jsonescape

import (
	"encoding/json"
	"unicode/utf16"
)

// Strings changes only string tokens in already-valid JSON. This normalizes
// scalar/concept codecs without calling them twice or decoding numbers (which
// would lose precision). Chronicle leaves Encoder unset in its default options.
func Strings(data []byte) ([]byte, error) {
	result := make([]byte, 0, len(data))
	for i := 0; i < len(data); {
		if data[i] != '"' {
			result = append(result, data[i])
			i++
			continue
		}
		start := i
		i++
		for i < len(data) && data[i] != '"' {
			if data[i] == '\\' {
				i++ // Skip an escaped quote or backslash, too.
			}
			i++
		}
		i++
		var value string
		if err := json.Unmarshal(data[start:i], &value); err != nil {
			return nil, err
		}
		result = AppendString(result, value)
	}
	return result, nil
}

// AppendString appends one JSON string token using JavaScriptEncoder.Default escaping.
func AppendString(data []byte, value string) []byte {
	data = append(data, '"')
	for _, r := range value {
		switch r {
		case '\b':
			data = append(data, '\\', 'b')
		case '\t':
			data = append(data, '\\', 't')
		case '\n':
			data = append(data, '\\', 'n')
		case '\f':
			data = append(data, '\\', 'f')
		case '\r':
			data = append(data, '\\', 'r')
		case '\\':
			data = append(data, '\\', '\\')
		default:
			if r >= ' ' && r <= '~' && r != '"' && r != '&' && r != '\'' && r != '+' && r != '<' && r != '>' && r != '`' {
				data = append(data, byte(r))
			} else if r <= 0xffff {
				data = appendUnicodeEscape(data, r)
			} else {
				high, low := utf16.EncodeRune(r)
				data = appendUnicodeEscape(appendUnicodeEscape(data, high), low)
			}
		}
	}
	return append(data, '"')
}

func appendUnicodeEscape(data []byte, r rune) []byte {
	const hex = "0123456789ABCDEF"
	return append(data, '\\', 'u', hex[r>>12&15], hex[r>>8&15], hex[r>>4&15], hex[r&15])
}
