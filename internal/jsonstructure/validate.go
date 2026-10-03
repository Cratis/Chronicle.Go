// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package jsonstructure validates untrusted JSON before any lossy map decoding.
package jsonstructure

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/cratis/chronicle.go/internal/faults"
)

// MaxBytes matches the client's maximum gRPC message size. MaxDepth matches the
// serialization graph's nesting bound; unknown properties obey it as well.
const (
	MaxBytes = 100 * 1024 * 1024
	MaxDepth = 256
)

// Validate checks one complete JSON value, rejecting duplicate decoded member
// names in every object, including maps and unknown properties inside arrays.
// Names are case-sensitive, with JSON escapes resolved by the standard decoder.
// It invokes no application code and returns only a payload-free protocol error.
func Validate(data []byte) error {
	if len(data) > MaxBytes {
		return faults.ErrProtocol
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if !uniqueValue(decoder, 0) {
		return faults.ErrProtocol
	}
	if _, err := decoder.Token(); err != io.EOF {
		return faults.ErrProtocol
	}
	return nil
}

func uniqueValue(decoder *json.Decoder, depth int) bool {
	if depth > MaxDepth {
		return false
	}
	token, err := decoder.Token()
	if err != nil {
		return false
	}
	delimiter, composite := token.(json.Delim)
	if !composite {
		return true
	}
	switch delimiter {
	case '{':
		seen := make(map[string]bool)
		for decoder.More() {
			token, err := decoder.Token()
			name, ok := token.(string)
			if err != nil || !ok || seen[name] {
				return false
			}
			seen[name] = true
			if !uniqueValue(decoder, depth+1) {
				return false
			}
		}
	case '[':
		for decoder.More() {
			if !uniqueValue(decoder, depth+1) {
				return false
			}
		}
	default:
		return false
	}
	closing, err := decoder.Token()
	return err == nil && ((delimiter == '{' && closing == json.Delim('}')) || (delimiter == '[' && closing == json.Delim(']')))
}
