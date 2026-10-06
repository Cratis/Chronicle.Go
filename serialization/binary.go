// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"strings"

	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/internal/faults"
)

func encodeBinary(value reflect.Value) any {
	if value.IsNil() {
		return nil
	}
	return base64.StdEncoding.EncodeToString(value.Bytes())
}

func decodeBinary(data []byte, value reflect.Value) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		value.Set(reflect.MakeSlice(value.Type(), 0, 0))
		return nil
	}
	var text string
	if json.Unmarshal(data, &text) != nil || strings.ContainsAny(text, " \t\r\n") {
		return faults.ErrProtocol
	}
	decoded, err := base64.StdEncoding.Strict().DecodeString(text)
	if err != nil {
		// CorruptInputError and JSON error details never escape this boundary.
		return faults.ErrProtocol
	}
	value.SetBytes(decoded)
	return nil
}

func hasBinary(n *node) bool {
	found := false
	if n != nil {
		_ = visitNodes(n, map[*node]bool{}, func(child *node) error {
			found = found || child.binary
			return nil
		})
	}
	return found
}

type binaryPlacement struct {
	mapValue, derived, restricted bool
}

type binaryVisit struct {
	node *node
	binaryPlacement
}

func validateBinaryPlacement(root *node) error {
	var walk func(*node, binaryPlacement) error
	active := map[binaryVisit]bool{}
	walk = func(n *node, placement binaryPlacement) error {
		if n.reference != nil {
			n = n.reference
		}
		key := binaryVisit{n, placement}
		if active[key] {
			return nil
		}
		active[key] = true
		defer delete(active, key)
		if n.binary && (placement.mapValue || placement.derived || placement.restricted) {
			return unsupported(n.typ, "binary beneath maps, derivatives, protection or indexes is not supported")
		}
		if n.item != nil {
			if hasBinary(n.item) {
				if (n.typ.Kind() == reflect.Slice || n.typ.Kind() == reflect.Array) && (n.typ.Kind() != reflect.Slice || !n.item.binary) {
					return unsupported(n.typ, "binary collections require a single slice of binary leaves")
				}
				if n.typ.Kind() == reflect.Pointer && (n.item.typ.Kind() == reflect.Pointer || n.item.typ.Kind() == reflect.Slice && !n.item.binary) {
					return unsupported(n.typ, "binary requires a single nullable pointer or nonnullable binary array")
				}
			}
			child := placement
			child.mapValue = child.mapValue || n.typ.Kind() == reflect.Map
			if err := walk(n.item, child); err != nil {
				return err
			}
		}
		for _, f := range n.fields {
			child := placement
			if hasBinary(f.value) {
				directives, err := declarations.Parse(declarations.V1, f.tag)
				if err != nil {
					return err
				}
				for _, directive := range directives {
					child.restricted = child.restricted || directive.Name == "pii" || directive.Name == "encrypted" || directive.Name == "index"
				}
			}
			if err := walk(f.value, child); err != nil {
				return err
			}
		}
		for _, derivative := range n.derivatives {
			child := placement
			child.derived = true
			if err := walk(derivative.node, child); err != nil {
				return err
			}
		}
		return nil
	}
	return walk(root, binaryPlacement{})
}
