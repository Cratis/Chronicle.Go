// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"encoding/json"
	"strings"
	"testing"
)

// Compare a leaf-only view of the immutable owning-package observation. Remove
// exactly the unsupported array property without rewriting any other JSON bytes,
// escaping or property order. This is not a new or hand-authored golden.
func binaryLeafCaptureJSON(t *testing.T, output json.RawMessage, policy string) string {
	t.Helper()
	var text string
	if err := json.Unmarshal(output, &text); err != nil {
		t.Fatal(err)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal([]byte(text), &object); err != nil {
		t.Fatal(err)
	}
	key := "Chunks"
	if strings.Contains(policy, "CamelCase") {
		key = "chunks"
	}
	if value, exists := object[key]; exists {
		property := ",\"" + key + "\":" + string(value)
		if strings.Count(text, property) != 1 {
			t.Fatal("capture property order changed")
		}
		text = strings.Replace(text, property, "", 1)
	}
	return text
}
