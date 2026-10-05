// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package jsonstructure_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/jsonstructure"
)

func TestValidateRejectsAmbiguousOrMalformedJSONWithoutPayloadDiagnostics(t *testing.T) {
	for name, input := range map[string]string{
		"ordinary parent": `{"secret":{"x":1,"x":2},"secret":{}}`,
		"escaped name":    `{"secret":1,"\u0073ecret":2}`,
		"array element":   `[[{"secret":1,"secret":2}]]`,
		"multiple values": `{} {"secret":1}`,
		"malformed":       `{"secret":]}`,
		"empty":           ``,
		"unclosed object": `{"secret":1`,
		"unclosed array":  `[1`,
		"trailing comma":  `{"secret":1,}`,
		"depth":           strings.Repeat("[", jsonstructure.MaxDepth+1) + "0" + strings.Repeat("]", jsonstructure.MaxDepth+1),
	} {
		t.Run(name, func(t *testing.T) {
			err := jsonstructure.Validate([]byte(input))
			if !errors.Is(err, faults.ErrProtocol) || strings.Contains(err.Error(), "secret") {
				t.Fatalf("unsafe validation error: %v", err)
			}
		})
	}
}

func TestValidateKeepsExactNamesAndNumberLexemes(t *testing.T) {
	for _, input := range []string{
		`{"name":1,"Name":2,"NAME":3}`,
		`{"a":{"name":1},"b":[{"name":2},{"name":3}]}`,
		`{"integer":9007199254740993,"huge":1e9999}`,
		`[null,true,false,"text",-1.2e-3]`,
		strings.Repeat("[", jsonstructure.MaxDepth) + "0" + strings.Repeat("]", jsonstructure.MaxDepth),
	} {
		if err := jsonstructure.Validate([]byte(input)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestValidateBoundsInputSizeBeforeDecoding(t *testing.T) {
	input := make([]byte, jsonstructure.MaxBytes+1)
	if err := jsonstructure.Validate(input); !errors.Is(err, faults.ErrProtocol) {
		t.Fatalf("oversized input: %v", err)
	}
}

func FuzzValidate(f *testing.F) {
	for _, input := range []string{`{}`, `{"a":1,"\u0061":2}`, `[[{"a":1,"a":2}]]`, `{"Name":1,"name":2}`, `{"secret":]}`} {
		f.Add([]byte(input))
	}
	f.Fuzz(func(t *testing.T, input []byte) {
		if err := jsonstructure.Validate(input); err != nil && err != faults.ErrProtocol {
			t.Fatal("validator exposed a payload-dependent error")
		}
	})
}
