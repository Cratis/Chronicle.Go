// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package jsonescape_test

import (
	"bytes"
	"testing"

	"github.com/cratis/chronicle.go/internal/jsonescape"
)

func TestStringsMatchesJavaScriptEncoderDefault(t *testing.T) {
	cases := []struct{ name, input, want string }{
		{"ASCII", `{"key":"synthetic-value"}`, `{"key":"synthetic-value"}`},
		{"sensitive ASCII", `"\"&'+<>` + "`" + `"`, `"\u0022\u0026\u0027\u002B\u003C\u003E\u0060"`},
		{"Unicode", `"é😀\u2028\u2029"`, `"\u00E9\uD83D\uDE00\u2028\u2029"`},
		{"controls", `"\b\t\n\f\r\\\u0000"`, `"\b\t\n\f\r\\\u0000"`},
		{"escaped input", `"\u003c\u0022"`, `"\u003C\u0022"`},
		{"scalars unchanged", `{"é":[9007199254740993,1.2300e+90,true,null]}`, `{"\u00E9":[9007199254740993,1.2300e+90,true,null]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := []byte(tc.input)
			original := bytes.Clone(input)
			got, err := jsonescape.Strings(input)
			if err != nil || string(got) != tc.want || !bytes.Equal(input, original) {
				t.Fatal("escaped JSON differs or input changed")
			}
		})
	}
}
