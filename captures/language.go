// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package captures

import (
	"strings"
	"unicode"
)

// Screenplay 4.16.0 CaptureParser uses .NET's non-ECMAScript \w for word
// continuations: letters, nonspacing marks, decimal digits and connectors.
// The regex matches UTF-16 code units, so supplementary-plane letters are not
// word characters (their surrogate code units are not in these categories).
func validWord(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r > 0xffff || (!unicode.IsLetter(r) && !unicode.Is(unicode.Mn, r) && !unicode.Is(unicode.Nd, r) && !unicode.Is(unicode.Pc, r)) {
			return false
		}
	}
	return true
}
func validCaptureName(value string) bool {
	return validWord(value) && ((value[0] >= 'A' && value[0] <= 'Z') || (value[0] >= 'a' && value[0] <= 'z') || value[0] == '_')
}
func validEventID(value string) bool {
	return validWord(value) && value[0] >= 'A' && value[0] <= 'Z'
}
func validMapTarget(value string) bool {
	return validWord(value) && ((value[0] >= 'a' && value[0] <= 'z') || value[0] == '_')
}
func validLiteral(value string) bool { return !strings.ContainsRune(value, '\x00') }

// quoteLiteral matches Screenplay Text/StringLiteral.Quote's small escape set;
// strconv.Quote would emit unsupported Go escapes such as \x and \u.
func quoteLiteral(value string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`, "\r", `\r`, "\t", `\t`).Replace(value) + `"`
}
func propertyOperand(path string) string {
	if path == "added" || path == "removed" {
		return quoteLiteral(path)
	}
	return path
}
