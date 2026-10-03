// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import "github.com/cratis/chronicle.go/declarations"

func hasProjectionDirective(tag string) bool {
	directives, err := declarations.Parse(declarations.V1, tag)
	if err != nil {
		return true // Validated earlier; retain fail-closed behavior.
	}
	for _, directive := range directives {
		if directive.Name != "index" {
			return true
		}
	}
	return false
}
