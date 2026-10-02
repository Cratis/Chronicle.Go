// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package first supplies a catalog-ambiguity fixture.
package first

// Opened has the same simple name as the independently registered second fixture.
type Opened struct {
	Name string `json:"name"`
}
