// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package second supplies a catalog-ambiguity fixture.
package second

// Opened has the same simple name as the independently registered first fixture.
type Opened struct {
	Name string `json:"name"`
}
