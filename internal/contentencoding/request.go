// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package contentencoding carries the module-private outgoing encoder seam.
package contentencoding

// Request is an outgoing-only request; ordinary plan serialization is unaffected.
type Request[T any] struct {
	Value     any
	ReadOnly  bool
	Immutable map[string]bool
	Providers []func(*T) error
	Failed    func(int, bool, bool) error
}
