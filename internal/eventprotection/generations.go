// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventprotection

import (
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

// IdentityProtected inspects every matching registered generation's frozen
// schema. An absent identity or an invalid schema fails closed. The predicate
// keeps this internal helper independent of the events package that uses it.
func IdentityProtected[D interface{ Schema() string }](descriptors []D, matches func(D) bool) (bool, error) {
	found, protected := false, false
	for _, descriptor := range descriptors {
		if !matches(descriptor) {
			continue
		}
		found = true
		roots, err := serialization.ProtectionRoots(descriptor.Schema())
		if err != nil {
			return false, err
		}
		protected = protected || len(roots) > 0
	}
	if !found {
		return false, faults.ErrInvalidConfiguration
	}
	return protected, nil
}
