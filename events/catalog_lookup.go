// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

// LookupID returns the currently registered descriptor for a persisted type ID.
// It performs no I/O. Catalogs currently register only one generation per ID.
func (c *Catalog) LookupID(id TypeID) (Descriptor, bool) {
	for _, descriptor := range c.ordered {
		if descriptor.Ref().ID == id {
			return descriptor, true
		}
	}
	return Descriptor{}, false
}

// LookupRef returns a descriptor only when both ID and generation match.
func (c *Catalog) LookupRef(ref TypeRef) (Descriptor, bool) {
	descriptor, ok := c.LookupID(ref.ID)
	if !ok || descriptor.Ref() != ref {
		return Descriptor{}, false
	}
	return descriptor, true
}
