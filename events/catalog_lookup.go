// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

// LookupID returns the current (highest) registered generation for a persisted ID.
func (c *Catalog) LookupID(id TypeID) (Descriptor, bool) {
	descriptor, ok := c.current[id]
	return descriptor, ok
}

// LookupRef returns a descriptor only when both ID and generation match.
// Unlike C# GetClrTypeFor, an unknown generation never silently selects a codec
// for a different shape. LookupID explicitly requests the latest codec instead.
func (c *Catalog) LookupRef(ref TypeRef) (Descriptor, bool) {
	descriptor, ok := c.refs[ref]
	return descriptor, ok
}
