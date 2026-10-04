// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

// IsVariant reports whether this compiled definition belongs to a VariantOf
// group, including a group with only one member. It reads immutable SDK compiler
// metadata, not the wire shape: an ordinary definition can have identical wire
// fields. GlobalFor declarations contribute mappings but are not emitted as
// definitions. The zero definition is invalid and returns false.
func (d Definition) IsVariant() bool { return d.data != nil && d.data.variant != nil }
