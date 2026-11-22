// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

// GenerationDelivery selects how the kernel delivers an observer's subscribed
// event generation. The zero value is Compatibility.
type GenerationDelivery uint8

const (
	// Compatibility retains legacy client-side selection from GenerationalContent,
	// with Content as the fallback when the subscribed generation is unavailable.
	Compatibility GenerationDelivery = iota
	// Pinned requests the subscribed generation as Content from the kernel. The
	// client decodes Content directly and preserves the delivered generation.
	Pinned
)
