// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

// SourceName names a registered event source definition, for example one
// registered through the eventsources package. It is distinct from SourceID,
// which identifies the entity whose event is appended, and from SourceType,
// which classifies the append route. The kernel resolves a nonempty name against
// the store's registered definitions and persists it as the event's source type.
// The zero value means the event carries no named-source attribution.
type SourceName string

// IsSet reports whether the name carries attribution.
func (n SourceName) IsSet() bool { return n != "" }
