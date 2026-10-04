// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import (
	"context"

	"github.com/cratis/chronicle.go/internal/contentencoding"
	"github.com/cratis/chronicle.go/internal/preparation"
	"github.com/cratis/chronicle.go/serialization"
)

// EventContent is an expiring editor over declared outgoing event properties.
// See serialization.EventContent for name, type, ownership and omission rules.
type EventContent = serialization.EventContent

// EventEnricher runs synchronously after the entire original event is encoded.
// The context carries selected audit snapshots. Callbacks run in registration
// order (including duplicates), must support concurrent callers, and must not
// retain the editor. They are borrowed for the client lifetime, never closed.
// Errors and panics fail preparation without retaining their payloads. Callers
// own and must join outstanding preparation calls before disposing providers.
type EventEnricher func(context.Context, TypeRef, *EventContent) error

// PreparationError reports only the phase, zero-based provider and event indices,
// and whether a callback panicked. Index -1 means not applicable. It never wraps
// or retains application errors or panic values, including diagnostic hooks.
type PreparationError = preparation.Error

// EncodeOutgoing is the module-private outgoing content bridge. Enrichment never
// changes Descriptor.Marshal or invokes incoming decoding/schema callbacks.
func (d Descriptor) EncodeOutgoing(request contentencoding.Request[EventContent]) ([]byte, error) {
	request.ReadOnly = request.ReadOnly && d.subject != nil && !d.taggedSubject
	request.Immutable = d.subjectFields
	return d.plan.MarshalContent(request)
}
