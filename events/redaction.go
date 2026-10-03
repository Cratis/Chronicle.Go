// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/google/uuid"
)

// RedactionReason explains why event content must be removed. It must be nonblank
// and must not itself contain the personal data or secret being removed.
type RedactionReason string

// RedactedTypeID is the kernel's synthetic replacement event type.
const RedactedTypeID TypeID = "EventRedacted"

// EventRedacted is the replacement payload, not the redaction command. Its audit
// fields describe the ORIGINAL event. The enclosing Appended.Context describes
// the redaction operation. OriginalEventType retains the stable ID even when the
// original Go type is unknown. Register this type explicitly to handle markers in
// a reactor; direct reads decode it without registration.
type EventRedacted struct {
	// Reason is the supplied explanation for removing the payload.
	Reason RedactionReason `json:"reason"`
	// OriginalEventType identifies the event before redaction, without a generation.
	OriginalEventType TypeID `json:"originalEventType"`
	// Occurred is when the original event occurred.
	Occurred time.Time `json:"occurred"`
	// CorrelationID identifies the original append, not the redaction request.
	CorrelationID uuid.UUID `json:"correlationId"`
	// Causation preserves the original audit chain as returned by the kernel.
	Causation []RedactionCausation `json:"causation"`
	// CausedBy contains original persisted identity IDs, not identity subjects.
	CausedBy []uuid.UUID `json:"causedBy"`
}

// RedactionCausation preserves one original audit link from a marker. Newer
// kernels remove Properties; older kernels may retain them. Do not log payloads.
type RedactionCausation struct {
	// Occurred is when this cause occurred.
	Occurred time.Time `json:"occurred"`
	// Type is the kind of cause.
	Type string `json:"type"`
	// Properties contains any properties actually returned by the kernel.
	Properties map[string]string `json:"properties"`
}

// OriginalType resolves the current registered Go type, or any for an unknown ID
// (C#'s object fallback). A nil catalog also returns any; no generation is guessed.
func (r EventRedacted) OriginalType(catalog *Catalog) reflect.Type {
	if catalog != nil {
		if descriptor, ok := catalog.LookupID(r.OriginalEventType); ok {
			return descriptor.GoType()
		}
	}
	return reflect.TypeFor[any]()
}

// Redaction decodes the kernel marker without registering a user event codec.
// Non-marker events, malformed payloads and missing reasons/type IDs fail with
// ErrProtocol. The returned slices/maps are owned by the caller. Unknown original
// IDs are preserved, never decoded as a registered original event with zero data.
func (a Appended) Redaction() (EventRedacted, error) {
	var result EventRedacted
	if a.Context.EventType.ID != RedactedTypeID {
		return result, fmt.Errorf("%w: event is not a redaction marker", faults.ErrProtocol)
	}
	if err := json.Unmarshal(a.Content, &result); err != nil {
		return EventRedacted{}, fmt.Errorf("%w: invalid redaction marker: %v", faults.ErrProtocol, err)
	}
	if strings.TrimSpace(string(result.Reason)) == "" || strings.TrimSpace(string(result.OriginalEventType)) == "" {
		return EventRedacted{}, fmt.Errorf("%w: redaction marker requires reason and original type", faults.ErrProtocol)
	}
	return result, nil
}
