// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package constraints

import (
	"maps"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
)

// EventFields identifies the ordered properties forming a composite unique value
// on one event type. Different event types may use different property paths.
type EventFields struct {
	// Event is the immutable registered event descriptor.
	Event events.Descriptor
	// Properties contains serialized JSON paths, in composite-key order.
	Properties []string
}

// Scope selects additional dimensions of a uniqueness claim. Source ID always
// identifies the owner; it is not an extra scope dimension. The zero value adds
// no dimensions. Every namespace and sequence has its own index independently.
type Scope struct {
	// PerEventSourceType includes the appending event's source type.
	PerEventSourceType bool
	// PerEventStreamType includes the appending event's stream type.
	PerEventStreamType bool
	// PerEventStreamID includes the appending event's stream ID.
	PerEventStreamID bool
}

// MessageProvider returns a client-side template for a violation. Returning empty
// preserves the kernel message. Providers must be safe for concurrent invocation.
type MessageProvider func(Violation) string

// Definition is an immutable kernel constraint declaration returned by Build.
// Its zero value is invalid. Accessors return defensive copies; message callbacks
// are retained by reference and must synchronize any captured mutable state.
type Definition struct {
	name            string
	kind            Type
	fields          []EventFields
	types           []events.Descriptor
	removers        []events.Descriptor
	scope           Scope
	sequences       []events.SequenceID
	ignoreCasing    bool
	message         MessageProvider
	messageProvider bool
}

// Name returns the stable constraint identity.
func (d Definition) Name() string { return d.name }

// Kind returns Unique or UniqueEventType for a built definition.
func (d Definition) Kind() Type { return d.kind }

// Fields returns ordered event/property declarations; empty for event-type constraints.
func (d Definition) Fields() []EventFields {
	fields := slices.Clone(d.fields)
	for i := range fields {
		fields[i].Properties = slices.Clone(fields[i].Properties)
	}
	return fields
}

// EventTypes returns participating event types in declaration order.
func (d Definition) EventTypes() []events.Descriptor { return slices.Clone(d.types) }

// RemovalTypes returns event types that release the owner's claim or lifecycle.
func (d Definition) RemovalTypes() []events.Descriptor { return slices.Clone(d.removers) }

// Scope returns the selected additional dimensions.
func (d Definition) Scope() Scope { return d.scope }

// EventSequences returns selected sequences; empty means all sequences.
func (d Definition) EventSequences() []events.SequenceID { return slices.Clone(d.sequences) }

// IgnoresCasing reports whether property comparisons ignore casing in the kernel.
func (d Definition) IgnoresCasing() bool { return d.ignoreCasing }

// HasMessageProvider distinguishes runtime callbacks from static WithMessage
// templates. Temporary definition factories cannot retain runtime collaborators.
func (d Definition) HasMessageProvider() bool { return d.messageProvider }

// ResolveMessage applies the definition's provider and {DetailKey} substitution
// only to a matching violation. Empty templates preserve the kernel message.
// Neither the callback nor mutation of the returned details can modify the input.
// Values containing placeholder text are inserted literally, not recursively.
func (d Definition) ResolveMessage(violation Violation) Violation {
	violation.Details = maps.Clone(violation.Details)
	if d.message == nil || violation.ConstraintName != d.name {
		return violation
	}
	input := violation
	input.Details = maps.Clone(input.Details)
	message := d.message(input)
	if message == "" {
		return violation
	}
	keys := slices.Sorted(maps.Keys(violation.Details))
	pairs := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		pairs = append(pairs, "{"+key+"}", violation.Details[key])
	}
	message = strings.NewReplacer(pairs...).Replace(message)
	if message != "" {
		violation.Message = message
	}
	return violation
}
