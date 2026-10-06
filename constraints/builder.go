// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package constraints

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

// Builder declares kernel-enforced uniqueness; it never checks local event data.
// Use UniqueValues or UniqueEventTypes. Builders are not concurrency-safe. Build
// validates and snapshots the declaration; subsequent edits cannot change it.
type Builder struct {
	definition Definition
	err        error
}

// UniqueValues declares cross-source property uniqueness with an explicit stable
// name. Add one On call per participating event type. The same source may reclaim
// its value. Case-sensitive comparison, no extra scope and all sequences are the defaults.
func UniqueValues(name string) *Builder {
	return &Builder{definition: Definition{name: name, kind: Unique}}
}

// UniqueEventTypes declares at most one occurrence from the covered event types
// per source lifecycle. The first type's persisted ID is the default name, as for
// C# Unique<T>(); use WithName for a shared lifecycle. Duplicate types are ignored.
func UniqueEventTypes(types ...events.Descriptor) *Builder {
	b := &Builder{definition: Definition{kind: UniqueEventType}}
	b.definition.types = b.distinctTypes(nil, types)
	if len(types) > 0 {
		b.definition.name = string(types[0].Ref().ID)
	}
	return b
}

// On adds an event's ordered composite property paths. Paths are exact serialized
// JSON names (including json tags), separated by dots for nested objects. Calling
// On twice for the same event ID, missing paths or an empty property set is invalid.
// On is only valid on a UniqueValues builder. Arguments are copied.
func (b *Builder) On(event events.Descriptor, properties ...string) *Builder {
	if b.definition.kind != Unique {
		b.invalid("On requires a property constraint")
		return b
	}
	for _, field := range b.definition.fields {
		if field.Event.Ref().ID == event.Ref().ID {
			b.invalid("event type %q already added", event.Ref().ID)
			return b
		}
	}
	b.definition.fields = append(b.definition.fields, EventFields{Event: event, Properties: slices.Clone(properties)})
	b.definition.types = append(b.definition.types, event)
	return b
}

// WithName overrides the stable name. The last call wins; blank names fail Build.
func (b *Builder) WithName(name string) *Builder { b.definition.name = name; return b }

// WithMessage supplies a client-side message template. {DetailKey} placeholders
// are replaced using violation details. An empty message preserves the kernel
// message. The last WithMessage or WithMessageProvider call wins.
func (b *Builder) WithMessage(message string) *Builder {
	b.definition.message = func(Violation) string { return message }
	b.definition.messageProvider = false
	return b
}

// WithMessageProvider supplies client-side localization/rendering, called once for
// each returned violation. It receives owned details and may run concurrently;
// the callback must be concurrency-safe. Nil is invalid. Build retains the callback,
// not a snapshot of its captured state. Detail placeholders are replaced afterwards.
func (b *Builder) WithMessageProvider(provider MessageProvider) *Builder {
	if provider == nil {
		b.invalid("nil message provider")
	}
	b.definition.message = provider
	b.definition.messageProvider = true
	return b
}

// IgnoreCasing enables the kernel's case-insensitive property comparison, without
// trimming values. It is invalid on event-type constraints.
func (b *Builder) IgnoreCasing() *Builder {
	if b.definition.kind != Unique {
		b.invalid("IgnoreCasing requires a property constraint")
	}
	b.definition.ignoreCasing = true
	return b
}

// RemovedWith adds events that release the appending source's claim or lifecycle.
// Calls are additive; duplicate IDs are ignored. A different source cannot release
// the owner's claim. Scope and sequence selection also apply to removal events.
func (b *Builder) RemovedWith(types ...events.Descriptor) *Builder {
	b.definition.removers = b.distinctTypes(b.definition.removers, types)
	return b
}

// PerEventSourceType partitions claims by the appending event's source type.
func (b *Builder) PerEventSourceType() *Builder {
	b.definition.scope.PerEventSourceType = true
	return b
}

// PerEventStreamType partitions claims by the appending event's stream type.
func (b *Builder) PerEventStreamType() *Builder {
	b.definition.scope.PerEventStreamType = true
	return b
}

// PerEventStreamID partitions claims by the appending event's stream ID.
func (b *Builder) PerEventStreamID() *Builder { b.definition.scope.PerEventStreamID = true; return b }

// ForEventSequences restricts validation and indexing to the selected sequences.
// Calls are additive and deduplicated in declaration order. Omission (or no IDs)
// means all sequences, not just the event log. Blank IDs are invalid.
func (b *Builder) ForEventSequences(ids ...events.SequenceID) *Builder {
	for _, id := range ids {
		if strings.TrimSpace(string(id)) == "" {
			b.invalid("blank event sequence ID")
		}
		if !slices.Contains(b.definition.sequences, id) {
			b.definition.sequences = append(b.definition.sequences, id)
		}
	}
	return b
}

// ForEventLog restricts validation and indexing to the event log, additively.
func (b *Builder) ForEventLog() *Builder { return b.ForEventSequences(events.EventLog) }

// Build validates names, event descriptors and schema property paths, and returns
// an immutable definition. Invalid declarations wrap chronicle.ErrInvalidConfiguration;
// unqualified binary capabilities wrap chronicle.ErrUnsupported. Adding the
// definition to a Registry additionally checks membership in that registry.
func (b *Builder) Build() (Definition, error) {
	if b == nil {
		return Definition{}, fmt.Errorf("%w: nil constraint builder", faults.ErrInvalidConfiguration)
	}
	if b.err != nil {
		return Definition{}, b.err
	}
	d := b.definition
	if (d.kind != Unique && d.kind != UniqueEventType) || strings.TrimSpace(d.name) == "" || len(d.types) == 0 {
		return Definition{}, fmt.Errorf("%w: constraint kind, name and event types are required", faults.ErrInvalidConfiguration)
	}
	for _, event := range append(d.EventTypes(), d.removers...) {
		if event.GoType() == nil || event.Ref().ID == "" || event.Ref().Generation == 0 {
			return Definition{}, fmt.Errorf("%w: empty constraint event descriptor", faults.ErrInvalidConfiguration)
		}
	}
	for _, fields := range d.fields {
		if len(fields.Properties) == 0 {
			return Definition{}, fmt.Errorf("%w: properties required for %s", faults.ErrInvalidConfiguration, fields.Event.Ref().ID)
		}
		var schema propertySchema
		if err := json.Unmarshal([]byte(fields.Event.Schema()), &schema); err != nil {
			return Definition{}, fmt.Errorf("%w: invalid event schema: %v", faults.ErrInvalidConfiguration, err)
		}
		for _, path := range fields.Properties {
			if !schema.hasPath(path) {
				return Definition{}, fmt.Errorf("%w: property %q does not exist on event %s", faults.ErrInvalidConfiguration, path, fields.Event.Ref().ID)
			}
			field, ok := serialization.FieldAtWithCapability(fields.Event.Fields(), path, serialization.Field.ContainsBinary)
			if ok && field.ContainsBinary() {
				// The pinned kernel hashes value.ToString(), not the binary content
				// (nor the contents of an ExpandoObject containing binary).
				return Definition{}, fmt.Errorf("%w: binary unique property %q is not supported", faults.ErrUnsupported, path)
			}
		}
	}
	d.fields, d.types, d.removers, d.sequences = d.Fields(), d.EventTypes(), d.RemovalTypes(), d.EventSequences()
	return d, nil
}

func (b *Builder) invalid(format string, args ...any) {
	if b.err == nil {
		b.err = fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, fmt.Sprintf(format, args...))
	}
}

func (b *Builder) distinctTypes(existing, additions []events.Descriptor) []events.Descriptor {
	for _, addition := range additions {
		index := slices.IndexFunc(existing, func(d events.Descriptor) bool { return d.Ref().ID == addition.Ref().ID })
		if index < 0 {
			existing = append(existing, addition)
		} else if existing[index].GoType() != addition.GoType() || existing[index].Ref() != addition.Ref() {
			b.invalid("conflicting descriptor for event %q", addition.Ref().ID)
		}
	}
	return existing
}

// Event schemas currently use inline object properties, including nullable objects.
// Do not accept array/dictionary traversal as an object path the kernel cannot read.
type propertySchema struct {
	Properties map[string]propertySchema `json:"properties"`
}

func (s propertySchema) hasPath(path string) bool {
	for _, part := range strings.Split(path, ".") {
		child, ok := s.Properties[part]
		if !ok || part == "" {
			return false
		}
		s = child
	}
	return true
}
