// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"slices"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
)

// Provenance locates a normalized subscription, mapping or convention. Offset is
// a decoded tag byte offset; -1 denotes typed authoring. It contains no literals.
type Provenance struct {
	FrontEnd  string
	GoField   string
	Path      string
	Directive string
	Offset    int
	Event     events.TypeRef
}

// Diagnostic describes preserved C# precedence, rather than silently shadowing a
// declaration. Both source locations survive normalization for tooling.
type Diagnostic struct {
	Message     string
	Previous    Provenance
	Replacement Provenance
}

// Definition is the immutable, fully resolved semantic representation shared by
// both front ends. Its zero value is invalid. Copies are safe for concurrent use.
type Definition struct{ data *definition }
type definition struct {
	id                             string
	model                          readmodels.Descriptor
	sequence                       events.SequenceID
	passive, notRewindable, noAuto bool
	keyField                       string
	exclusions                     []string
	from                           []fromDefinition
	provenance                     []Provenance
	diagnostics                    []Diagnostic
}
type fromDefinition struct {
	event       events.TypeRef
	key, parent expression
	writes      []write
}

// Identifier returns the stable projection identity.
func (d Definition) Identifier() string {
	if d.data == nil {
		return ""
	}
	return d.data.id
}

// Model returns the projection-bound model descriptor.
func (d Definition) Model() readmodels.Descriptor {
	if d.data == nil {
		return readmodels.Descriptor{}
	}
	return d.data.model
}

// EventSequence returns the selected input sequence.
func (d Definition) EventSequence() events.SequenceID { return d.data.sequence }

// IsPassive reports whether the model is computed on demand rather than materialized.
func (d Definition) IsPassive() bool { return d.data.passive }

// KeyField returns explicit key metadata; empty means no field was annotated.
// It does not redirect the event's correlation key or invent a root mapping.
func (d Definition) KeyField() string { return d.data.keyField }

// Provenance returns detached mapping/convention origins.
func (d Definition) Provenance() []Provenance { return slices.Clone(d.data.provenance) }

// Diagnostics returns detached shadowing diagnostics from normalization.
func (d Definition) Diagnostics() []Diagnostic { return slices.Clone(d.data.diagnostics) }

// KernelDefinition is the one encoder for every front end. Each call returns an
// owned protobuf; mutating it never changes this definition or reconnect snapshots.
func (d Definition) KernelDefinition() *contracts.ProjectionDefinition {
	if d.data == nil {
		return nil
	}
	data := d.data
	auto := contracts.AutoMap_Enabled
	if data.noAuto {
		auto = contracts.AutoMap_Disabled
	}
	result := &contracts.ProjectionDefinition{Identifier: data.id, ReadModel: string(data.model.Identifier()), EventSequenceId: string(data.sequence), IsActive: !data.passive, IsRewindable: !data.notRewindable, InitialModelState: "{}", AutoMap: auto, All: &contracts.FromEveryDefinition{}, NoAutoMapProperties: slices.Clone(data.exclusions)}
	for _, from := range data.from {
		properties := make(map[string]string, len(from.writes))
		for _, write := range from.writes {
			properties[write.path] = write.expression.encode()
		}
		result.From = append(result.From, &contracts.KeyValuePair_EventType_FromDefinition{Key: &contracts.EventType{Id: string(from.event.ID), Generation: uint32(from.event.Generation)}, Value: &contracts.FromDefinition{Key: from.key.encode(), ParentKey: from.parent.encode(), Properties: properties}})
	}
	return result
}
