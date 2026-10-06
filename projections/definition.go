// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"crypto/sha256"
	"reflect"
	"slices"

	"google.golang.org/protobuf/proto"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
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
	id                     string
	model                  readmodels.Descriptor
	sequence               events.SequenceID
	passive, notRewindable bool
	subscribesAll          bool
	variant                reflect.Type
	entering               []fromDefinition
	sequenceExplicit       bool
	sourceStore            string
	initialState           string
	labels                 []string
	nodeDefinition
	provenance  []Provenance
	diagnostics []Diagnostic
}
type nodeDefinition struct {
	noAuto           bool
	ownNoAuto        bool
	inheritAuto      bool
	keyField         string
	exclusions       []string
	from             []fromDefinition
	joins            []joinDefinition
	removals         []removalDefinition
	all              []write
	includeChildren  bool
	children, nested map[string]*nodeDefinition
	identifiedBy     string
	// derivative is the sole registered concrete type selected for a children
	// collection whose element is a derived-type family; nil otherwise.
	derivative *serialization.Derivative
}
type joinDefinition struct {
	fromDefinition
	on string
}
type removalDefinition struct {
	event       events.TypeRef
	key, parent expression
	join        bool
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
func (d Definition) EventSequence() events.SequenceID {
	if d.data == nil {
		return ""
	}
	return d.data.sequence
}

// IsPassive reports whether the model is computed on demand rather than materialized.
func (d Definition) IsPassive() bool { return d.data != nil && d.data.passive }

// KeyField returns explicit key metadata; empty means no field was annotated.
// It does not redirect the event's correlation key or invent a root mapping.
func (d Definition) KeyField() string {
	if d.data == nil {
		return ""
	}
	return d.data.keyField
}

// Provenance returns detached mapping/convention origins.
func (d Definition) Provenance() []Provenance {
	if d.data == nil {
		return nil
	}
	return slices.Clone(d.data.provenance)
}

// Diagnostics returns detached shadowing diagnostics from normalization.
func (d Definition) Diagnostics() []Diagnostic {
	if d.data == nil {
		return nil
	}
	return slices.Clone(d.data.diagnostics)
}

// Hash returns a SHA-256 fingerprint of the finalized wire definition. Map keys
// are encoded deterministically; timestamps and authoring provenance are absent.
// Rebinding naming or a source store changes the hash when the wire shape changes.
// Stability is limited to one build: protobuf deterministic encoding is not
// guaranteed across binaries or dependency versions. Do not persist hashes across
// upgrades. Neither the kernel nor C# uses this client-side convenience hash.
func (d Definition) Hash() ([32]byte, error) {
	if d.data == nil {
		return [32]byte{}, invalid("definition required")
	}
	data, err := (proto.MarshalOptions{Deterministic: true}).Marshal(d.KernelDefinition())
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(data), nil
}

// KernelDefinition is the one encoder for every front end. Each call returns an
// owned protobuf; mutating it never changes this definition or reconnect snapshots.
func (d Definition) KernelDefinition() *contracts.ProjectionDefinition {
	if d.data == nil {
		return nil
	}
	data := d.data
	node := encodeNode(&data.nodeDefinition)
	return &contracts.ProjectionDefinition{
		Identifier: data.id, ReadModel: string(data.model.Identifier()), EventSequenceId: string(data.sequence),
		IsActive: !data.passive, IsRewindable: !data.notRewindable, InitialModelState: data.initialState, Tags: slices.Clone(data.labels),
		AutoMap: node.AutoMap, All: node.All, NoAutoMapProperties: node.NoAutoMapProperties,
		From: node.From, Join: node.Join, Children: node.Children, Nested: node.Nested,
		RemovedWith: node.RemovedWith, RemovedWithJoin: node.RemovedWithJoin, SubscribesToAllEvents: data.subscribesAll,
	}
}

func encodeNode(data *nodeDefinition) *contracts.ChildrenDefinition {
	auto := contracts.AutoMap_Enabled
	if data.inheritAuto {
		auto = contracts.AutoMap_Inherit
	} else if data.noAuto {
		auto = contracts.AutoMap_Disabled
	}
	result := &contracts.ChildrenDefinition{IdentifiedBy: data.identifiedBy, AutoMap: auto, All: &contracts.FromEveryDefinition{Properties: encodeWrites(data.all), IncludeChildren: data.includeChildren}, NoAutoMapProperties: slices.Clone(data.exclusions)}
	for _, from := range data.from {
		result.From = append(result.From, &contracts.KeyValuePair_EventType_FromDefinition{Key: encodeEvent(from.event), Value: &contracts.FromDefinition{Key: from.key.encode(), ParentKey: from.parent.encode(), Properties: encodeWrites(from.writes)}})
	}
	for _, join := range data.joins {
		result.Join = append(result.Join, &contracts.KeyValuePair_EventType_JoinDefinition{Key: encodeEvent(join.event), Value: &contracts.JoinDefinition{On: join.on, Key: join.key.encode(), Properties: encodeWrites(join.writes)}})
	}
	for _, removal := range data.removals {
		if removal.join {
			result.RemovedWithJoin = append(result.RemovedWithJoin, &contracts.KeyValuePair_EventType_RemovedWithJoinDefinition{Key: encodeEvent(removal.event), Value: &contracts.RemovedWithJoinDefinition{Key: removal.key.encode()}})
		} else {
			result.RemovedWith = append(result.RemovedWith, &contracts.KeyValuePair_EventType_RemovedWithDefinition{Key: encodeEvent(removal.event), Value: &contracts.RemovedWithDefinition{Key: removal.key.encode(), ParentKey: removal.parent.encode()}})
		}
	}
	if len(data.children) > 0 {
		result.Children = make(map[string]*contracts.ChildrenDefinition, len(data.children))
		for path, child := range data.children {
			result.Children[path] = encodeNode(child)
		}
	}
	if len(data.nested) > 0 {
		result.Nested = make(map[string]*contracts.ChildrenDefinition, len(data.nested))
		for path, child := range data.nested {
			result.Nested[path] = encodeNode(child)
		}
	}
	return result
}

func encodeWrites(writes []write) map[string]string {
	properties := make(map[string]string, len(writes))
	for _, w := range writes {
		properties[w.path] = w.expression.encode()
	}
	return properties
}
func encodeEvent(event events.TypeRef) *contracts.EventType {
	return &contracts.EventType{Id: string(event.ID), Generation: uint32(event.Generation)}
}
