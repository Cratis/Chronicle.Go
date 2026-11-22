// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

// Fields returns detached metadata from the same plan used by the model schema.
func (d Descriptor) Fields() []serialization.Field {
	if d.definition == nil {
		return nil
	}
	return d.definition.plan.Fields()
}

// HasExplicitEventSequence reports whether the model overrides source inference.
func (d Descriptor) HasExplicitEventSequence() bool {
	return d.definition != nil && d.definition.config.sequenceExplicit
}

// BindProjection returns a new descriptor associated with a compiled producer.
// It is a registry-composition hook, not runtime registration. Explicit conflicting
// observer/sequence settings fail; passive projections use None rather than a
// materialized sink. Existing handles still refer to the same model declaration.
func BindProjection(d Descriptor, id string, sequence events.SequenceID, passive bool) (Descriptor, error) {
	if err := d.validateProjectionBinding(id, sequence); err != nil {
		return Descriptor{}, err
	}
	config := d.definition.config
	if err := config.bindProjectionSink(passive); err != nil {
		return Descriptor{}, err
	}
	config.observerID, config.sequence = id, sequence
	copy := *d.definition
	copy.config = config
	return Descriptor{definition: &copy}, nil
}

func (d Descriptor) validateProjectionBinding(id string, sequence events.SequenceID) error {
	if d.definition == nil || id == "" || sequence == "" {
		return invalid("model and projection identity required")
	}
	config := d.definition.config
	if config.observer != Projection || (config.observerID != "" && config.observerID != id) {
		return invalid("model has a conflicting producer")
	}
	if config.sequenceExplicit && config.sequence != sequence {
		return invalid("model and projection event sequences conflict")
	}
	return nil
}

func (config *modelConfig) bindProjectionSink(passive bool) error {
	if passive {
		if config.sinkExplicit && config.sink.Type != NoSink {
			return invalid("passive projection cannot use this sink")
		}
		if config.sink.ConfigurationID != "00000000-0000-0000-0000-000000000000" {
			return invalid("passive projection cannot use a configured sink")
		}
		config.sink.Type = NoSink
	} else if config.sink.Type == NoSink && config.sinkExplicit {
		return invalid("active projection requires a materialized sink")
	}
	if config.sink.Type == NoSink && config.sink.ConfigurationID != "00000000-0000-0000-0000-000000000000" {
		return invalid("None projection cannot use a configured sink")
	}
	return nil
}

func sameDeclaration(a, b Descriptor) bool {
	return a.definition != nil && b.definition != nil && a.definition.origin == b.definition.origin
}
