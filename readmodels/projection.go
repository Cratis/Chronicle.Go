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

// BindProjection returns a new descriptor associated with a compiled producer.
// It is a registry-composition hook, not runtime registration. Explicit conflicting
// observer/sequence settings fail; passive projections use None rather than a
// materialized sink. Existing handles still refer to the same model declaration.
func BindProjection(d Descriptor, id string, sequence events.SequenceID, passive bool) (Descriptor, error) {
	if d.definition == nil || id == "" || sequence == "" {
		return Descriptor{}, invalid("model and projection identity required")
	}
	config := d.definition.config
	if config.observer != Projection || (config.observerID != "" && config.observerID != id) {
		return Descriptor{}, invalid("model has a conflicting producer")
	}
	if config.sequenceExplicit && config.sequence != sequence {
		return Descriptor{}, invalid("model and projection event sequences conflict")
	}
	if passive {
		if config.sink.Type != MongoDB && config.sink.Type != NoSink {
			return Descriptor{}, invalid("passive projection cannot use this sink")
		}
		if config.sink.ConfigurationID != "00000000-0000-0000-0000-000000000000" {
			return Descriptor{}, invalid("passive projection cannot use a configured sink")
		}
		config.sink.Type = NoSink
	} else if config.sink.Type == NoSink {
		return Descriptor{}, invalid("active projection requires a materialized sink")
	}
	config.observerID, config.sequence = id, sequence
	copy := *d.definition
	copy.config = config
	return Descriptor{definition: &copy}, nil
}

func sameDeclaration(a, b Descriptor) bool {
	return a.definition != nil && b.definition != nil && a.definition.origin == b.definition.origin
}
