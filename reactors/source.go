// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors

import (
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/observersource"
)

func (p *Plan) inferSource() error {
	c := p.declaration.config
	if c.sequenceExplicit {
		return nil
	}
	if c.sourceStore != "" {
		p.sourceStore = c.sourceStore
		return nil
	}
	descriptors := make([]events.Descriptor, 0, len(p.ordered))
	for _, ref := range p.ordered {
		descriptors = append(descriptors, p.descriptors[ref.ID])
	}
	var err error
	p.sourceStore, err = observersource.Infer(descriptors)
	return err
}

// SourceStore returns the automatic subscription origin, or empty for an
// unspecified origin/explicit sequence. It never provisions by itself.
func (p *Plan) SourceStore() string { return p.sourceStore }

// ForStore binds an immutable plan without rediscovery. Event-origin metadata
// matching the current store selects event-log; external metadata selects inbox.
// Explicit sequences are unchanged; an observer-level source always selects inbox.
func (p *Plan) ForStore(store string) *Plan {
	if p.declaration.config.sequenceExplicit {
		return p
	}
	copy := *p
	copy.declaration.config.sequence = events.EventLog
	if p.sourceStore != "" && (p.sourceStore != store || p.declaration.config.sourceStore != "") {
		copy.declaration.config.sequence = events.SequenceID(events.InboxPrefix + p.sourceStore)
	}
	return &copy
}
