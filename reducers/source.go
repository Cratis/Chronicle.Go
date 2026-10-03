// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reducers

import (
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/observersource"
	"github.com/cratis/chronicle.go/readmodels"
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

// SourceStore returns the automatic subscription origin, empty for an
// unspecified origin or explicit sequence. It does not provision by itself.
func (p *Plan) SourceStore() string { return p.sourceStore }

// ForStore binds source sequence, read-model producer and fingerprint together,
// without rediscovery. Explicit sequences are unchanged; an observer-level source
// always selects inbox, while event-origin metadata uses event-log in its own store.
func (p *Plan) ForStore(store string) (*Plan, error) {
	copy := *p
	if !p.declaration.config.sequenceExplicit {
		copy.declaration.config.sequence = events.EventLog
		if p.sourceStore != "" && (p.sourceStore != store || p.declaration.config.sourceStore != "") {
			copy.declaration.config.sequence = events.SequenceID(events.InboxPrefix + p.sourceStore)
		}
	}
	var err error
	copy.model, err = readmodels.BindReducer(p.model, string(p.Identifier()), copy.EventSequence(), p.declaration.config.passive || p.IsPassive())
	if err != nil {
		return nil, err
	}
	copy.hash = copy.fingerprint()
	return &copy, nil
}
