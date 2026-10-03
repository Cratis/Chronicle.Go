// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import (
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
)

func inferSource(d *definition, catalog *events.Catalog, currentStore []string) error {
	if d.sequenceExplicit {
		return nil
	}
	// Include authored handlers, not only root From. Generated sibling removals
	// are added after inference so they cannot change a variant's source.
	// The broader authored-handler coverage is an intentional Go difference.
	var visit func(*nodeDefinition) error
	check := func(ref events.TypeRef) error {
		event, ok := catalog.LookupRef(ref)
		if !ok {
			return invalid("source event missing from catalog")
		}
		store := event.SourceStore()
		if store == "" {
			return nil
		}
		if d.sourceStore != "" && store != d.sourceStore {
			return invalid("projection uses incompatible source stores")
		}
		d.sourceStore = store
		return nil
	}
	visit = func(n *nodeDefinition) error {
		for _, f := range n.from {
			if err := check(f.event); err != nil {
				return err
			}
		}
		for _, j := range n.joins {
			if err := check(j.event); err != nil {
				return err
			}
		}
		for _, r := range n.removals {
			if err := check(r.event); err != nil {
				return err
			}
		}
		for _, child := range n.children {
			if err := visit(child); err != nil {
				return err
			}
		}
		for _, child := range n.nested {
			if err := visit(child); err != nil {
				return err
			}
		}
		return nil
	}
	if err := visit(&d.nodeDefinition); err != nil {
		return declarationFailure(d.id, Provenance{Offset: -1}, err)
	}
	store := ""
	if len(currentStore) > 0 {
		store = currentStore[len(currentStore)-1]
	}
	bound, err := (Definition{data: d}).ForStore(store)
	if err == nil {
		*d = *bound.data
	}
	return err
}

// ForStore resolves a previously validated template against the actual store.
// It never rediscovers mappings or changes an explicit sequence. Unspecified
// origins use event-log; an external origin uses inbox-<store>, without provisioning.
func (d Definition) ForStore(store string) (Definition, error) {
	if d.data == nil {
		return Definition{}, invalid("definition required")
	}
	if d.data.sequenceExplicit {
		return d, nil
	}
	copy := *d.data
	copy.sequence = events.EventLog
	if copy.sourceStore != "" && copy.sourceStore != store {
		copy.sequence = events.SequenceID("inbox-" + copy.sourceStore)
	}
	model, err := readmodels.BindProjection(copy.model, copy.id, copy.sequence, copy.passive)
	if err != nil {
		return Definition{}, err
	}
	copy.model = model
	return Definition{data: &copy}, nil
}
