// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"errors"
	"fmt"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/serialization"
)

// ErrUnsubscribedEventSeeded means a strict projection scenario rejected a
// registered event before serialization, provider invocation, append, or history
// mutation for that event. Earlier successful seeds remain; Given is not atomic.
var ErrUnsubscribedEventSeeded = errors.New("unsubscribed event seeded")

// projectionSubscription is captured once from selected immutable SDK artifacts.
// Nil means default behavior (also used for reducer scenarios). Membership is by
// persisted event ID, never by generation or Go name. The map is never mutated.
type projectionSubscription struct {
	projection string
	ids        map[events.TypeID]struct{}
}

func strictProjectionSubscription(definition projections.Definition, catalog *events.Catalog) (*projectionSubscription, error) {
	return projectionMembership(definition, catalog)
}

// projectionMembership admits the shared root source-key scenario profile.
// Initial values do not alter event membership.
func projectionMembership(definition projections.Definition, catalog *events.Catalog) (*projectionSubscription, error) {
	if definition.Identifier() == "" || catalog == nil || definition.Model().GoType() == nil {
		return nil, chronicle.ErrNotRegistered
	}
	// The classification survives compile/group lowering, naming, and store
	// snapshots. Wire fields alone cannot distinguish a one-member variant.
	if definition.IsVariant() {
		return nil, fmt.Errorf("%w: strict projection scenario variants", chronicle.ErrUnsupported)
	}
	wire := definition.KernelDefinition()
	if len(wire.Join) != 0 || len(wire.RemovedWithJoin) != 0 || len(wire.Children) != 0 || len(wire.Nested) != 0 || len(wire.FromEvery) != 0 || wire.FromEventProperty != nil {
		return nil, fmt.Errorf("%w: strict projection scenario relationships or derivative definitions", chronicle.ErrUnsupported)
	}
	if wire.SubscribesToAllEvents && (len(wire.From) != 0 || len(wire.RemovedWith) != 0) {
		return nil, fmt.Errorf("%w: strict projection scenario mixed all-event subscription", chronicle.ErrUnsupported)
	}
	roots, err := serialization.ProtectionRoots(definition.Model().Schema())
	if err != nil {
		return nil, err
	}
	if len(roots) != 0 {
		return nil, fmt.Errorf("%w: strict projection scenario protected model", chronicle.ErrUnsupported)
	}
	selected := &projectionSubscription{projection: definition.Identifier(), ids: make(map[events.TypeID]struct{})}
	descriptors := catalog.Descriptors()
	add := func(id events.TypeID) error {
		if _, selectedAlready := selected.ids[id]; selectedAlready {
			return nil
		}
		if _, ok := catalog.LookupID(id); !ok {
			return chronicle.ErrNotRegistered
		}
		// ID membership also admits registered historical generations. Every
		// admitted descriptor must satisfy the unclassified profile, not just
		// the current generation's schema.
		for _, descriptor := range descriptors {
			if descriptor.Ref().ID != id {
				continue
			}
			roots, err := serialization.ProtectionRoots(descriptor.Schema())
			if err != nil {
				return err
			}
			if len(roots) != 0 {
				return fmt.Errorf("%w: strict projection scenario protected event", chronicle.ErrUnsupported)
			}
		}
		selected.ids[id] = struct{}{}
		return nil
	}
	if wire.SubscribesToAllEvents {
		if !sourceKey(wire.All.GetKey()) {
			return nil, fmt.Errorf("%w: strict projection scenario custom all-event key", chronicle.ErrUnsupported)
		}
		for _, descriptor := range descriptors {
			if err := add(descriptor.Ref().ID); err != nil {
				return nil, err
			}
		}
	}
	for _, from := range wire.From {
		if from.Key == nil || from.Value == nil {
			return nil, chronicle.ErrNotRegistered
		}
		if !sourceKey(from.Value.Key) || !sourceKey(from.Value.ParentKey) {
			return nil, fmt.Errorf("%w: strict projection scenario custom key", chronicle.ErrUnsupported)
		}
		if err := add(events.TypeID(from.Key.Id)); err != nil {
			return nil, err
		}
	}
	for _, removal := range wire.RemovedWith {
		if removal.Key == nil || removal.Value == nil {
			return nil, chronicle.ErrNotRegistered
		}
		if !sourceKey(removal.Value.Key) || !sourceKey(removal.Value.ParentKey) {
			return nil, fmt.Errorf("%w: strict projection scenario custom removal key", chronicle.ErrUnsupported)
		}
		if err := add(events.TypeID(removal.Key.Id)); err != nil {
			return nil, err
		}
	}
	if len(selected.ids) == 0 && !wire.SubscribesToAllEvents {
		return nil, chronicle.ErrNotRegistered
	}
	return selected, nil
}

func sourceKey(key string) bool { return key == "" || key == "$eventSourceId" }

func (s *projectionSubscription) admit(id events.TypeID) error {
	if s == nil {
		return nil
	}
	if _, ok := s.ids[id]; ok {
		return nil
	}
	// Both identifiers come from frozen SDK metadata. No content or application
	// error is formatted, wrapped, or traversed for classification.
	return fmt.Errorf("%w: projection %q, event type %q", ErrUnsubscribedEventSeeded, s.projection, id)
}
