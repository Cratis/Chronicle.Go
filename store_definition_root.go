// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/projections"
)

// ErrDestructiveRegistrationUnknown refuses new definitions after a dispatched
// cumulative registration has an unknown outcome. Later acknowledgements do not
// clear it or prove remote quiescence. Ordinary registration/readiness is unchanged.
var ErrDestructiveRegistrationUnknown = errors.New("chronicle: destructive registration outcome unknown; new definitions refused")

var errDefinitionSuperseded = errors.New("chronicle: definition snapshot superseded before dispatch")

type definitionRoot struct {
	revision  uint64
	snapshot  registrySnapshot
	decisions *decision.Catalog
	previous  uint64
	delta     projections.Definition
}

// definitionCoordinator exists before the first registration and survives failed
// handle acquisition and transport generations. All transitions use Client.mu.
// It coordinates this client only, not independent clients with the same owner.
type definitionCoordinator struct {
	root               *definitionRoot
	clock              *atomic.Uint64
	flight             bool
	destructiveUnknown bool
	changed            chan struct{}
}

func (d *definitionCoordinator) notifyLocked() {
	close(d.changed)
	d.changed = make(chan struct{})
}

func (c *Client) definitionsLocked(name StoreName) (*definitionCoordinator, error) {
	if d := c.definitions[name]; d != nil {
		return d, nil
	}
	snapshot, err := c.initialStoreSnapshotLocked(name)
	if err != nil {
		return nil, err
	}
	clock := &atomic.Uint64{}
	clock.Store(1)
	d := &definitionCoordinator{clock: clock, changed: make(chan struct{})}
	d.root = newDefinitionRoot(snapshot, clock, 1)
	if c.definitions == nil {
		c.definitions = make(map[StoreName]*definitionCoordinator)
	}
	c.definitions[name] = d
	return d, nil
}

func newDefinitionRoot(snapshot registrySnapshot, clock *atomic.Uint64, revision uint64) *definitionRoot {
	catalog := decision.NewCatalog(nil, snapshot.events)
	catalog.Epoch, catalog.ExpectedEpoch = clock, revision
	for _, definition := range snapshot.projections {
		catalog.Projections = append(catalog.Projections, definition.KernelDefinition())
	}
	return &definitionRoot{revision: revision, snapshot: snapshot, decisions: catalog}
}

func (s *EventStore) definitionRoot() *definitionRoot {
	s.client.mu.Lock()
	defer s.client.mu.Unlock()
	return s.definitions.root
}

func definitionStageKey(name StoreName, revision uint64, stage string, full bool) string {
	return fmt.Sprintf("%s:%q:%d:%t", stage, name, revision, full)
}

func (s *EventStore) definitionCurrent(root *definitionRoot) bool {
	s.client.mu.Lock()
	defer s.client.mu.Unlock()
	return s.definitions.root == root
}

func (s *EventStore) waitDefinitionFlight(ctx context.Context) error {
	for {
		s.client.mu.Lock()
		if s.client.closed {
			s.client.mu.Unlock()
			return ErrClosed
		}
		if err := ctx.Err(); err != nil {
			s.client.mu.Unlock()
			return err
		}
		if s.definitions.destructiveUnknown {
			s.client.mu.Unlock()
			return ErrDestructiveRegistrationUnknown
		}
		flight, changed := s.definitions.flight, s.definitions.changed
		s.client.mu.Unlock()
		if !flight {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.client.life.Done():
			return ErrClosed
		case <-changed:
		}
	}
}
