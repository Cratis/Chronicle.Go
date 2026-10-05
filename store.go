// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"google.golang.org/protobuf/types/known/emptypb"
)

// StoreName identifies a logical event store.
type StoreName = metadata.StoreName

// Namespace identifies isolated state within a store.
type Namespace = metadata.Namespace

// DefaultNamespace is selected when no WithNamespace option is supplied.
const DefaultNamespace = metadata.DefaultNamespace

// EnvelopeError preserves kernel authorization/validation/execution failure despite gRPC OK.
type EnvelopeError = wire.EnvelopeError

// ValidationResult is a structured kernel validation diagnostic.
type ValidationResult = wire.ValidationResult

// StoreOption selects a store handle. Options are last-wins; nil is invalid.
type StoreOption func(*storeConfig)
type storeConfig struct{ namespace Namespace }

// WithNamespace selects a nonblank namespace; it does not grant authorization.
func WithNamespace(namespace Namespace) StoreOption {
	return func(c *storeConfig) { c.namespace = namespace }
}

type storeKey struct {
	name      StoreName
	namespace Namespace
}

// EventStore is a concurrency-safe registered store/namespace handle.
// It borrows its Client; Close the client to release resources. Cache eviction
// detaches this facade, not its resources or ability to perform explicit operations.
type EventStore struct {
	*storeOwner
}

// storeOwner retains exactly one resource set per client/store/namespace, even
// when no facade belongs to the lookup cache. Never copy it: observer managers,
// sequence subscriptions and reduction feeds have independent live lifetimes.
// It has no exported methods to promote onto the public facade.
type storeOwner struct {
	client        *Client
	definitions   *definitionCoordinator
	readerRoot    *definitionRoot
	latestReaders *readmodels.Service
	name          StoreName
	namespace     Namespace
	catalog       *events.Catalog
	constraints   []constraints.Definition
	log           *eventsequences.Sequence
	sequencesMu   sync.Mutex
	sequences     map[events.SequenceID]*eventsequences.Sequence

	projectionSnapshot       []projections.Definition
	reactorSnapshot          []*reactors.Plan
	reducerSnapshot          []*reducers.Plan
	readModelReactorSnapshot []*reactors.ReadModelPlan
	compliance               *compliance.Manager
	readModels               *readmodels.Service
	decisionCatalog          *decision.Catalog
	reactors                 storeObservers
	reducers                 storeObservers
	readModelReactors        storeObservers
	readModelChanges         readmodels.ReductionChanges
}

// Name returns the logical store name.
func (s *EventStore) Name() StoreName { return s.name }

// Namespace returns this handle's isolated namespace.
func (s *EventStore) Namespace() Namespace { return s.namespace }

// Compliance returns the namespace-bound PII key lifecycle manager. Erasure and
// reauthorization reach all stores in this namespace, never other namespaces.
func (s *EventStore) Compliance() *compliance.Manager { return s.compliance }

// EventTypes returns the frozen catalog registered before this handle was published.
func (s *EventStore) EventTypes() *events.Catalog { return s.catalog }

// EventLog returns the primary sequence, cached for this store handle.
func (s *EventStore) EventLog() *eventsequences.Sequence { return s.log }

// EventSequence returns the cached handle for a nonblank sequence ID. Concurrent
// calls share the handle and its append subscriptions within this store/namespace.
func (s *EventStore) EventSequence(id events.SequenceID) (*eventsequences.Sequence, error) {
	if id == events.EventLog {
		return s.log, nil
	}
	s.sequencesMu.Lock()
	defer s.sequencesMu.Unlock()
	if sequence := s.sequences[id]; sequence != nil {
		return sequence, nil
	}
	sequence, err := eventsequences.New(s.name, s.namespace, id, s.catalog, &clientTransport{client: s.client, store: s})
	if err != nil {
		return nil, err
	}
	if s.sequences == nil {
		s.sequences = map[events.SequenceID]*eventsequences.Sequence{events.EventLog: s.log}
	}
	s.sequences[id] = sequence
	return sequence, nil
}

// EventStore connects, ensures the store/namespace and registers its explicit
// events, read models and constraints before returning a cached handle. Cache keys include both coordinates.
// Concurrent calls share registration; failed passes can be retried on the same handle.
// Required registrations never report successful readiness after a failed envelope.
func (c *Client) EventStore(ctx context.Context, name StoreName, options ...StoreOption) (*EventStore, error) {
	if err := c.requirePrepared("event store", false); err != nil {
		return nil, err
	}
	config := storeConfig{namespace: DefaultNamespace}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: nil store option", ErrInvalidConfiguration)
		}
		option(&config)
	}
	if strings.TrimSpace(string(name)) == "" || strings.TrimSpace(string(config.namespace)) == "" {
		return nil, fmt.Errorf("%w: store and namespace must be nonblank", ErrInvalidConfiguration)
	}
	if err := c.connect(ctx, false); err != nil {
		return nil, err
	}
	key := storeKey{name: name, namespace: config.namespace}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	store := c.stores[key]
	if store == nil {
		owner, err := c.storeOwnerLocked(key)
		if err != nil {
			c.mu.Unlock()
			return nil, err
		}
		store = &EventStore{storeOwner: owner}
		c.stores[key] = store
	}
	c.mu.Unlock()
	if _, err := store.WaitForRegistration(ctx); err != nil {
		return nil, err
	}
	return store, nil
}

// storeOwnerLocked composes callback-free resources once per coordinate. Their
// transport retains a private binding, not a cached facade or cache membership.
func (c *Client) storeOwnerLocked(key storeKey) (*storeOwner, error) {
	if owner := c.storeOwners[key]; owner != nil {
		return owner, nil
	}
	snapshot, err := c.selectedStoreSnapshotLocked(key.name)
	if err != nil {
		return nil, err
	}
	owner := &storeOwner{client: c, name: key.name, namespace: key.namespace, catalog: snapshot.events, constraints: snapshot.constraints, definitions: c.definitions[key.name]}
	store := &EventStore{storeOwner: owner}
	store.log, err = eventsequences.New(key.name, key.namespace, events.EventLog, snapshot.events, &clientTransport{client: c, store: store})
	if err != nil {
		return nil, err
	}
	store.sequences = map[events.SequenceID]*eventsequences.Sequence{events.EventLog: store.log}
	if err = store.initializeReadModelsFromSnapshot(snapshot); err != nil {
		return nil, err
	}
	store.compliance, err = compliance.New(key.name, key.namespace, &clientTransport{client: c, store: store})
	if err != nil {
		return nil, err
	}
	if c.storeOwners == nil {
		c.storeOwners = make(map[storeKey]*storeOwner)
	}
	c.storeOwners[key] = owner
	return owner, nil
}

// EvictEventStores clears the store lookup cache and subsequent automatic
// registration membership. It does no I/O and does not cancel or join work.
// Already-issued handles, watches and sequences remain usable and client-owned;
// their explicit use can register the current generation without recaching them.
// A previously captured Ready/reconnect pass may finish after eviction returns.
// The next lookup returns a new facade sharing the coordinate's retained resources.
// Eviction does not invalidate decision evidence, reclaim live resources or delete
// server data. Repeated eviction succeeds; unprepared/closed clients return their
// existing ClientStateError. No preparation callbacks are invoked.
func (c *Client) EvictEventStores() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.requirePreparedLocked("evict event stores", false); err != nil {
		return err
	}
	clear(c.stores)
	return nil
}

// EventStores lists authorized logical stores after connection preflight.
func (c *Client) EventStores(ctx context.Context) ([]StoreName, error) {
	if err := c.requirePrepared("event stores", false); err != nil {
		return nil, err
	}
	if err := c.connect(ctx, false); err != nil {
		return nil, err
	}
	result, err := eventstores.NewEventStoresClient(c.transport).AllEventStores(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	if err = wire.CheckEnvelope(result); err != nil {
		return nil, err
	}
	names := make([]StoreName, len(result.Data))
	for i, store := range result.Data {
		if store == nil {
			return nil, ErrProtocol
		}
		names[i] = StoreName(store.Name)
	}
	return names, nil
}
