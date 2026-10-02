// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"strings"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
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

// EventStore is an immutable, concurrency-safe registered store/namespace handle.
// It borrows its Client; Close the client to release resources.
type EventStore struct {
	client      *Client
	name        StoreName
	namespace   Namespace
	catalog     *events.Catalog
	constraints []constraints.Definition
	log         *eventsequences.Sequence

	readModels *readmodels.Service
	reactors   storeReactors
}

// Name returns the logical store name.
func (s *EventStore) Name() StoreName { return s.name }

// Namespace returns this handle's isolated namespace.
func (s *EventStore) Namespace() Namespace { return s.namespace }

// EventTypes returns the frozen catalog registered before this handle was published.
func (s *EventStore) EventTypes() *events.Catalog { return s.catalog }

// EventLog returns the primary sequence, cached for this store handle.
func (s *EventStore) EventLog() *eventsequences.Sequence { return s.log }

// EventSequence returns a handle for a nonblank sequence ID.
func (s *EventStore) EventSequence(id events.SequenceID) (*eventsequences.Sequence, error) {
	if id == events.EventLog {
		return s.log, nil
	}
	return eventsequences.New(s.name, s.namespace, id, s.catalog, &clientTransport{client: s.client, store: s})
}

// EventStore connects, ensures the store/namespace and registers its explicit
// events, read models and constraints before returning a cached handle. Cache keys include both coordinates.
// Concurrent calls share registration; failed passes can be retried on the same handle.
// Required registrations never report successful readiness after a failed envelope.
func (c *Client) EventStore(ctx context.Context, name StoreName, options ...StoreOption) (*EventStore, error) {
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
		catalog, definitions := c.catalog, c.constraints
		if selected, ok := c.catalogs[key.name]; ok {
			catalog, definitions = selected, c.storeConstraints[key.name]
		}
		store = &EventStore{client: c, name: key.name, namespace: key.namespace, catalog: catalog, constraints: definitions}
		var err error
		store.log, err = eventsequences.New(key.name, key.namespace, events.EventLog, catalog, &clientTransport{client: c, store: store})
		if err != nil {
			c.mu.Unlock()
			return nil, err
		}
		if err = store.initializeReadModels(); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		c.stores[key] = store
	}
	c.mu.Unlock()
	if _, err := store.WaitForRegistration(ctx); err != nil {
		return nil, err
	}
	return store, nil
}

// EventStores lists authorized logical stores after connection preflight.
func (c *Client) EventStores(ctx context.Context) ([]StoreName, error) {
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
