// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"strings"

	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
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
type storeAttempt struct {
	done  chan struct{}
	store *EventStore
	err   error
}

// EventStore is an immutable, concurrency-safe registered store/namespace handle.
// It borrows its Client; Close the client to release resources.
type EventStore struct {
	client    *Client
	name      StoreName
	namespace Namespace
	catalog   *events.Catalog
	log       *eventsequences.Sequence
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
	return eventsequences.New(s.name, s.namespace, id, s.catalog, s.client.transport)
}

// EventStore connects, ensures the store/namespace and registers its explicit
// events before returning a cached handle. Cache keys include both coordinates.
// Concurrent calls share registration; failures are evicted so a later call can retry.
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
	if err := c.Connect(ctx); err != nil {
		return nil, err
	}
	key := storeKey{name: name, namespace: config.namespace}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if attempt, found := c.stores[key]; found {
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-attempt.done:
			return attempt.store, attempt.err
		}
	}
	attempt := &storeAttempt{done: make(chan struct{})}
	c.stores[key] = attempt
	c.work.Add(1)
	c.mu.Unlock()
	defer c.work.Done()
	store, err := c.registerStore(ctx, key)
	c.mu.Lock()
	if c.closed && err == nil {
		err = ErrClosed
		store = nil
	}
	attempt.store, attempt.err = store, err
	if err != nil {
		delete(c.stores, key)
	}
	close(attempt.done)
	c.mu.Unlock()
	return store, err
}

func (c *Client) registerStore(ctx context.Context, key storeKey) (*EventStore, error) {
	result, err := eventstores.NewEventStoresClient(c.transport).EnsureEventStore(ctx, &eventstores.EnsureEventStoreRequest{Name: string(key.name)})
	if err != nil {
		return nil, err
	}
	if err = wire.CheckEnvelope(result); err != nil {
		return nil, err
	}
	catalog := c.catalog
	if selected, ok := c.catalogs[key.name]; ok {
		catalog = selected
	}
	store := &EventStore{client: c, name: key.name, namespace: key.namespace, catalog: catalog}
	if err = store.Namespaces().Ensure(ctx, key.namespace); err != nil {
		return nil, err
	}
	request := &eventtypes.RegisterEventTypesRequest{EventStore: string(key.name)}
	for _, descriptor := range catalog.Descriptors() {
		ref := descriptor.Ref()
		request.Types = append(request.Types, &eventtypes.EventTypeRegistration{Type: &eventtypes.EventType{Id: string(ref.ID), Generation: uint32(ref.Generation)}, Schema: descriptor.Schema()})
	}
	if len(request.Types) > 0 {
		registered, err := eventtypes.NewEventTypesClient(c.transport).RegisterEventTypes(ctx, request)
		if err != nil {
			return nil, err
		}
		if err = wire.CheckEnvelope(registered); err != nil {
			return nil, err
		}
	}
	store.log, err = eventsequences.New(key.name, key.namespace, events.EventLog, catalog, c.transport)
	if err != nil {
		return nil, err
	}
	return store, nil
}

// EventStores lists authorized logical stores after connection preflight.
func (c *Client) EventStores(ctx context.Context) ([]StoreName, error) {
	if err := c.Connect(ctx); err != nil {
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
