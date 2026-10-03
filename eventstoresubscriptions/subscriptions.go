// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package eventstoresubscriptions provisions cross-store outbox to inbox forwarding.
// Definitions are store-wide; the kernel forwards between corresponding namespaces.
package eventstoresubscriptions

import (
	"context"
	"fmt"
	"slices"
	"strings"

	contracts "github.com/cratis/chronicle.go/contracts/observation/eventstoresubscriptions"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
)

// ID identifies a subscription in its target store.
type ID string

// Definition is immutable. Construct it with Define; the zero value is invalid.
type Definition struct {
	id     ID
	source metadata.StoreName
	types  []events.TypeID
}

// Define creates a subscription. No IDs means all current types in catalog, not
// all future types. Repeated IDs are deduplicated in first-seen order. Explicit
// IDs need not be registered locally. Filters always use generation one like C#.
func Define(catalog *events.Catalog, id ID, source metadata.StoreName, ids ...events.TypeID) (Definition, error) {
	if catalog == nil || strings.TrimSpace(string(id)) == "" || strings.TrimSpace(string(source)) == "" {
		return Definition{}, invalid("catalog, subscription ID and source store required")
	}
	if len(ids) == 0 {
		for _, d := range catalog.Descriptors() {
			if !d.IsHistorical() {
				ids = append(ids, d.Ref().ID)
			}
		}
	}
	result := Definition{id: id, source: source}
	seen := map[events.TypeID]bool{}
	for _, id := range ids {
		if strings.TrimSpace(string(id)) == "" {
			return Definition{}, invalid("event ID required")
		}
		if !seen[id] {
			result.types = append(result.types, id)
			seen[id] = true
		}
	}
	return result, nil
}

// Identifier returns the subscription identity.
func (d Definition) Identifier() ID { return d.id }

// SourceStore returns the source outbox's store.
func (d Definition) SourceStore() metadata.StoreName { return d.source }

// EventTypes returns a detached list of persisted event IDs.
func (d Definition) EventTypes() []events.TypeID { return slices.Clone(d.types) }

// KernelDefinition returns an owned wire snapshot with generation-one filters.
func (d Definition) KernelDefinition() *contracts.EventStoreSubscriptionDefinition {
	if d.id == "" {
		return nil
	}
	result := &contracts.EventStoreSubscriptionDefinition{Identifier: string(d.id), SourceEventStore: string(d.source)}
	for _, id := range d.types {
		result.EventTypes = append(result.EventTypes, &contracts.EventType{Id: string(id), Generation: 1})
	}
	return result
}

// Service is safe for concurrent use. It borrows its connection and catalog,
// performs no retries and never owns their lifetimes. Contexts bound each RPC.
type Service struct {
	store   metadata.StoreName
	catalog *events.Catalog
	client  contracts.EventStoreSubscriptionsClient
}

// New constructs a store-wide service without I/O. A connection must be supplied.
func New(store metadata.StoreName, catalog *events.Catalog, conn grpc.ClientConnInterface) (*Service, error) {
	if strings.TrimSpace(string(store)) == "" || catalog == nil || conn == nil {
		return nil, invalid("store, catalog and connection required")
	}
	return &Service{store, catalog, contracts.NewEventStoreSubscriptionsClient(conn)}, nil
}

// Subscribe registers forwarding from source's outbox to inbox-<source> in this
// target. No IDs selects all current catalog IDs. Self-subscriptions are rejected.
func (s *Service) Subscribe(ctx context.Context, id ID, source metadata.StoreName, ids ...events.TypeID) error {
	d, err := Define(s.catalog, id, source, ids...)
	if err != nil {
		return err
	}
	return s.Register(ctx, d)
}

// Register submits an immutable definition. Success acknowledges registration,
// not delivery. A dispatched failure may have applied; no automatic retry occurs.
func (s *Service) Register(ctx context.Context, d Definition) error {
	if d.id == "" || d.source == s.store {
		return invalid("valid external subscription required")
	}
	_, err := s.client.Add(ctx, &contracts.AddEventStoreSubscriptions{TargetEventStore: string(s.store), Subscriptions: []*contracts.EventStoreSubscriptionDefinition{d.KernelDefinition()}})
	return err
}

// Unsubscribe removes the definition; already forwarded inbox events remain.
func (s *Service) Unsubscribe(ctx context.Context, id ID) error {
	if strings.TrimSpace(string(id)) == "" {
		return invalid("subscription ID required")
	}
	_, err := s.client.Remove(ctx, &contracts.RemoveEventStoreSubscriptions{TargetEventStore: string(s.store), SubscriptionIds: []string{string(id)}})
	return err
}

// GetAll returns detached definitions. Like C#, generation metadata is reduced
// to IDs; registering the returned definition sends generation-one filters.
func (s *Service) GetAll(ctx context.Context) ([]Definition, error) {
	result, err := s.client.GetSubscriptions(ctx, &contracts.GetEventStoreSubscriptionsRequest{TargetEventStore: string(s.store)})
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, faults.ErrProtocol
	}
	definitions := make([]Definition, 0, len(result.Items))
	for _, item := range result.Items {
		if item == nil || item.Identifier == "" || item.SourceEventStore == "" {
			return nil, faults.ErrProtocol
		}
		d := Definition{id: ID(item.Identifier), source: metadata.StoreName(item.SourceEventStore)}
		for _, event := range item.EventTypes {
			if event == nil || event.Id == "" {
				return nil, faults.ErrProtocol
			}
			d.types = append(d.types, events.TypeID(event.Id))
		}
		definitions = append(definitions, d)
	}
	return definitions, nil
}
func invalid(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}
