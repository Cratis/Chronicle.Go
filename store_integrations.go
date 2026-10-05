// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"slices"

	"github.com/cratis/chronicle.go/captures"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventstoresubscriptions"
	"github.com/cratis/chronicle.go/externalservices"
	"github.com/cratis/chronicle.go/webhooks"
)

// Subscriptions returns a store-wide cross-store subscription service. Namespace
// handles share definitions; forwarding uses matching namespaces in both stores.
// Operations retain the store's registration barrier across connection generations.
func (s *EventStore) Subscriptions() *eventstoresubscriptions.Service {
	service, _ := eventstoresubscriptions.New(s.name, s.catalog, &clientTransport{client: s.client, store: s}) // Validated store/catalog and non-nil transport.
	return service
}

// Webhooks returns store-wide outgoing webhook definitions, not a local HTTP server.
func (s *EventStore) Webhooks() *webhooks.Service {
	service, _ := webhooks.New(s.name, s.catalog, &clientTransport{client: s.client, store: s}) // Validated coordinates.
	return service
}

// ExternalServices returns store-wide HTTP/database declarations. The kernel,
// never the SDK, owns external connections and credential acquisition.
func (s *EventStore) ExternalServices() *externalservices.Service {
	service, _ := externalservices.New(s.name, &clientTransport{client: s.client, store: s}) // Validated coordinates.
	return service
}

// Captures returns explicit capture validation/saving. Saving never activates a
// capture; unsupported source/mapping capabilities remain inspectable errors.
func (s *EventStore) Captures() *captures.Service {
	service, _ := captures.New(s.name, &clientTransport{client: s.client, store: s}) // Validated coordinates.
	return service
}

func (s *EventStore) externalSubscriptions() []eventstoresubscriptions.Definition {
	byStore := map[string]map[events.TypeID]bool{}
	add := func(store string, ids []events.TypeID) {
		if store == "" || store == string(s.name) {
			return
		}
		if byStore[store] == nil {
			byStore[store] = map[events.TypeID]bool{}
		}
		for _, id := range ids {
			byStore[store][id] = true
		}
	}
	for _, plan := range s.reactorPlans() {
		ids := make([]events.TypeID, 0)
		for _, ref := range plan.EventTypes() {
			ids = append(ids, ref.ID)
		}
		add(plan.SourceStore(), ids)
	}
	for _, plan := range s.reducerPlans() {
		ids := make([]events.TypeID, 0)
		for _, ref := range plan.EventTypes() {
			ids = append(ids, ref.ID)
		}
		add(plan.SourceStore(), ids)
	}
	for _, definition := range s.projectionDefinitions() {
		source, ids := definition.ExternalSubscription()
		add(source, ids)
	}
	stores := make([]string, 0, len(byStore))
	for store := range byStore {
		stores = append(stores, store)
	}
	slices.Sort(stores)
	result := make([]eventstoresubscriptions.Definition, 0, len(stores))
	for _, store := range stores {
		ids := make([]events.TypeID, 0, len(byStore[store]))
		for id := range byStore[store] {
			ids = append(ids, id)
		}
		slices.Sort(ids)
		definition, _ := eventstoresubscriptions.Define(s.catalog, eventstoresubscriptions.ID(store), StoreName(store), ids...) // Compiled artifact metadata is validated.
		result = append(result, definition)
	}
	return result
}

func (s *EventStore) needsExternalSubscriptionRegistration(g *generation) bool {
	if len(s.externalSubscriptions()) == 0 {
		return false
	}
	outcome := g.registrations.For(fmt.Sprintf("external-subscriptions:%q", s.name)).Snapshot()
	return !outcome.HasRun || outcome.RetryPending
}

func (s *EventStore) registerExternalSubscriptions(ctx context.Context, g *generation) error {
	return s.sharedStage(ctx, g, "external-subscriptions", func(ctx context.Context) error {
		service, err := eventstoresubscriptions.New(s.name, s.catalog, g.transport)
		if err != nil {
			return err
		}
		for _, definition := range s.externalSubscriptions() {
			if err = service.Register(ctx, definition); err != nil {
				return err
			}
		}
		return nil
	})
}
