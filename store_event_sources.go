// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import "github.com/cratis/chronicle.go/eventsources"

// EventSources returns store-wide event source registration. Definitions are
// not namespace-scoped, and registration adds or replaces by name without
// removing other names. Calls retain the store's registration/lifecycle barrier.
func (s *EventStore) EventSources() *eventsources.Service {
	service, _ := eventsources.New(s.name, &clientTransport{client: s.client, store: s}) // Validated coordinates and connection.
	return service
}
