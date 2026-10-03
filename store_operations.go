// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"github.com/cratis/chronicle.go/jobs"
	"github.com/cratis/chronicle.go/observation"
)

// Observers returns namespace-scoped observer administration. Persistent Remove
// deliberately affects every namespace in this store; local Unregister methods
// only cancel this client's subscriptions and do not delete server records.
func (s *EventStore) Observers() *observation.Service {
	service, _ := observation.New(s.name, s.namespace, &clientTransport{client: s.client, store: s}) // Validated coordinates and connection.
	return service
}

// Jobs returns namespace-scoped job administration. Job absence is not evidence
// of successful completion. Calls retain the store's registration/lifecycle barrier.
func (s *EventStore) Jobs() *jobs.Service {
	service, _ := jobs.New(s.name, s.namespace, &clientTransport{client: s.client, store: s}) // Validated coordinates and connection.
	return service
}
