// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import "github.com/cratis/chronicle.go/alerts"

// Alerts returns namespace-scoped alert incident queries. Calls retain the
// store's registration/lifecycle barrier. Readiness other than Ready means
// results can be incomplete.
func (s *EventStore) Alerts() *alerts.Service {
	service, _ := alerts.New(s.name, s.namespace, &clientTransport{client: s.client, store: s}) // Validated coordinates and connection.
	return service
}
