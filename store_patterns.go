// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import "github.com/cratis/chronicle.go/patterns"

// Patterns returns an immutable store/namespace query facade borrowing this
// client. Calls retain authentication, registration and generation cancellation
// barriers. Construction performs no I/O and creates no cross-store cache.
func (s *EventStore) Patterns() *patterns.Service {
	service, _ := patterns.New(s.name, s.namespace, &clientTransport{client: s.client, store: s}) // Validated store coordinates and non-nil borrowed transport.
	return service
}
