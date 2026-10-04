// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import "testing"

// Synthetic handles bypass EventStore acquisition; establish its invariant
// explicitly without registration I/O.
func testDefinitionStore(t *testing.T, store *EventStore) *EventStore {
	t.Helper()
	store.client.mu.Lock()
	defer store.client.mu.Unlock()
	var err error
	store.definitions, err = store.client.definitionsLocked(store.name)
	if err != nil {
		t.Fatal(err)
	}
	return store
}
