// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"strings"

	"github.com/cratis/chronicle.go/contracts/namespaces"
	"github.com/cratis/chronicle.go/internal/wire"
)

// Namespaces administers namespaces for one logical store. Handles are concurrency-safe.
type Namespaces struct{ store *EventStore }

// Namespaces returns administration for this logical store, not only the selected namespace.
func (s *EventStore) Namespaces() *Namespaces { return &Namespaces{store: s} }

// List returns existing namespaces; namespace selection is not authorization.
func (n *Namespaces) List(ctx context.Context) ([]Namespace, error) {
	result, err := namespaces.NewNamespacesClient(n.store.client.transport).AllNamespaces(ctx, &namespaces.AllNamespacesRequest{EventStore: string(n.store.name)})
	if err != nil {
		return nil, err
	}
	if err = wire.CheckEnvelope(result); err != nil {
		return nil, err
	}
	values := make([]Namespace, len(result.Data))
	for i, namespace := range result.Data {
		if namespace == nil {
			return nil, ErrProtocol
		}
		values[i] = Namespace(namespace.Name)
	}
	return values, nil
}

// Remove irreversibly removes the explicit target namespace, its observers, their
// reminders and the data Chronicle stores for it. It uses the handle's store and
// credentials, not its selected namespace. Before calling, the operator must stop
// every client using the target; neither the SDK nor the kernel enforces this
// prerequisite. There is no confirmation, client-side authorization check or retry.
// Kernel authorization applies; the handle is not an administrative permission.
// Removing a missing namespace is a no-op. The kernel rejects the default
// namespace (case-insensitively) and the system store. An error or cancellation
// does not prove rollback; inspect server state before explicitly retrying.
// Already-issued handles are not evicted, closed or invalidated by this call.
func (n *Namespaces) Remove(ctx context.Context, namespace Namespace) error {
	if strings.TrimSpace(string(namespace)) == "" {
		return fmt.Errorf("%w: namespace must be nonblank", ErrInvalidConfiguration)
	}
	result, err := namespaces.NewNamespacesClient(n.store.client.transport).RemoveNamespace(ctx, &namespaces.RemoveNamespaceRequest{EventStore: string(n.store.name), Namespace: string(namespace)})
	if err != nil {
		return err
	}
	return wire.CheckEnvelope(result)
}

// Ensure idempotently creates a nonblank namespace in this store.
func (n *Namespaces) Ensure(ctx context.Context, namespace Namespace) error {
	if strings.TrimSpace(string(namespace)) == "" {
		return fmt.Errorf("%w: empty namespace", ErrInvalidConfiguration)
	}
	result, err := namespaces.NewNamespacesClient(n.store.client.transport).EnsureNamespace(ctx, &namespaces.EnsureNamespaceRequest{EventStore: string(n.store.name), Namespace: string(namespace)})
	if err != nil {
		return err
	}
	return wire.CheckEnvelope(result)
}
