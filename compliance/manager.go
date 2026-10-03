// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package compliance

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	contracts "github.com/cratis/chronicle.go/contracts/compliance"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
)

// ErrLifecycle identifies an unsuccessful or incomplete subject-key operation.
// A failure is not evidence that no key was erased. Retry erasure deliberately;
// never automatically authorize replacement keys as error recovery.
var ErrLifecycle = errors.New("chronicle: subject-key lifecycle failed")

// LifecycleError preserves error identity without printing identifiers or server
// diagnostics. Inspect Cause only within the application's sensitive-data boundary.
type LifecycleError struct {
	// Operation is ErasePII or AllowNewEncryptionKeyFor.
	Operation string
	// Cause preserves gRPC status, cancellation and ErrUnsupported identities.
	Cause error
}

// Error returns a payload-free description.
func (e *LifecycleError) Error() string { return ErrLifecycle.Error() + ": " + e.Operation }

// Unwrap exposes both the lifecycle category and underlying failure.
func (e *LifecycleError) Unwrap() []error { return []error{ErrLifecycle, e.Cause} }

// Manager borrows its transport and is safe for concurrent use. Its operations
// affect every event store in its namespace, not other namespaces. The kernel
// owns fencing, durable key deletion, peer-cache eviction and authorization.
type Manager struct {
	store     metadata.StoreName
	namespace metadata.Namespace
	client    contracts.ComplianceClient
}

// New constructs a manager without I/O. EventStore.Compliance supplies the normal
// connection lifecycle and registration barriers; adapters own those themselves.
func New(store metadata.StoreName, namespace metadata.Namespace, conn grpc.ClientConnInterface) (*Manager, error) {
	if strings.TrimSpace(string(store)) == "" || strings.TrimSpace(string(namespace)) == "" || conn == nil || (reflect.ValueOf(conn).Kind() == reflect.Pointer && reflect.ValueOf(conn).IsNil()) {
		return nil, fmt.Errorf("%w: store, namespace and transport required", faults.ErrInvalidConfiguration)
	}
	return &Manager{store: store, namespace: namespace, client: contracts.NewComplianceClient(conn)}, nil
}

// ErasePII fences and destroys the subject's PII keys across stores in this
// namespace. Repeated erasure is safe. Events remain stored but become unreadable;
// future PII writes are refused until deliberately reauthorized. Confidentiality
// keys are disjoint and never erased. No automatic retry or local key cache exists.
func (m *Manager) ErasePII(ctx context.Context, subject string) error {
	if m == nil || m.client == nil {
		return fmt.Errorf("%w: compliance manager required", faults.ErrInvalidConfiguration)
	}
	if err := validateSubject(ctx, subject); err != nil {
		return err
	}
	response, err := m.client.DeleteEncryptionKey(ctx, &contracts.DeleteEncryptionKeyRequest{EventStore: string(m.store), Namespace: string(m.namespace), Identifier: subject})
	if err == nil && response == nil {
		err = faults.ErrProtocol
	}
	if err != nil {
		return &LifecycleError{Operation: "ErasePII", Cause: wire.RPCError(err)}
	}
	return nil
}

// DeleteEncryptionKeyFor is the C#-named equivalent of ErasePII.
func (m *Manager) DeleteEncryptionKeyFor(ctx context.Context, identifier string) error {
	return m.ErasePII(ctx, identifier)
}

// AllowNewEncryptionKeyFor explicitly permits future PII writes after erasure.
// It does not recover erased ciphertext. Authorization is namespace-wide across
// stores. Old kernels report ErrUnsupported, never silent success.
func (m *Manager) AllowNewEncryptionKeyFor(ctx context.Context, identifier string) error {
	if m == nil || m.client == nil {
		return fmt.Errorf("%w: compliance manager required", faults.ErrInvalidConfiguration)
	}
	if err := validateSubject(ctx, identifier); err != nil {
		return err
	}
	response, err := m.client.AllowNewEncryptionKey(ctx, &contracts.AllowNewEncryptionKeyRequest{EventStore: string(m.store), Namespace: string(m.namespace), Identifier: identifier})
	if err == nil && response == nil {
		err = faults.ErrProtocol
	}
	if err != nil {
		return &LifecycleError{Operation: "AllowNewEncryptionKeyFor", Cause: wire.RPCError(err)}
	}
	return nil
}

func validateSubject(ctx context.Context, subject string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return ValidateSubject(subject)
}
