// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package metadata carries immutable correlation, causation and actor snapshots in contexts.
// Store and namespace selection is explicit, not an authorization decision.
package metadata

import (
	"context"
	"maps"
	"time"

	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/fundamentals.go/correlation"
	"github.com/google/uuid"
)

// StoreName identifies a logical event store.
type StoreName string

// Namespace identifies isolated event state within a store.
type Namespace string

// DefaultNamespace is used when none is selected.
const DefaultNamespace Namespace = "Default"

// CorrelationID is an RFC 4122 UUID; its zero value means not supplied.
// Text and JSON use the canonical UUID form, never BCL byte order.
type CorrelationID uuid.UUID

// NewCorrelationID creates a random UUID using cryptographic randomness.
func NewCorrelationID() (CorrelationID, error) {
	id, err := uuid.NewRandom()
	return CorrelationID(id), err
}

// ParseCorrelationID parses a UUID in one of the forms accepted by google/uuid.
func ParseCorrelationID(value string) (CorrelationID, error) {
	id, err := uuid.Parse(value)
	return CorrelationID(id), err
}

// String returns the canonical UUID representation.
func (id CorrelationID) String() string { return uuid.UUID(id).String() }

// MarshalText implements encoding.TextMarshaler.
func (id CorrelationID) MarshalText() ([]byte, error) { return uuid.UUID(id).MarshalText() }

// UnmarshalText implements encoding.TextUnmarshaler.
func (id *CorrelationID) UnmarshalText(data []byte) error {
	var value uuid.UUID
	if err := value.UnmarshalText(data); err != nil {
		return err
	}
	*id = CorrelationID(value)
	return nil
}

// Causation records one ordered audit link. Properties must not contain secrets or PII.
type Causation struct {
	// Occurred is the time the cause occurred.
	Occurred time.Time
	// Type is the kind of cause.
	Type string
	// Properties contains exact string-valued audit facts.
	Properties map[string]string
}

type identityKey struct{}
type causationKey struct{}

// WithCorrelation returns a derived context; it does not mutate ctx.
func WithCorrelation(ctx context.Context, id CorrelationID) context.Context {
	return correlation.WithID(ctx, correlation.ID([16]byte(id)))
}

// Correlation returns the correlation or its zero value when absent.
func Correlation(ctx context.Context) CorrelationID {
	return CorrelationID([16]byte(correlation.FromContext(ctx)))
}

// WithIdentity stores a defensive, deduplicated actor snapshot.
func WithIdentity(ctx context.Context, actor identities.Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, actor.Snapshot())
}

// Identity returns a copy of the actor chain, or Chronicle's NotSet identity.
func Identity(ctx context.Context) identities.Identity {
	if value, ok := ctx.Value(identityKey{}).(identities.Identity); ok {
		return value.Snapshot()
	}
	return identities.NotSet()
}

// WithCausation appends one copied audit link, preserving the parent chain.
func WithCausation(ctx context.Context, cause Causation) context.Context {
	chain := CausationChain(ctx)
	cause.Properties = maps.Clone(cause.Properties)
	return context.WithValue(ctx, causationKey{}, append(chain, cause))
}

// CausationChain returns a deep copy of the ordered chain; absence is an empty slice.
func CausationChain(ctx context.Context) []Causation {
	value, _ := ctx.Value(causationKey{}).([]Causation)
	result := make([]Causation, len(value))
	for i, cause := range value {
		result[i] = cause
		result[i].Properties = maps.Clone(cause.Properties)
	}
	return result
}
