// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package metadata

import (
	"context"

	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/preparation"
)

// IdentityProvider selects the complete actor, including its on-behalf-of chain.
// A handled NotSet, Unknown or System is a real selection, not a fallback request.
// Providers are borrowed, synchronous and concurrent; no context is retained.
type IdentityProvider func(context.Context) (identities.Identity, bool, error)

// CorrelationProvider selects a correlation. Handled zero masks parent metadata
// and requests a fresh ID; explicit append options bypass this provider entirely.
type CorrelationProvider func(context.Context) (CorrelationID, bool, error)

// CausationProvider selects the complete chain. Handled empty masks the parent.
// Returned slices and maps are snapshotted immediately before other callbacks.
type CausationProvider func(context.Context) ([]Causation, bool, error)

// ProviderError is a payload-free outgoing callback failure; see
// events.PreparationError. It never retains or unwraps the application's error.
type ProviderError = preparation.Error

// IdentityFrom returns an owned actor and whether it was explicitly installed.
// WithIdentity(ctx, identities.NotSet()) is present and does not mean absence.
func IdentityFrom(ctx context.Context) (identities.Identity, bool) {
	value, ok := ctx.Value(identityKey{}).(identities.Identity)
	if !ok {
		return identities.NotSet(), false
	}
	return value.Snapshot(), true
}

// WithCausationChain replaces, rather than extends, a context's causation chain.
// Inputs and outputs are deeply copied; an empty chain masks the parent chain.
func WithCausationChain(ctx context.Context, chain []Causation) context.Context {
	return context.WithValue(ctx, causationKey{}, snapshotCauses(chain))
}

func snapshotCauses(chain []Causation) []Causation {
	result := make([]Causation, len(chain))
	for i, cause := range chain {
		result[i] = cause
		result[i].Properties = make(map[string]string, len(cause.Properties))
		for key, value := range cause.Properties {
			result[i].Properties[key] = value
		}
	}
	return result
}
