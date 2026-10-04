// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package outgoing is the private audit and outgoing-content preparation bridge.
package outgoing

import (
	"context"
	"errors"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/contentencoding"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/preparation"
	"github.com/cratis/chronicle.go/metadata"
)

// Config contains immutable configuration with borrowed callbacks.
type Config struct {
	Enrichers   []events.EventEnricher
	Identity    metadata.IdentityProvider
	Correlation metadata.CorrelationProvider
	Causation   metadata.CausationProvider
	Root        *metadata.Causation
}

// Binding is an internal transaction bridge capability.
type Binding struct{}

// Provider is implemented by SDK transports, not a public connection extension.
type Provider interface{ OutgoingConfiguration() Config }

// Audit holds detached, selected metadata. CorrelationPresent distinguishes a
// handled zero (fresh) from absence (eligible to inherit a bound unit's ID).
type Audit struct {
	Identity           identities.Identity
	Correlation        metadata.CorrelationID
	CorrelationPresent bool
	Causes             []metadata.Causation
}

// Resolve snapshots each provider immediately, before invoking the next one.
func (c Config) Resolve(ctx context.Context, id metadata.CorrelationID, explicit bool, inherited metadata.CorrelationID) (Audit, error) {
	result := Audit{Identity: metadata.Identity(ctx), Correlation: id, CorrelationPresent: explicit}
	if c.Identity != nil {
		err := preparation.Call(ctx, "identity", 0, -1, func() error {
			actor, handled, err := c.Identity(ctx)
			if err == nil && handled {
				result.Identity = actor.Snapshot()
			}
			return err
		})
		if err != nil {
			return Audit{}, err
		}
	}
	if !explicit {
		handled := false
		if c.Correlation != nil {
			err := preparation.Call(ctx, "correlation", 0, -1, func() error {
				selected, present, err := c.Correlation(ctx)
				if err == nil && present {
					result.Correlation, handled = selected, true
				}
				return err
			})
			if err != nil {
				return Audit{}, err
			}
		}
		if !handled {
			result.Correlation = metadata.Correlation(ctx)
		}
		result.CorrelationPresent = handled || result.Correlation != (metadata.CorrelationID{})
		if !result.CorrelationPresent {
			result.Correlation = inherited
		}
	}
	if result.Correlation == (metadata.CorrelationID{}) {
		var err error
		result.Correlation, err = metadata.NewCorrelationID()
		if err != nil {
			return Audit{}, err
		}
	}
	result.Causes = metadata.CausationChain(ctx)
	if c.Causation != nil {
		err := preparation.Call(ctx, "causation", 0, -1, func() error {
			chain, handled, err := c.Causation(ctx)
			if err == nil && handled {
				result.Causes = metadata.CausationChain(metadata.WithCausationChain(ctx, chain))
			}
			return err
		})
		if err != nil {
			return Audit{}, err
		}
	}
	hasRoot := false
	for _, cause := range result.Causes {
		if cause.Type == "Root" {
			hasRoot = true
			break
		}
	}
	if c.Root != nil && !hasRoot {
		root := metadata.CausationChain(metadata.WithCausationChain(ctx, []metadata.Causation{*c.Root}))
		result.Causes = append(root, result.Causes...)
	}
	if err := ctx.Err(); err != nil {
		return Audit{}, err
	}
	return result, nil
}

// Context derives owned audit values, retaining caller cancellation and metadata.
func (a Audit) Context(ctx context.Context) context.Context {
	ctx = metadata.WithIdentity(ctx, a.Identity)
	ctx = metadata.WithCorrelation(ctx, a.Correlation)
	return metadata.WithCausationChain(ctx, a.Causes)
}

// Encode completes base encoding before the first provider and never decodes it.
func (c Config) Encode(ctx context.Context, descriptor events.Descriptor, value any, explicitSubject bool, index int) ([]byte, error) {
	var providerFailure error
	baseContentFailure := false
	request := contentencoding.Request[events.EventContent]{Value: value, ReadOnly: !explicitSubject}
	request.Failed = func(provider int, contentFailed, panicked bool) error {
		// The failed base encoding is an unsupported serialization result.
		// Derive this SDK category here, never by inspecting application causes.
		baseContentFailure = baseContentFailure || (provider == -1 && contentFailed)
		if previous, ok := providerFailure.(*preparation.Error); ok {
			panicked = panicked || previous.Panicked
		}
		if providerFailure == nil || contentFailed {
			providerFailure = &preparation.Error{Phase: "content", ProviderIndex: provider, EventIndex: index, Panicked: panicked}
		}
		return providerFailure
	}
	for i, provider := range c.Enrichers {
		request.Providers = append(request.Providers, func(content *events.EventContent) error {
			providerFailure = preparation.Call(ctx, "enrichment", i, index, func() error { return provider(ctx, descriptor.Ref(), content) })
			return providerFailure
		})
	}
	var data []byte
	var err error
	// Only SDK-owned boundary errors escape; codec and ignored setter failures
	// cannot smuggle application errors through formatting or errors.Is/As.
	err = preparation.Call(ctx, "content", -1, index, func() error {
		data, err = descriptor.EncodeOutgoing(request)
		return err
	})
	if canceled := ctx.Err(); canceled != nil {
		return nil, canceled
	}
	if providerFailure != nil {
		if baseContentFailure {
			return nil, errors.Join(providerFailure, faults.ErrUnsupported)
		}
		return nil, providerFailure
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}
