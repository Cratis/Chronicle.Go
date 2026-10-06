// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/cratis/chronicle.go/internal/diagnostics"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"github.com/cratis/chronicle.go/seeding"
)

// CaptureClient applies ordinary ClientOptions, validates configuration and copies
// selected declaration collections without SDK I/O or schema, definition, service,
// or selector callbacks. Arbitrary ClientOption functions themselves still execute.
// Immutable descriptor plans and borrowed callbacks keep their existing ownership.
// Logger and handler identities are captured, not their mutable destination state;
// see WithLogger for the pristine slog default's standard-log bridge exception.
func CaptureClient(options ...ClientOption) (*ClientPreparation, error) {
	config := clientConfig{logger: slog.Default(), uri: "chronicle://localhost:35000", connectTimeout: 5 * time.Second,
		maxSendMessageSize: defaultMaxMessageSize, maxReceiveMessageSize: defaultMaxMessageSize, defaultSinkType: readmodels.MongoDB,
		keepAliveTimeout: 5 * time.Second, reactorRetryWait: connection.Wait, registrationRetry: RegistrationRetry{MaxAttempts: 5, InitialDelay: 2 * time.Second, MaximumDelay: 30 * time.Second, AttemptTimeout: 30 * time.Second}}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: nil client option", ErrInvalidConfiguration)
		}
		option(&config)
	}
	if err := captureOutgoing(&config); err != nil {
		return nil, err
	}
	if err := validateDefaultSinkType(config.defaultSinkType); err != nil {
		return nil, err
	}
	uri, tlsConfig, err := validateConfig(config)
	if err != nil {
		return nil, err
	}
	p := &ClientPreparation{names: slices.Sorted(maps.Keys(config.stores)), stores: make(map[StoreName]*registryDeclarations)}
	captured := make(map[*Registry]*registryDeclarations)
	capture := func(registry *Registry) *registryDeclarations {
		if declarations, ok := captured[registry]; ok {
			return declarations
		}
		declarations := captureRegistry(registry)
		logging := diagnostics.Configuration{Logger: config.logger}
		for i, declaration := range declarations.reactors {
			declarations.reactors[i] = declaration.WithClientDiagnostics(logging)
		}
		for i, declaration := range declarations.reducers {
			declarations.reducers[i] = declaration.WithClientDiagnostics(logging)
		}
		for i, declaration := range declarations.readModelReactors {
			declarations.readModelReactors[i] = declaration.WithClientDiagnostics(logging)
		}
		captured[registry] = declarations
		p.selected = append(p.selected, declarations)
		return declarations
	}
	p.defaults = capture(config.registry)
	for _, name := range p.names {
		if strings.TrimSpace(string(name)) == "" {
			return nil, invalidFactory("empty registry store name")
		}
		p.stores[name] = capture(config.stores[name])
	}
	if config.skipKeepAlive {
		for _, declarations := range p.selected {
			if declarations.hasObservers() {
				return nil, invalidFactory("SkipKeepAlive cannot run reactors, reducers or read-model reactors; kernel subscriptions require a logical connection session")
			}
		}
	}
	config.registry, config.stores = nil, nil
	config.skipCompatibility = config.skipCompatibility || uri.skipCompatibility
	if config.resolver == nil {
		config.resolver = connection.Resolver(uri.nameServer)
	}
	c := &Client{config: config, uri: uri, tls: tlsConfig,
		catalogs: make(map[StoreName]*events.Catalog), storeConstraints: make(map[StoreName][]constraints.Definition), stores: make(map[storeKey]*EventStore),
		readModelCatalogs: make(map[StoreName]*readmodels.Catalog), storeProjections: make(map[StoreName][]projections.Definition),
		storeSeeds: make(map[StoreName]seeding.Definition), changed: make(chan struct{}), closeDone: make(chan struct{}),
		reactors:          reactorCatalogs{stores: make(map[StoreName][]*reactorPlan)},
		readModelReactors: readModelReactorCatalogs{stores: make(map[StoreName][]*reactors.ReadModelPlan)},
		reducers:          reducerCatalogs{stores: make(map[StoreName][]*reducers.Plan)}}
	if config.loadBalancer == nil {
		c.balancer = connection.NewBalancer(uri.loadBalancer, tlsConfig)
	}
	c.hooks = connectionHooks{connected: slices.Clone(config.connectedHooks), disconnected: slices.Clone(config.disconnectedHooks), wake: make(chan struct{}, 1)}
	c.config.connectedHooks, c.config.disconnectedHooks = nil, nil
	c.life, c.cancel = context.WithCancel(context.Background())
	c.transport = &clientTransport{client: c}
	p.client = c
	return p, nil
}
