// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"github.com/cratis/chronicle.go/seeding"
)

type preparationState uint8

const (
	preparationCaptured preparationState = iota
	preparationRunning
	preparationSucceeded
	preparationFailed
)

// ClientPreparation captures declarations for one client identity. Construct with
// CaptureClient; the zero value is invalid. Do not copy it. Prepare is synchronous
// and caller-owned: Close cancels but does not join preparation or its cleanup.
// Join any outstanding Prepare call before closing the borrowed scope provider.
type ClientPreparation struct {
	client   *Client
	selected []*registryDeclarations
	defaults *registryDeclarations
	names    []StoreName
	stores   map[StoreName]*registryDeclarations
}

// CaptureClient applies ordinary ClientOptions, validates configuration and copies
// selected declaration collections without SDK I/O or schema, definition, service,
// or selector callbacks. Arbitrary ClientOption functions themselves still execute.
// Immutable descriptor plans and borrowed callbacks keep their existing ownership.
func CaptureClient(options ...ClientOption) (*ClientPreparation, error) {
	config := clientConfig{uri: "chronicle://localhost:35000", connectTimeout: 5 * time.Second,
		maxSendMessageSize: defaultMaxMessageSize, maxReceiveMessageSize: defaultMaxMessageSize,
		keepAliveTimeout: 5 * time.Second, reactorRetryWait: connection.Wait, registrationRetry: RegistrationRetry{MaxAttempts: 5, InitialDelay: 2 * time.Second, MaximumDelay: 30 * time.Second, AttemptTimeout: 30 * time.Second}}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: nil client option", ErrInvalidConfiguration)
		}
		option(&config)
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
	c.balancer = connection.NewBalancer(uri.loadBalancer, tlsConfig)
	c.life, c.cancel = context.WithCancel(context.Background())
	c.transport = &clientTransport{client: c}
	p.client = c
	return p, nil
}

// Client returns the same unprepared identity on every call, for an explicit
// borrowed binding (for example dependencyinjection.BindValue). Never transfer
// ownership to a provider or use Client by value. Operations require Prepare.
func (p *ClientPreparation) Client() *Client { return p.client }

// Prepare admits at most one attempt. Nil scopes selects captured WithServices or
// the container-free default. A typed nil or explicit scopes plus WithServices is
// invalid. Before admission, a nil context or invalid scopes returns an error
// without consuming the attempt; an already canceled context consumes it.
// While running (including cleanup), all calls return ErrPreparationInProgress.
// After completion, arguments are ignored and the exact retained result is returned,
// including a historically prepared identity subsequently closed. Retry requires
// a new CaptureClient. The selected scopes are borrowed for preparation and runtime.
// Callbacks may handle ErrNotPrepared; denied operations never poison publication.
func (p *ClientPreparation) Prepare(ctx context.Context, scopes reactors.ScopeFactory) (*Client, error) {
	if p == nil || p.client == nil {
		return nil, invalidFactory("client preparation required")
	}
	c := p.client
	c.mu.Lock()
	switch c.preparation {
	case preparationRunning:
		c.mu.Unlock()
		return nil, &ClientStateError{Operation: "prepare", cause: ErrPreparationInProgress}
	case preparationSucceeded:
		c.mu.Unlock()
		return c, nil
	case preparationFailed:
		err := c.preparationError
		c.mu.Unlock()
		return nil, err
	}
	if nilValue(ctx) || (scopes != nil && nilValue(scopes)) || (scopes != nil && c.config.reactorServicesSet) {
		c.mu.Unlock()
		return nil, invalidFactory("invalid preparation context or conflicting scope factory")
	}
	if scopes == nil {
		scopes = c.config.reactorServices
	}
	if scopes == nil {
		scopes = artifacts.DefaultScopeFactory()
	}
	c.preparation = preparationRunning
	c.mu.Unlock()

	// No work/generation lease: a callback or closer may Close this identity.
	preparationCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(c.life, cancel)
	defer stop()
	defer cancel()
	var compiled map[*registryDeclarations]registrySnapshot
	runtimeServices := clientIdentityServices(scopes, c)
	err := artifacts.Protect("client", "prepare", func() error {
		if err := preparationCtx.Err(); err != nil {
			return err
		}
		if err := c.life.Err(); err != nil {
			return ErrClosed
		}
		var compileErr error
		compiled, compileErr = p.compile(preparationCtx, scopes, runtimeServices)
		return compileErr
	})
	// Publication is all-or-nothing, after every scope and owned result is closed.
	c.mu.Lock()
	defer c.mu.Unlock()
	err = artifacts.Protect("client", "publish", func() error {
		var closedErr error
		if c.closed || c.life.Err() != nil {
			closedErr = ErrClosed
		}
		callerErr, preparationErr := ctx.Err(), preparationCtx.Err()
		if callerErr == nil && preparationErr == nil && closedErr == nil {
			return err
		}
		return errors.Join(err, callerErr, preparationErr, closedErr)
	})
	if err != nil {
		c.preparation, c.preparationError = preparationFailed, err
		return nil, err
	}
	frozen := compiled[p.defaults]
	c.catalog, c.constraints, c.readModelCatalog = frozen.events, frozen.constraints, frozen.models
	c.projections, c.seeds = frozen.projections, frozen.seeds
	c.reactors.defaults, c.reducers.defaults, c.readModelReactors.defaults = frozen.reactors, frozen.reducers, frozen.readModelReactors
	for _, name := range p.names {
		frozen := compiled[p.stores[name]]
		c.catalogs[name], c.storeConstraints[name] = frozen.events, frozen.constraints
		c.readModelCatalogs[name], c.storeProjections[name] = frozen.models, frozen.projections
		c.reactors.stores[name], c.reducers.stores[name] = frozen.reactors, frozen.reducers
		c.readModelReactors.stores[name], c.storeSeeds[name] = frozen.readModelReactors, frozen.seeds
	}
	c.config.reactorServices = runtimeServices
	c.preparation = preparationSucceeded
	return c, nil
}

func (p *ClientPreparation) compile(ctx context.Context, scopes, runtimeServices reactors.ScopeFactory) (map[*registryDeclarations]registrySnapshot, error) {
	plans := make(map[*registryDeclarations]registryFactoryPlans)
	// All metadata and visible constructor dependencies precede the first scope.
	for _, declarations := range p.selected {
		if err := artifacts.Protect("registry", "metadata", func() error { return validateDefinitionMetadata(declarations) }); err != nil {
			return nil, err
		}
		plan, err := preflightDefinitionFactories(declarations, scopes, p.client)
		if err != nil {
			return nil, err
		}
		plans[declarations] = plan
	}
	schemas := make(map[*registryDeclarations]registrySchemas)
	for _, declarations := range p.selected {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		prepared, err := prepareRegistrySchemas(declarations, p.client.config.naming)
		if err != nil {
			return nil, err
		}
		schemas[declarations] = prepared
	}
	compiled := make(map[*registryDeclarations]registrySnapshot)
	for _, declarations := range p.selected {
		frozen, err := compilePreparedRegistry(ctx, declarations, schemas[declarations], runtimeServices, p.client.config.validateEventTypes, plans[declarations])
		if err != nil {
			return nil, err
		}
		compiled[declarations] = frozen
	}
	return compiled, nil
}

func (c *Client) requirePrepared(operation string, offline bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.requirePreparedLocked(operation, offline)
}

func (c *Client) requirePreparedLocked(operation string, offline bool) error {
	if c.closed && (!offline || c.preparation != preparationSucceeded) {
		return &ClientStateError{Operation: operation, cause: ErrClosed}
	}
	if c.preparation != preparationSucceeded {
		return &ClientStateError{Operation: operation, cause: ErrNotPrepared}
	}
	return nil
}
