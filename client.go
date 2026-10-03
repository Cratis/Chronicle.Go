// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"crypto/tls"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
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

// Client owns a generation supervisor and frozen registries. Construct with
// NewClient or Dial; the zero value is not usable. It is safe for concurrent use.
type Client struct {
	mu               sync.Mutex
	closed           bool
	closeOnce        sync.Once
	closeDone        chan struct{}
	closeError       error
	life             context.Context
	cancel           context.CancelFunc
	work             sync.WaitGroup
	transport        *clientTransport
	config           clientConfig
	uri              ConnectionString
	tls              *tls.Config
	balancer         *connection.Balancer
	catalog          *events.Catalog
	catalogs         map[StoreName]*events.Catalog
	constraints      []constraints.Definition
	storeConstraints map[StoreName][]constraints.Definition
	stores           map[storeKey]*EventStore
	current          *generation
	supervisor       *supervision
	changed          chan struct{}
	nextGeneration   uint64
	connectionError  error

	readModelCatalog  *readmodels.Catalog
	readModelCatalogs map[StoreName]*readmodels.Catalog
	projections       []projections.Definition
	storeProjections  map[StoreName][]projections.Definition
	reactors          reactorCatalogs
	reducers          reducerCatalogs
	seeds             seeding.Definition
	storeSeeds        map[StoreName]seeding.Definition
	readModelReactors readModelReactorCatalogs
}

// String describes the client without revealing endpoints or credentials.
func (c *Client) String() string { return "Chronicle client" }

// GoString is the credential-safe representation for fmt's %#v format.
func (c *Client) GoString() string { return c.String() }

// NewClient validates and freezes configuration without network I/O. TLS validates
// by default. Omitted credentials select Chronicle's public development credentials.
// All selected registries are captured before application preparation. Definition
// factories and seeders run synchronously once per distinct registry; reconnect
// reuses their frozen outputs without application preparation callbacks.
// Use NewClientContext when preparation scopes need caller cancellation.
func NewClient(options ...ClientOption) (*Client, error) {
	return NewClientContext(context.Background(), options...)
}

// NewClientContext is NewClient with a context for definition preparation and
// optional scoped construction. The context is not retained by the client.
func NewClientContext(ctx context.Context, options ...ClientOption) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config := clientConfig{uri: "chronicle://localhost:35000", connectTimeout: 5 * time.Second,
		maxSendMessageSize: defaultMaxMessageSize, maxReceiveMessageSize: defaultMaxMessageSize, defaultSinkType: readmodels.MongoDB,
		keepAliveTimeout: 5 * time.Second, reactorRetryWait: connection.Wait, registrationRetry: RegistrationRetry{MaxAttempts: 5, InitialDelay: 2 * time.Second, MaximumDelay: 30 * time.Second, AttemptTimeout: 30 * time.Second}}
	for _, option := range options {
		if option == nil {
			return nil, fmt.Errorf("%w: nil client option", ErrInvalidConfiguration)
		}
		option(&config)
	}
	if err := validateDefaultSinkType(config.defaultSinkType); err != nil {
		return nil, err
	}
	uri, tlsConfig, err := validateConfig(config)
	if err != nil {
		return nil, err
	}
	// Capture every selected declaration epoch before any application preparation.
	// Shared registry references use one capture and one compilation, even if a
	// callback later changes that registry for a future client.
	names := slices.Sorted(maps.Keys(config.stores))
	captured := make(map[*Registry]*registryDeclarations)
	var selected []*registryDeclarations
	capture := func(registry *Registry) *registryDeclarations {
		if declarations, ok := captured[registry]; ok {
			return declarations
		}
		declarations := captureRegistry(registry)
		captured[registry] = declarations
		selected = append(selected, declarations)
		return declarations
	}
	defaults := capture(config.registry)
	for _, name := range names {
		if strings.TrimSpace(string(name)) == "" {
			return nil, fmt.Errorf("%w: empty registry store name", ErrInvalidConfiguration)
		}
		capture(config.stores[name])
	}
	if config.skipKeepAlive {
		for _, declarations := range captured {
			if declarations.hasObservers() {
				return nil, fmt.Errorf("%w: SkipKeepAlive cannot run reactors, reducers or read-model reactors; kernel subscriptions require a logical connection session", ErrInvalidConfiguration)
			}
		}
	}
	for _, declarations := range selected {
		if err := artifacts.Protect("registry", "metadata", func() error { return validateDefinitionMetadata(declarations) }); err != nil {
			return nil, err
		}
	}
	// Finish every selected base schema before any definition constructor or
	// composition callback can change application configuration for another store.
	schemas := make(map[*registryDeclarations]registrySchemas)
	for _, declarations := range selected {
		prepared, err := prepareRegistrySchemasWithSink(declarations, config.naming, config.defaultSinkType)
		if err != nil {
			return nil, err
		}
		schemas[declarations] = prepared
	}
	compiled := make(map[*registryDeclarations]registrySnapshot)
	compile := func(declarations *registryDeclarations) (registrySnapshot, error) {
		if frozen, ok := compiled[declarations]; ok {
			return frozen, nil
		}
		frozen, err := compilePreparedRegistry(ctx, declarations, schemas[declarations], config.reactorServices, config.validateEventTypes)
		if err == nil {
			compiled[declarations] = frozen
		}
		return frozen, err
	}
	frozen, err := compile(defaults)
	if err != nil {
		return nil, err
	}
	c := &Client{config: config, uri: uri, tls: tlsConfig, catalog: frozen.events, constraints: frozen.constraints,
		catalogs: make(map[StoreName]*events.Catalog), storeConstraints: make(map[StoreName][]constraints.Definition), stores: make(map[storeKey]*EventStore),
		readModelCatalog: frozen.models, readModelCatalogs: make(map[StoreName]*readmodels.Catalog),
		projections: frozen.projections, storeProjections: make(map[StoreName][]projections.Definition),
		seeds: frozen.seeds, storeSeeds: make(map[StoreName]seeding.Definition),
		changed: make(chan struct{}), closeDone: make(chan struct{}),
		reactors:          reactorCatalogs{defaults: frozen.reactors, stores: make(map[StoreName][]*reactorPlan)},
		readModelReactors: readModelReactorCatalogs{defaults: frozen.readModelReactors, stores: make(map[StoreName][]*reactors.ReadModelPlan)},
		reducers:          reducerCatalogs{defaults: frozen.reducers, stores: make(map[StoreName][]*reducers.Plan)}}
	for _, name := range names {
		frozen, err := compile(captured[config.stores[name]])
		if err != nil {
			return nil, err
		}
		c.catalogs[name], c.storeConstraints[name] = frozen.events, frozen.constraints
		c.readModelCatalogs[name], c.storeProjections[name] = frozen.models, frozen.projections
		c.reactors.stores[name] = frozen.reactors
		c.readModelReactors.stores[name] = frozen.readModelReactors
		c.reducers.stores[name] = frozen.reducers
		c.storeSeeds[name] = frozen.seeds
	}
	if config.skipKeepAlive {
		if len(c.reactors.defaults)+len(c.reducers.defaults)+len(c.readModelReactors.defaults) != 0 {
			return nil, fmt.Errorf("%w: SkipKeepAlive cannot run reactors, reducers or read-model reactors; kernel subscriptions require a logical connection session", ErrInvalidConfiguration)
		}
		for _, name := range slices.Sorted(maps.Keys(config.stores)) {
			if len(c.reactors.stores[name])+len(c.reducers.stores[name])+len(c.readModelReactors.stores[name]) != 0 {
				return nil, fmt.Errorf("%w: SkipKeepAlive cannot run reactors, reducers or read-model reactors; kernel subscriptions require a logical connection session", ErrInvalidConfiguration)
			}
		}
	}
	c.config.registry, c.config.stores = nil, nil
	c.config.skipCompatibility = config.skipCompatibility || uri.skipCompatibility
	if c.config.resolver == nil {
		c.config.resolver = connection.Resolver(uri.nameServer)
	}
	c.balancer = connection.NewBalancer(uri.loadBalancer, tlsConfig)
	c.life, c.cancel = context.WithCancel(context.Background())
	c.transport = &clientTransport{client: c}
	return c, nil
}

func validateConfig(config clientConfig) (ConnectionString, *tls.Config, error) {
	if err := config.naming.Validate(); err != nil {
		return ConnectionString{}, nil, err
	}
	if config.reactorServicesSet && nilValue(config.reactorServices) {
		return ConnectionString{}, nil, fmt.Errorf("%w: nil scope factory", ErrInvalidConfiguration)
	}
	if (config.tlsSet && config.tls == nil) || (config.borrowedSet && nilValue(config.borrowed)) ||
		(config.tokenSet && nilValue(config.tokenSource)) || (config.resolverSet && nilValue(config.resolver)) {
		return ConnectionString{}, nil, fmt.Errorf("%w: nil TLS, transport, resolver or token source", ErrInvalidConfiguration)
	}
	if config.borrowed != nil && !config.uriSet && !config.tokenSet && !config.noAuth {
		return ConnectionString{}, nil, fmt.Errorf("%w: borrowed connection requires explicit authentication", ErrInvalidConfiguration)
	}
	uri, err := ParseConnectionString(config.uri)
	if err != nil {
		return uri, nil, err
	}
	if uri.apiKey != "" || len(uri.unsupported) > 0 {
		return uri, nil, fmt.Errorf("%w: API keys and certificate/plaintext URI options; use WithTLS for PEM material", ErrUnsupported)
	}
	if config.borrowed != nil && (uri.srv || len(uri.addresses) > 1 || uri.loadBalancer != "") {
		return uri, nil, fmt.Errorf("%w: borrowed channels own endpoint selection", ErrInvalidConfiguration)
	}
	if (config.maxSendMessageSizeSet && (config.maxSendMessageSize < 1 || config.maxSendMessageSize > math.MaxInt32)) ||
		(config.maxReceiveMessageSizeSet && (config.maxReceiveMessageSize < 1 || config.maxReceiveMessageSize > math.MaxInt32)) {
		return uri, nil, fmt.Errorf("%w: message sizes must be between 1 and 2147483647 bytes", ErrInvalidConfiguration)
	}
	if config.skipKeepAlive && config.keepAliveTimeoutSet {
		return uri, nil, fmt.Errorf("%w: SkipKeepAlive conflicts with an explicit keepalive timeout", ErrInvalidConfiguration)
	}
	policy := config.registrationRetry
	if config.connectTimeout <= 0 || config.keepAliveTimeout <= 0 || policy.MaxAttempts < 1 || policy.MaxAttempts > 100 || policy.InitialDelay <= 0 || policy.MaximumDelay < policy.InitialDelay || policy.AttemptTimeout <= 0 {
		return uri, nil, fmt.Errorf("%w: invalid lifecycle timeout or retry policy", ErrInvalidConfiguration)
	}
	if (config.noAuth && (uri.explicitCredentials || config.tokenSource != nil)) || (config.tokenSource != nil && (uri.noAuth || uri.explicitCredentials)) {
		return uri, nil, fmt.Errorf("%w: conflicting authentication options", ErrInvalidConfiguration)
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if config.tls != nil {
		tlsConfig = config.tls.Clone()
		if tlsConfig.MinVersion == 0 {
			tlsConfig.MinVersion = tls.VersionTLS12
		}
		if tlsConfig.MinVersion < tls.VersionTLS12 {
			return uri, nil, fmt.Errorf("%w: TLS 1.2 or newer required", ErrInvalidConfiguration)
		}
	} else {
		tlsConfig.InsecureSkipVerify = uri.skipTLS || (config.development && !uri.tlsSpecified)
	}
	return uri, tlsConfig, nil
}

func nilValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Func, reflect.Interface, reflect.Slice, reflect.Chan:
		return reflected.IsNil()
	default:
		return false
	}
}

// Dial constructs and connects a client. Startup failure closes owned resources.
func Dial(ctx context.Context, options ...ClientOption) (*Client, error) {
	client, err := NewClientContext(ctx, options...)
	if err != nil {
		return nil, err
	}
	if err = client.Connect(ctx); err != nil {
		return nil, joinClose(err, client.Close())
	}
	return client, nil
}

func (c *Client) notifyLocked() { close(c.changed); c.changed = make(chan struct{}) }
