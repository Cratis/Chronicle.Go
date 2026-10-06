// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"reflect"
	"sync"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/seeding"
)

// Client owns a generation supervisor and frozen registries. Construct with
// NewClient, Dial or CaptureClient; the zero value is not usable. Do not copy it.
// It is safe for concurrent use; a captured identity requires successful Prepare.
type Client struct {
	mu               sync.Mutex
	closed           bool
	preparation      preparationState
	preparationError error
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
	hooks            connectionHooks
	catalog          *events.Catalog
	catalogs         map[StoreName]*events.Catalog
	constraints      []constraints.Definition
	storeConstraints map[StoreName][]constraints.Definition
	stores           map[storeKey]*EventStore // Lookup and automatic registration membership only.
	storeOwners      map[storeKey]*storeOwner // Resource ownership, retained until client shutdown.
	definitions      map[StoreName]*definitionCoordinator
	current          *generation
	supervisor       *supervision
	changed          chan struct{}
	nextGeneration   uint64
	connectionError  error

	// Retain finalized preparation independently of callback-bearing captures.
	registryOutput       *registryPreparationOutput
	storeRegistryOutputs map[StoreName]*registryPreparationOutput

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
	if nilValue(ctx) {
		return nil, fmt.Errorf("%w: nil preparation context", ErrInvalidConfiguration)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p, err := CaptureClient(options...)
	if err != nil {
		return nil, err
	}
	client, err := p.Prepare(ctx, nil)
	if err != nil {
		return nil, joinClose(err, p.Client().Close())
	}
	return client, nil
}

func validateConfig(config clientConfig) (ConnectionString, *tls.Config, error) {
	for _, hooks := range [][]ConnectionHook{config.connectedHooks, config.disconnectedHooks} {
		for _, hook := range hooks {
			if hook == nil {
				return ConnectionString{}, nil, fmt.Errorf("%w: nil connection hook", ErrInvalidConfiguration)
			}
		}
	}
	if config.loggerSet && config.logger == nil {
		return ConnectionString{}, nil, fmt.Errorf("%w: nil logger", ErrInvalidConfiguration)
	}
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
	if config.grpcStatsHandlerSet && nilValue(config.grpcStatsHandler) {
		return ConnectionString{}, nil, fmt.Errorf("%w: nil gRPC stats handler", ErrInvalidConfiguration)
	}
	if config.grpcStatsHandlerSet && config.borrowedSet {
		return ConnectionString{}, nil, fmt.Errorf("%w: gRPC stats handler conflicts with borrowed connection", ErrInvalidConfiguration)
	}
	if config.borrowed != nil && !config.uriSet && !config.tokenSet && !config.noAuth {
		return ConnectionString{}, nil, fmt.Errorf("%w: borrowed connection requires explicit authentication", ErrInvalidConfiguration)
	}
	uri, err := ParseConnectionString(config.uri)
	if err != nil {
		return uri, nil, err
	}
	if err := validateLoadBalancer(config, uri); err != nil {
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
