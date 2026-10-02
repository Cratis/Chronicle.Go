// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"crypto/tls"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
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
}

// String describes the client without revealing endpoints or credentials.
func (c *Client) String() string { return "Chronicle client" }

// GoString is the credential-safe representation for fmt's %#v format.
func (c *Client) GoString() string { return c.String() }

// NewClient validates and freezes configuration without network I/O. TLS validates
// by default. Omitted credentials select Chronicle's public development credentials.
func NewClient(options ...ClientOption) (*Client, error) {
	config := clientConfig{uri: "chronicle://localhost:35000", connectTimeout: 5 * time.Second,
		keepAliveTimeout: 5 * time.Second, registrationRetry: RegistrationRetry{MaxAttempts: 5, InitialDelay: 2 * time.Second, MaximumDelay: 30 * time.Second, AttemptTimeout: 30 * time.Second}}
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
	frozen, err := freezeRegistry(config.registry, config.naming)
	if err != nil {
		return nil, err
	}
	c := &Client{config: config, uri: uri, tls: tlsConfig, catalog: frozen.events, constraints: frozen.constraints,
		catalogs: make(map[StoreName]*events.Catalog), storeConstraints: make(map[StoreName][]constraints.Definition), stores: make(map[storeKey]*EventStore),
		readModelCatalog: frozen.models, readModelCatalogs: make(map[StoreName]*readmodels.Catalog),
		projections: frozen.projections, storeProjections: make(map[StoreName][]projections.Definition),
		changed: make(chan struct{}), closeDone: make(chan struct{})}
	for name, registry := range config.stores {
		if strings.TrimSpace(string(name)) == "" {
			return nil, fmt.Errorf("%w: empty registry store name", ErrInvalidConfiguration)
		}
		frozen, err := freezeRegistry(registry, config.naming)
		if err != nil {
			return nil, err
		}
		c.catalogs[name], c.storeConstraints[name] = frozen.events, frozen.constraints
		c.readModelCatalogs[name], c.storeProjections[name] = frozen.models, frozen.projections
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
	if (config.tlsSet && config.tls == nil) || (config.borrowedSet && config.borrowed == nil) ||
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
	client, err := NewClient(options...)
	if err != nil {
		return nil, err
	}
	if err = client.Connect(ctx); err != nil {
		return nil, joinClose(err, client.Close())
	}
	return client, nil
}

func (c *Client) notifyLocked() { close(c.changed); c.changed = make(chan struct{}) }
