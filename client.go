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

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/connection"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Client owns a shared RPC transport and immutable registry snapshots. Construct
// with NewClient or Dial; the zero value is not usable. It is safe for concurrent
// use. Unary calls honor caller deadlines (no blanket per-call timeout).
type Client struct {
	mu              sync.Mutex
	closed          bool
	closeOnce       sync.Once
	closeError      error
	life            context.Context
	cancel          context.CancelFunc
	work            sync.WaitGroup
	raw             *grpc.ClientConn
	transport       *clientTransport
	owned           bool
	oauth           *connection.OAuth
	tokens          TokenSource
	config          clientConfig
	catalog         *events.Catalog
	catalogs        map[StoreName]*events.Catalog
	stores          map[storeKey]*storeAttempt
	connected       bool
	attempt         *connectAttempt
	connectionError error
}

// String describes the client without revealing endpoints, credentials or registries.
func (c *Client) String() string { return "Chronicle client" }

// GoString is the credential-safe representation for fmt's %#v format.
func (c *Client) GoString() string { return c.String() }

// NewClient validates configuration and freezes registries without network I/O.
// TLS validation defaults to enabled; omitted credentials use Chronicle's dev
// client credentials. Production callers should provide explicit credentials.
func NewClient(options ...ClientOption) (*Client, error) {
	config := clientConfig{uri: "chronicle://localhost:35000", connectTimeout: 5 * time.Second}
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
	c := &Client{config: config, catalog: snapshot(config.registry), catalogs: make(map[StoreName]*events.Catalog), stores: make(map[storeKey]*storeAttempt)}
	for name, registry := range config.stores {
		if strings.TrimSpace(string(name)) == "" {
			return nil, fmt.Errorf("%w: empty registry store name", ErrInvalidConfiguration)
		}
		c.catalogs[name] = snapshot(registry)
	}
	c.tokens = config.tokenSource
	if c.tokens == nil && !config.noAuth && !uri.noAuth {
		c.oauth = connection.NewOAuth(uri.addresses[0].String(), uri.clientID, uri.secret, tlsConfig)
		c.tokens = c.oauth
	}
	c.raw, c.owned = config.borrowed, config.borrowed == nil
	if c.owned {
		c.raw, err = grpc.NewClient("dns:///"+uri.addresses[0].String(), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
			grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(100*1024*1024), grpc.MaxCallSendMsgSize(100*1024*1024)))
		if err != nil {
			if c.oauth != nil {
				c.oauth.Close()
			}
			return nil, fmt.Errorf("chronicle: create channel: %w", err)
		}
	}
	c.config.skipCompatibility = config.skipCompatibility || uri.skipCompatibility
	c.life, c.cancel = context.WithCancel(context.Background())
	c.transport = &clientTransport{client: c}
	return c, nil
}

func validateConfig(config clientConfig) (ConnectionString, *tls.Config, error) {
	if (config.tlsSet && config.tls == nil) || (config.borrowedSet && config.borrowed == nil) || (config.tokenSet && nilTokenSource(config.tokenSource)) {
		return ConnectionString{}, nil, fmt.Errorf("%w: nil TLS, transport or token source", ErrInvalidConfiguration)
	}
	uri, err := ParseConnectionString(config.uri)
	if err != nil {
		return uri, nil, err
	}
	if uri.srv || len(uri.addresses) != 1 || uri.apiKey != "" || len(uri.unsupported) > 0 {
		return uri, nil, fmt.Errorf("%w: SRV, multi-host, API keys and certificate URI options require a later client slice; use WithTLS for PEM material", ErrUnsupported)
	}
	if config.connectTimeout <= 0 {
		return uri, nil, fmt.Errorf("%w: connect timeout must be positive", ErrInvalidConfiguration)
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
	} // Explicit development opt-out only.
	return uri, tlsConfig, nil
}

func nilTokenSource(source TokenSource) bool {
	if source == nil {
		return true
	}
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Func, reflect.Interface, reflect.Slice, reflect.Chan:
		return value.IsNil()
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

// Close cancels admitted work, joins the keep-alive worker and closes owned
// transports. Repeated calls return the same result. Borrowed connections remain
// open. External TokenSource implementations must honor cancellation to allow joining.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.cancel()
		c.mu.Unlock()
		if c.owned {
			c.closeError = c.raw.Close()
		}
		c.work.Wait()
		if c.oauth != nil {
			c.oauth.Close()
		}
	})
	return c.closeError
}
