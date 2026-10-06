// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"crypto/tls"
	"log/slog"
	"time"

	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/clientoptions"
	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/cratis/chronicle.go/internal/outgoing"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/grpc"
)

// Token is a bearer credential and optional expiration. Formatting redacts its value.
type Token = connection.Token

// TokenSource supplies credentials, honors context cancellation and supports concurrent calls.
// Sources may also implement TokenInvalidator to discard rejected credentials.
type TokenSource = connection.TokenSource

// TokenInvalidator is optionally implemented by a TokenSource. Invalidate discards
// cached credentials after Unauthenticated; it must be concurrency-safe and nonblocking.
// The failed operation is not retried. The next operation requests a fresh token.
type TokenInvalidator interface {
	Invalidate()
}

// ClientOption configures construction only. Nil options are invalid. Scalar options
// are last-wins; authentication conflicts are errors. TLS validation cannot be
// disabled by a development option when an explicit TLS configuration is present.
type ClientOption func(*clientConfig)

type clientConfig struct {
	logger                                          *slog.Logger
	loggerSet                                       bool
	uri                                             string
	tls                                             *tls.Config
	tokenSource                                     TokenSource
	noAuth, development, skipCompatibility          bool
	connectTimeout                                  time.Duration
	keepAliveTimeout                                time.Duration
	keepAliveTimeoutSet, skipKeepAlive              bool
	maxSendMessageSize, maxReceiveMessageSize       int
	maxSendMessageSizeSet, maxReceiveMessageSizeSet bool
	registrationRetry                               RegistrationRetry
	connectedHooks, disconnectedHooks               []ConnectionHook
	loadBalancer                                    LoadBalancer
	loadBalancerSet                                 bool
	resolver                                        SRVResolver
	resolverSet                                     bool
	borrowed                                        grpc.ClientConnInterface
	tlsSet, tokenSet, borrowedSet, uriSet           bool
	validateEventTypes                              bool
	concurrency                                     eventsequences.ConcurrencyPolicy
	appendOriginResolver                            eventsequences.AppendOriginResolver
	outgoing                                        outgoing.Config
	rootCausation                                   *metadata.RootCausation
	naming                                          serialization.NamingPolicy
	defaultSinkType                                 readmodels.SinkType
	registry                                        *Registry
	stores                                          map[StoreName]*Registry
	reactorServices                                 reactorScopeFactory
	reactorServicesSet                              bool
	reactorRetryWait                                func(context.Context, time.Duration) error
}

// WithLogger selects the borrowed logger for SDK lifecycle and observer diagnostics.
// The last option wins; a final nil logger is invalid. Without this option,
// CaptureClient captures slog.Default (also used immediately by NewClientContext).
// Explicit artifact loggers override this fallback. Only logger and handler
// identities are captured; their underlying state and writers remain app-owned.
// In particular, the pristine slog default uses log.Default's current writer,
// which a later slog.SetDefault(custom) bridges to that custom handler. Supply an
// explicit logger with a stable handler/writer for strict destination isolation.
// Loggers/handlers are never closed, and Chronicle never calls slog.SetDefault.
// Handlers must support
// concurrent, synchronous calls and honor cancellation without blocking shutdown
// or reentering client lifecycle methods. Handler panics are contained, but
// arbitrary blocking handlers cannot be made harmless. SDK records contain only
// fixed operation/stage/category fields, not arbitrary errors or metadata.
// Handler-added fields, mutable handler state and context inspection remain the
// caller's responsibility. A handler running on SDK-owned work must not call
// Client.Close: joining that work from its own callback can deadlock.
// This option does not register a *slog.Logger service for artifact constructors.
func WithLogger(logger *slog.Logger) ClientOption {
	return func(c *clientConfig) { c.logger, c.loggerSet = logger, true }
}

// WithAppendOriginResolver selects a borrowed metadata-only callback for local
// immediate append attribution on every store/namespace/sequence of this client.
// Last wins; nil disables resolution and preserves OriginFrom(ctx). Configuration
// is frozen at NewClient; the callback must support synchronous concurrent calls.
// It runs outside SDK locks with the actual append context and cannot replace it.
// Errors/panics fail before RPCs or notifications with a payload-free
// eventsequences.AppendOriginResolutionError. SDK-owned unit commits bypass it.
func WithAppendOriginResolver(resolver eventsequences.AppendOriginResolver) ClientOption {
	return func(c *clientConfig) { c.appendOriginResolver = resolver }
}

// WithConnectionString selects the URI; it is validated by NewClient.
func WithConnectionString(value string) ClientOption {
	return func(c *clientConfig) { c.uri, c.uriSet = value, true }
}

// WithTLS snapshots a TLS configuration. Supply RootCAs and/or Certificates for PEM
// material loaded with crypto/x509 and tls.LoadX509KeyPair. Nil is invalid.
func WithTLS(config *tls.Config) ClientOption {
	return func(c *clientConfig) {
		c.tlsSet = true
		if config == nil {
			c.tls = nil
		} else {
			c.tls = config.Clone()
			if config.RootCAs != nil {
				c.tls.RootCAs = config.RootCAs.Clone()
			}
		}
	}
}

// WithTokenSource uses a caller-owned credential source; it is never closed by the client.
func WithTokenSource(source TokenSource) ClientOption {
	return func(c *clientConfig) { c.tokenSource, c.tokenSet = source, true }
}

// WithNoAuthentication suppresses OAuth and authorization metadata. Conflicting URI credentials are invalid.
func WithNoAuthentication() ClientOption { return func(c *clientConfig) { c.noAuth = true } }

// WithDevelopmentDefaults explicitly permits the kernel's self-signed development
// certificate. Never use this option in production. Explicit validating TLS wins.
func WithDevelopmentDefaults() ClientOption { return func(c *clientConfig) { c.development = true } }

// WithConnectTimeout sets the startup budget (default five seconds). It must be positive.
func WithConnectTimeout(timeout time.Duration) ClientOption {
	return func(c *clientConfig) { c.connectTimeout = timeout }
}

// WithSkipCompatibilityCheck disables structural preflight at the caller's risk.
func WithSkipCompatibilityCheck() ClientOption {
	return func(c *clientConfig) { c.skipCompatibility = true }
}

// WithGRPCConnection borrows a connection, which Close will not close. The caller
// owns transport security and must explicitly select WithConnectionString (the
// OAuth authority), WithTokenSource, or WithNoAuthentication. Use the latter when
// the channel already handles authentication. Client lifecycle checks still apply.
func WithGRPCConnection(conn *grpc.ClientConn) ClientOption {
	return func(c *clientConfig) { c.borrowed, c.borrowedSet = conn, true }
}

// BorrowConnection is the module-private option seam used by chronicletest.
// clientConfig is private; external callers still use WithGRPCConnection.
func (c *clientConfig) BorrowConnection(transport clientoptions.BorrowedTransport) {
	c.borrowed, c.borrowedSet = transport.Connection, true
}

// WithEventTypeGenerationValidation enables kernel schema and migration-chain
// validation. Like C#, validation defaults to disabled. When enabled, changed
// schemas for existing generations are rejected by the kernel. NewClient requires
// a complete adjacent migration chain for every current generation above one.
func WithEventTypeGenerationValidation(enabled bool) ClientOption {
	return func(c *clientConfig) { c.validateEventTypes = enabled }
}

// WithNamingPolicy selects property naming for every artifact in this client.
// The default preserves Go spelling. Explicit json tags always win. Last wins;
// invalid policies fail NewClient. Registry declarations are not mutated.
func WithNamingPolicy(policy serialization.NamingPolicy) ClientOption {
	return func(c *clientConfig) { c.naming = policy }
}

// WithRegistry snapshots registered events, read models and constraints at
// CaptureClient time (also used by NewClient); later mutations do not affect this
// client. Nil means an empty registry.
func WithRegistry(registry *Registry) ClientOption {
	return func(c *clientConfig) { c.registry = registry }
}

// WithRegistryForStore replaces the default registry for one logical store.
func WithRegistryForStore(name StoreName, registry *Registry) ClientOption {
	return func(c *clientConfig) {
		if c.stores == nil {
			c.stores = make(map[StoreName]*Registry)
		}
		c.stores[name] = registry
	}
}
