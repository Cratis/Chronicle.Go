// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"time"

	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/cratis/chronicle.go/internal/registration"
)

// SRVResolver is a cancelable DNS service-record resolver. The caller owns it.
type SRVResolver = connection.SRVResolver

// WithSRVResolver replaces system DNS for chronicle+srv connections. Nil is invalid.
func WithSRVResolver(resolver SRVResolver) ClientOption {
	return func(c *clientConfig) { c.resolver, c.resolverSet = resolver, true }
}

// WithKeepAliveTimeout sets the maximum silence between kernel heartbeats, default
// five seconds. It must be positive. HTTP/2 pings additionally use 60s/30s like C#.
func WithKeepAliveTimeout(timeout time.Duration) ClientOption {
	return func(c *clientConfig) { c.keepAliveTimeout, c.keepAliveTimeoutSet = timeout, true }
}

// RegistrationRetry configures bounded, idempotent registration passes. Defaults
// are five attempts, 2s initial/30s maximum jittered delay and 30s per attempt.
// After a failed transient pass, one owned worker retries after MaximumDelay.
// Deterministic validation/authorization failures require a new explicit wait.
type RegistrationRetry = registration.Policy

// WithRegistrationRetry replaces all registration retry settings. Zero fields are invalid.
func WithRegistrationRetry(policy RegistrationRetry) ClientOption {
	return func(c *clientConfig) { c.registrationRetry = policy }
}
