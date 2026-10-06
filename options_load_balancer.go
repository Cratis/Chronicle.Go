// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"io"
)

// LoadBalancer selects a candidate once per connection attempt, including a
// single-address URI and every reconnect. Calls are serialized per client, outside
// SDK locks. Next receives a fresh copy in URI order, or SRV priority/weight order
// after re-resolution. It may mutate or retain that copy, but must return an
// original candidate. The attempt context is bounded by WithConnectTimeout and
// canceled by Close; implementations must honor cancellation and must not call
// Close/Shutdown from Next. Errors, panics and non-candidates fail before dialing
// or OAuth and become LoadBalancerError; reconnect retries with normal backoff.
// The selected address is also the OAuth authority. A shared balancer must support
// concurrent calls from different clients.
type LoadBalancer interface {
	// Next selects one of candidates using the attempt's cancellation budget.
	Next(ctx context.Context, candidates []ServerAddress) (ServerAddress, error)
}

// WithLoadBalancer borrows an explicit strategy, never closing it even if it
// implements io.Closer. Last wins. Nil (including typed nil), a loadBalancer=
// URI option, or WithGRPCConnection is rejected with ErrInvalidConfiguration
// before I/O. Unlike C#'s explicit-option precedence, conflicting URI selection
// is rejected rather than silently overridden. Without this option the URI
// strategy applies, defaulting to least-connections.
func WithLoadBalancer(balancer LoadBalancer) ClientOption {
	return func(c *clientConfig) { c.loadBalancer, c.loadBalancerSet = balancer, true }
}

// LoadBalancerError reports a failed selection. It is always transient for the
// connection supervisor, even when its cause is an authentication status.
type LoadBalancerError struct {
	// Err is the application failure, available through Unwrap. Panic values are
	// discarded; errors for panics and non-candidates contain no application data.
	Err error
}

// Error returns a fixed diagnostic without formatting the cause.
func (*LoadBalancerError) Error() string { return "chronicle: load balancer selection failed" }

// Unwrap returns the selection failure for errors.Is and errors.As.
func (e *LoadBalancerError) Unwrap() error { return e.Err }

// Format redacts the application cause for every fmt verb.
func (e *LoadBalancerError) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, e.Error()) }
