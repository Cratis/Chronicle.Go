// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import "github.com/cratis/chronicle.go/eventsequences"

// WithCheckFirstAppendIntoAScope protects empty resolved scopes against competing
// first appends. It defaults to false, like C#. Last option wins. Explicit
// NoCheck remains unchecked. Applies to all sequences and unit-of-work commits.
func WithCheckFirstAppendIntoAScope(enabled bool) ClientOption {
	return func(c *clientConfig) { c.concurrency.CheckFirstAppendIntoAScope = enabled }
}

// WithDefaultConcurrencyStrategy replaces automatic scope selection. Nil restores
// the default optimistic strategy. Last option wins. Implementations are borrowed
// for the client's lifetime and must support concurrent calls. Explicit append
// scopes bypass the strategy; Resolve expectations still use the client's
// first-append policy. No additional tail read is made for explicit expectations.
func WithDefaultConcurrencyStrategy(strategy eventsequences.ConcurrencyScopeStrategy) ClientOption {
	return func(c *clientConfig) {
		if nilValue(strategy) {
			c.concurrency.Strategy = nil
		} else {
			c.concurrency.Strategy = strategy
		}
	}
}

// ConcurrencyPolicy supplies the owning client's frozen policy to sequence handles.
func (t *clientTransport) ConcurrencyPolicy() eventsequences.ConcurrencyPolicy {
	return t.client.config.concurrency
}
