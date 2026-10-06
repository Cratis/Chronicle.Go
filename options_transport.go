// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import "google.golang.org/grpc/stats"

const defaultMaxMessageSize = 100 * 1024 * 1024

// WithGRPCStatsHandler borrows a gRPC stats handler for every owned connection,
// including replacement generations on reconnect. The last option wins; a final
// nil (including a typed nil) or combination with WithGRPCConnection is invalid.
// Chronicle never closes the handler or its providers/exporters.
//
// This is restricted configuration for trusted callbacks, not enforced safety
// for arbitrary code. Callbacks must support concurrent use, return promptly,
// preserve cancellation, deadlines and required context values in TagRPC/TagConn,
// and must not reenter blocking client lifecycle methods such as Close. A handler
// can still block or return an unsuitable context. It does not authorize retries
// or change write outcome guarantees. No arbitrary dial options or interceptors
// are exposed by this option.
func WithGRPCStatsHandler(handler stats.Handler) ClientOption {
	return func(c *clientConfig) { c.grpcStatsHandler, c.grpcStatsHandlerSet = handler, true }
}

// WithMaxSendMessageSize sets the maximum serialized gRPC message size in bytes
// for every RPC. Valid values are 1..2147483647; NewClient validates the final
// value and the last option wins. Owned channels default to 100 MiB. Borrowed
// channels retain their owner's defaults unless explicitly overridden per RPC;
// the underlying channel is never modified.
func WithMaxSendMessageSize(bytes int) ClientOption {
	return func(c *clientConfig) { c.maxSendMessageSize, c.maxSendMessageSizeSet = bytes, true }
}

// WithMaxReceiveMessageSize sets the maximum serialized gRPC message size in
// bytes for every RPC. Valid values are 1..2147483647; NewClient validates the
// final value and the last option wins. Owned channels default to 100 MiB.
// Borrowed channels retain their owner's defaults unless explicitly overridden
// per RPC; the underlying channel is never modified.
func WithMaxReceiveMessageSize(bytes int) ClientOption {
	return func(c *clientConfig) { c.maxReceiveMessageSize, c.maxReceiveMessageSizeSet = bytes, true }
}

// WithSkipKeepAlive omits the application Connect session, heartbeat
// acknowledgments and watchdog. HTTP/2 keepalive remains 60s/30s. Startup still
// checks compatibility unless explicitly skipped, and always probes the protected
// GetConnectedClients RPC before readiness. Probe errors are returned, not ignored.
// No logical session is registered. Configured reactors, reducers (including
// passive reducers), read-model reactors and explicit WithKeepAliveTimeout are
// therefore invalid. grpc-go may reconnect its transport, but this does not
// guarantee registration replay after kernel restarts without the application
// session. Prefer this option for short-lived clients, not observer runtimes.
func WithSkipKeepAlive() ClientOption {
	return func(c *clientConfig) { c.skipKeepAlive = true }
}
