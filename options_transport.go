// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

const defaultMaxMessageSize = 100 * 1024 * 1024

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
