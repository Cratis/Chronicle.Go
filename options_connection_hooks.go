// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import "context"

// ConnectionEvent describes authenticated protocol readiness or its loss, not
// registration completion or observer attachment. Use WaitForRegistration/Ready
// for those barriers. Borrowed channels report logical sessions, not channel state.
type ConnectionEvent struct {
	// Generation is the same monotonically increasing counter as RegistrationOutcome.Generation.
	Generation uint64
	// ConnectionID is the logical session ID; empty with WithSkipKeepAlive.
	ConnectionID string
	// Address is the selected host:port; empty for WithGRPCConnection.
	Address string
	// Err is nil for Connected and the termination cause for Disconnected.
	// Close and Shutdown pair a delivered Connected with Disconnected(ErrClosed).
	Err error
}

// ConnectionHook observes a transition and cannot reject a connection. A single
// client-owned dispatcher orders events C(N), D(N), C(N+1), invoking one event's
// hooks concurrently and joining them before the next event. Hooks never run on
// the caller goroutine or under SDK locks. Supervision, reconnect, RPCs and Ready
// never wait for hooks; hooks may call Ready, EventStore and WaitForRegistration.
// If a generation ends before its Connected hooks start, both notifications are
// skipped (visible as a Generation gap); at most two events remain pending.
// Every delivered Connected gets exactly one Disconnected, including on Close.
// Connected's context ends with the generation or client; Disconnected uses the
// client lifetime context, which may already be canceled. Panics are contained,
// values discarded, and logged through WithLogger with operation client, stage
// connected/disconnected and category panic. Close/Shutdown join running hooks;
// hooks must not call Close/Shutdown themselves. CloseContext bounds that wait.
// WithSkipKeepAlive reports ConnectionID "" and Disconnected only on Close or
// Shutdown: no session stream is watched for loss in that mode.
type ConnectionHook func(ctx context.Context, event ConnectionEvent)

// WithOnConnected adds an application hook for authenticated protocol readiness
// (the same point Connect returns), not registration or attachment. Options
// accumulate in declaration order, without deduplication, and are frozen at
// CaptureClient for every owned or borrowed generation. Nil is rejected with
// ErrInvalidConfiguration before I/O. Execution follows ConnectionHook's contract.
func WithOnConnected(hook ConnectionHook) ClientOption {
	return func(c *clientConfig) { c.connectedHooks = append(c.connectedHooks, hook) }
}

// WithOnDisconnected adds an application hook for loss of a delivered ready
// generation, including Close/Shutdown (ErrClosed). Options accumulate in order
// and are frozen at CaptureClient, inherited by owned and borrowed generations.
// Nil is rejected with ErrInvalidConfiguration before I/O. Execution and
// cancellation follow ConnectionHook's contract, including coalesced generations.
func WithOnDisconnected(hook ConnectionHook) ClientOption {
	return func(c *clientConfig) { c.disconnectedHooks = append(c.disconnectedHooks, hook) }
}
