// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

// EventsWithConcurrencyScopes is a reactor return value containing ordered,
// self-describing events and explicit concurrency checks. It is appended to the
// event log using AppendBatch, without grouping or reordering. An empty Events
// slice requires an effective protected scope. Inputs are borrowed until the
// invocation completes; do not mutate them concurrently.
type EventsWithConcurrencyScopes struct {
	// Events are appended in order without grouping by source.
	Events []Entry
	// Scopes enroll concurrency checks in the same atomic append.
	Scopes []LabeledScope
}
