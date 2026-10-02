// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events

const (
	// UnspecifiedSequence is Chronicle's unspecified sequence identity.
	UnspecifiedSequence SequenceID = "[unspecified]"
	// SystemSequence contains system events.
	SystemSequence SequenceID = "system"
	// Outbox is the default outbox sequence.
	Outbox SequenceID = "outbox"
	// Inbox is the virtual sequence representing all inboxes.
	Inbox SequenceID = "inbox"
	// InboxPrefix prefixes per-source inbox sequence IDs.
	InboxPrefix = "inbox-"
	// UnspecifiedSourceType does not narrow concurrency or read scopes.
	UnspecifiedSourceType SourceType = ""
)

// IsEventLog reports whether id identifies the primary event sequence.
func (id SequenceID) IsEventLog() bool { return id == EventLog }
