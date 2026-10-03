// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors

import (
	"slices"

	"github.com/cratis/chronicle.go/events"
)

// WithEventLog explicitly selects the local event log.
func WithEventLog() Option { return WithEventSequence(events.EventLog) }

// WithTags sets artifact labels only; labels never filter deliveries. Inputs are
// copied, the last option wins, and blank labels are rejected.
func WithTags(tags ...string) Option {
	labels := slices.Clone(tags)
	return func(c *configuration) { c.tags = labels }
}

// WithEventTagFilter selects events having any supplied tag (OR). Source type,
// stream type and tag categories combine with AND in the kernel. Empty clears
// the filter, blank tags are invalid, input is copied, and the last option wins.
func WithEventTagFilter(tags ...string) Option {
	filters := slices.Clone(tags)
	return func(c *configuration) { c.filterTags = filters }
}

// WithEventSourceType sets the observer filter and bare returned-event source
// type, like C# EventSourceTypeAttribute. Empty means all inputs and default output.
func WithEventSourceType(value events.SourceType) Option {
	return func(c *configuration) { c.sourceType = value }
}

// WithEventStreamType sets the observer filter and bare returned-event stream
// type, like C# EventStreamTypeAttribute. All is the default; blank is invalid.
func WithEventStreamType(value events.StreamType) Option {
	return func(c *configuration) { c.streamType = value }
}

// WithEventStreamID sets bare returned-event metadata only, never a filter.
// EventStreamIDProvider takes precedence. Empty uses the append default.
func WithEventStreamID(value events.StreamID) Option {
	return func(c *configuration) { c.streamID = value }
}
