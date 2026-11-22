// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

// WithDefaultSinkType returns detached metadata with a client default. Explicit
// WithSink selections and passive producer bindings win. NoSink enables immediate
// source-keyed projections, not materialized windows; projection preparation
// rejects unsupported correlation shapes. This registry-composition hook preserves
// declaration identity, schema and codec snapshots without I/O or callbacks.
func (d Descriptor) WithDefaultSinkType(kind SinkType) (Descriptor, error) {
	if d.definition == nil {
		return Descriptor{}, invalid("model required")
	}
	switch kind {
	case MongoDB, SQL, InMemory, NoSink:
	default:
		return Descriptor{}, invalid("default sink must be MongoDB, SQL, InMemory or None")
	}
	copy := *d.definition
	if !copy.config.sinkExplicit && copy.config.sink.Type != NoSink {
		copy.config.sink.Type = kind
	}
	return Descriptor{definition: &copy}, nil
}
