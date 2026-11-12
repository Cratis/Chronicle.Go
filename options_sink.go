// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"fmt"

	"github.com/cratis/chronicle.go/graphs"
	"github.com/cratis/chronicle.go/readmodels"
)

// WithDefaultSinkType selects the kernel-owned materialized sink for models
// without an explicit readmodels.WithSink. The default is MongoDB; SQL and
// InMemory are also accepted. Last wins. NoSink and unknown types fail
// CaptureClient or NewClient with ErrInvalidConfiguration before preparation or
// connection work.
// Passive producers still select NoSink. The option covers every selected
// store registry, not individual namespaces, and borrows no database resources.
// Selecting a provider does not configure or verify its server-side backend.
func WithDefaultSinkType(kind readmodels.SinkType) ClientOption {
	return func(c *clientConfig) { c.defaultSinkType = kind }
}

func validateDefaultSinkType(kind readmodels.SinkType) error {
	switch kind {
	case readmodels.MongoDB, readmodels.SQL, readmodels.InMemory:
		return nil
	default:
		return fmt.Errorf("%w: default sink must be MongoDB, SQL or InMemory", ErrInvalidConfiguration)
	}
}

// WithDefaultGraphSinkType selects the kernel-owned sink that stores graph
// projections without an explicit graphs.Graph.UsingSink. The default is Neo4j;
// InMemoryGraph is also accepted. Last wins. Any other type fails CaptureClient or
// NewClient with ErrInvalidConfiguration before preparation or connection work.
// Selecting a provider does not configure or verify its server-side backend.
func WithDefaultGraphSinkType(kind graphs.SinkType) ClientOption {
	return func(c *clientConfig) { c.defaultGraphSinkType = kind }
}

func validateDefaultGraphSinkType(kind graphs.SinkType) error {
	switch kind {
	case graphs.Neo4j, graphs.InMemoryGraph:
		return nil
	default:
		return fmt.Errorf("%w: default graph sink must be Neo4j or InMemoryGraph", ErrInvalidConfiguration)
	}
}
