// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections

import "github.com/cratis/chronicle.go/internal/faults"

// QueryResult contains owned JSON entries from a kernel-executed PDL query.
// Querying does not register a projection or persist the resulting models.
type QueryResult struct {
	ReadModelEntries []string
	// Schema is the inferred or declared JSON schema, when returned by the kernel.
	Schema string
}

// Preview is the historical name of QueryResult; both use the kernel Preview RPC.
type Preview = QueryResult

// SyntaxError locates a PDL declaration error returned by the kernel. Message may
// include declaration contents and should not be logged without redaction.
type SyntaxError struct {
	Message      string
	Line, Column int32
}

// QueryError retains kernel parsing diagnostics without echoing declaration
// contents from Error. Inspect Errors explicitly; errors.Is matches invalid config.
type QueryError struct{ Errors []SyntaxError }

func (e *QueryError) Error() string {
	return "chronicle: unable to query projection: invalid declaration"
}

// Unwrap exposes the stable invalid-configuration category.
func (e *QueryError) Unwrap() error { return faults.ErrInvalidConfiguration }
