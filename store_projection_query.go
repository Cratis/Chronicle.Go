// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/projections"
)

// QueryProjection executes Projection Declaration Language in this namespace
// without registering it. Omitting sequence selects event-log; at most one
// nonblank sequence is accepted. A declaration may omit its read-model target
// for schema inference. Kernel 19.29.4 drops inferred properties
// (Chronicle#4539, fixed in 19.32.3); use an explicitly registered target schema
// on that kernel.
// The caller controls cancellation/deadlines; no retries or
// Go callbacks run. Invalid PDL returns *projections.QueryError.
func (s *EventStore) QueryProjection(ctx context.Context, declaration string, sequence ...events.SequenceID) (projections.QueryResult, error) {
	if strings.TrimSpace(declaration) == "" || len(sequence) > 1 {
		return projections.QueryResult{}, ErrInvalidConfiguration
	}
	selected := events.EventLog
	if len(sequence) > 0 {
		selected = sequence[0]
	}
	if strings.TrimSpace(string(selected)) == "" {
		return projections.QueryResult{}, ErrInvalidConfiguration
	}
	transport := &clientTransport{client: s.client, store: s}
	response, err := contracts.NewProjectionsClient(transport).Preview(ctx, &contracts.PreviewProjectionRequest{EventStore: string(s.name), Namespace: string(s.namespace), EventSequenceId: string(selected), Declaration: declaration})
	if err != nil {
		return projections.QueryResult{}, wire.RPCError(err)
	}
	if response == nil || (response.Value0 == nil) == (response.Value1 == nil) {
		return projections.QueryResult{}, ErrProtocol
	}
	if response.Value1 != nil {
		if len(response.Value1.Errors) == 0 {
			return projections.QueryResult{}, ErrProtocol
		}
		failure := &projections.QueryError{}
		for _, detail := range response.Value1.Errors {
			if detail == nil {
				return projections.QueryResult{}, ErrProtocol
			}
			failure.Errors = append(failure.Errors, projections.SyntaxError{Message: detail.Message, Line: detail.Line, Column: detail.Column})
		}
		return projections.QueryResult{}, failure
	}
	result := projections.QueryResult{ReadModelEntries: slices.Clone(response.Value0.ReadModelEntries)}
	for _, entry := range result.ReadModelEntries {
		if !json.Valid([]byte(entry)) {
			return projections.QueryResult{}, ErrProtocol
		}
	}
	if response.Value0.ReadModel != nil {
		result.Schema = response.Value0.ReadModel.Schema
		if !json.Valid([]byte(result.Schema)) {
			return projections.QueryResult{}, ErrProtocol
		}
	}
	return result, nil
}

// PreviewProjection is the historical spelling of QueryProjection. It uses the
// same kernel RPC and returns the same owned result and error contracts.
func (s *EventStore) PreviewProjection(ctx context.Context, declaration string, sequence ...events.SequenceID) (projections.Preview, error) {
	return s.QueryProjection(ctx, declaration, sequence...)
}
