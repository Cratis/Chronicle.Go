// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/eventsequences"
)

func TestAppendFailureDispositionAndErrorIdentity(t *testing.T) {
	for _, method := range []string{"Append", "AppendMany", "AppendManyForEventSources"} {
		t.Run(method, func(t *testing.T) {
			for _, test := range []struct {
				name        string
				constraints bool
				concurrency bool
				diagnostics []string
			}{
				{name: "errors only", diagnostics: []string{"commit acknowledgment lost", "FutureCode"}},
				{name: "constraints", constraints: true},
				{name: "constraints with errors", constraints: true, diagnostics: []string{"FutureCode"}},
				{name: "concurrency", concurrency: true},
				{name: "concurrency with errors", concurrency: true, diagnostics: []string{"FutureCode"}},
				{name: "both violations", constraints: true, concurrency: true},
				{name: "both violations with errors", constraints: true, concurrency: true, diagnostics: []string{"FutureCode"}},
			} {
				t.Run(test.name, func(t *testing.T) {
					var constraints []*sequences.ConstraintViolation
					var concurrency []*sequences.ConcurrencyViolation
					if test.constraints {
						constraints = []*sequences.ConstraintViolation{{ConstraintName: "unique", Message: "taken"}}
					}
					if test.concurrency {
						concurrency = []*sequences.ConcurrencyViolation{{EventSourceId: "A", ExpectedSequenceNumber: 1, ActualSequenceNumber: 2}}
					}
					sequence, calls := sequenceFixture(t, map[string]rpcHandler{method: func(context.Context, any) (any, error) {
						if method == "Append" {
							response := &sequences.AppendResponse{HasConstraintViolations: test.constraints, HasConcurrencyViolations: test.concurrency, HasErrors: len(test.diagnostics) > 0, ConstraintViolations: constraints, Errors: test.diagnostics}
							if test.concurrency {
								response.ConcurrencyViolation = concurrency[0]
							}
							return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: response}, nil
						}
						return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{HasConstraintViolations: test.constraints, HasConcurrencyViolations: test.concurrency, HasErrors: len(test.diagnostics) > 0, ConstraintViolations: constraints, ConcurrencyViolations: concurrency, Errors: test.diagnostics}}, nil
					}})
					var disposition eventsequences.Disposition
					var diagnostics []eventsequences.AppendError
					var operationErr, resultErr error
					if method == "Append" {
						result, err := sequence.Append(testContext(t), "A", opened{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
						disposition, diagnostics, operationErr, resultErr = result.Disposition, result.Errors, err, result.Err()
						if result.Position != nil || result.Target.First != nil {
							t.Fatal("failed append returned committed coordinates", result)
						}
					} else {
						var result eventsequences.BatchResult
						if method == "AppendMany" {
							result, operationErr = sequence.AppendMany(testContext(t), "A", []any{opened{}}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
						} else {
							result, operationErr = sequence.AppendBatch(testContext(t), []eventsequences.Entry{{Source: "A", Event: opened{}}}, eventsequences.WithScopes(eventsequences.LabeledScope{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck()}}))
						}
						disposition, diagnostics, resultErr = result.Disposition, result.Errors, result.Err()
						if len(result.Positions) != 0 || result.Target.First != nil {
							t.Fatal("failed batch returned committed coordinates", result)
						}
					}
					want := eventsequences.Unknown
					if test.constraints || test.concurrency {
						want = eventsequences.Rejected
					}
					if disposition != want || calls.Load() != 1 || (operationErr != nil) != (want == eventsequences.Unknown) {
						t.Fatalf("disposition=%v want=%v error=%v calls=%d", disposition, want, operationErr, calls.Load())
					}
					var wantDiagnostics []eventsequences.AppendError
					for _, diagnostic := range test.diagnostics {
						wantDiagnostics = append(wantDiagnostics, eventsequences.AppendError(diagnostic))
						if !errors.Is(resultErr, eventsequences.AppendError(diagnostic)) || !strings.Contains(resultErr.Error(), diagnostic) {
							t.Fatalf("lost diagnostic %q: %v", diagnostic, resultErr)
						}
						if want == eventsequences.Unknown && !errors.Is(operationErr, eventsequences.AppendError(diagnostic)) {
							t.Fatalf("operation error lost diagnostic %q: %v", diagnostic, operationErr)
						}
					}
					if !reflect.DeepEqual(diagnostics, wantDiagnostics) {
						t.Fatalf("diagnostics=%v want=%v", diagnostics, wantDiagnostics)
					}
					var unknown *eventsequences.OutcomeUnknownError
					var constraintErr *eventsequences.ConstraintError
					var concurrencyErr *eventsequences.ConcurrencyError
					if errors.As(resultErr, &constraintErr) != test.constraints || errors.As(resultErr, &concurrencyErr) != test.concurrency || errors.As(resultErr, &unknown) != (want == eventsequences.Unknown) {
						t.Fatalf("unexpected result error identities: %v", resultErr)
					}
					if want == eventsequences.Unknown && !errors.As(operationErr, &unknown) {
						t.Fatalf("operation error is not OutcomeUnknownError: %v", operationErr)
					}
				})
			}
		})
	}
}
