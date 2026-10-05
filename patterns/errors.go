// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package patterns

import (
	"context"
	"fmt"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Stable error categories are shared with Chronicle's other facades.
var (
	ErrInvalidConfiguration = faults.ErrInvalidConfiguration
	ErrProtocol             = faults.ErrProtocol
	ErrUnsupported          = faults.ErrUnsupported
)

// ValidationResult preserves server diagnostics, including unknown numeric severity.
// C# considers every validation diagnostic a failure, even Information/Warning.
type ValidationResult struct {
	// Severity is the wire numeric value, without normalization of future values.
	Severity int32
	// Message is deliberately inspectable server text, potentially sensitive.
	Message string
	// Members is a caller-owned slice of affected field names.
	Members []string
}

// EnvelopeError reports failed authorization, validation or execution despite
// gRPC OK. Diagnostic fields may contain personal data; Error never formats them.
type EnvelopeError struct {
	// CorrelationID identifies this response, not the input scope.
	CorrelationID metadata.CorrelationID
	// Authorized preserves the wire authorization decision.
	Authorized bool
	// ValidationResults preserves every diagnostic with independent member slices.
	ValidationResults []ValidationResult
	// ExceptionMessages preserves server text for deliberate inspection only.
	ExceptionMessages []string
}

func (e *EnvelopeError) Error() string { return "chronicle: patterns query envelope failed" }

// CallError wraps a transport or pre-dispatch failure without formatting its
// possibly sensitive text. Unwrap preserves the original error graph, not a
// flattened copy; Is additionally recognizes gRPC cancellation/deadline status.
type CallError struct {
	// Code preserves the gRPC status code (Unknown for ordinary local errors).
	Code codes.Code
	// Cause is available for deliberate diagnostics via errors.As/Unwrap.
	Cause error
}

func (e *CallError) Error() string {
	return fmt.Sprintf("chronicle: patterns query failed (%s)", e.Code)
}

// Unwrap exposes the original cause once, preserving joins and ordinary wrappers.
func (e *CallError) Unwrap() error { return e.Cause }

// Is adds context identities without discarding the underlying cause graph.
func (e *CallError) Is(target error) bool {
	return (target == context.Canceled && e.Code == codes.Canceled) ||
		(target == context.DeadlineExceeded && e.Code == codes.DeadlineExceeded)
}

// UnsupportedError reports an unavailable patterns RPC. No inference fallback
// runs. Cause retains the transport status for deliberate inspection.
type UnsupportedError struct{ Cause error }

func (e *UnsupportedError) Error() string { return "chronicle: patterns RPC unsupported by server" }

// Unwrap preserves the original transport error graph.
func (e *UnsupportedError) Unwrap() error { return e.Cause }

// Is recognizes the SDK's stable unsupported category.
func (e *UnsupportedError) Is(target error) bool { return target == ErrUnsupported }

func callError(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.Unimplemented {
		return &UnsupportedError{Cause: err}
	}
	return &CallError{Code: status.Code(err), Cause: err}
}

func invalid(message string) error {
	return fmt.Errorf("%w: patterns %s", ErrInvalidConfiguration, message)
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return invalid("context required")
	}
	return ctx.Err()
}

func envelope(authorized bool, correlation metadata.CorrelationID, validation []ValidationResult, exceptions []string) error {
	if authorized && len(validation) == 0 && len(exceptions) == 0 {
		return nil
	}
	return &EnvelopeError{CorrelationID: correlation, Authorized: authorized, ValidationResults: validation, ExceptionMessages: exceptions}
}
