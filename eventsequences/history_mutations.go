// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/protobuf/proto"
)

// MutationOutcomeUnknownError means a history mutation or stream completion may
// have been accepted despite the error. Never blindly retry: revision can add
// another revision, and source redaction can affect newly appended events.
// Unwrap preserves transport, context and kernel envelope error identities.
type MutationOutcomeUnknownError struct {
	// Cause is the failure after dispatch, not proof that nothing changed.
	Cause error
}

func (e *MutationOutcomeUnknownError) Error() string {
	return fmt.Sprintf("chronicle: history mutation outcome unknown: %v", e.Cause)
}

// Unwrap returns the underlying failure.
func (e *MutationOutcomeUnknownError) Unwrap() error { return e.Cause }

// Redact requests irreversible replacement of one event's payload and revisions
// with an EventRedacted marker. Zero is valid; the three highest positions are
// reserved (Unavailable, Max and BeforeFirst) and are invalid targets.
// A nonblank reason is required. Actor and causation come from metadata in ctx.
// Nil error confirms request acceptance, NOT completion of the asynchronous
// mutation or observer replay. Authorize this operation at the consumer boundary.
// It never emits AppendOperations, participates in a unit of work, or retries.
func (s *Sequence) Redact(ctx context.Context, position events.SequenceNumber, reason events.RedactionReason) error {
	if err := validateMutation(ctx, position, reason); err != nil {
		return err
	}
	response, err := s.service.Redact(ctx, &sequences.RedactRequest{
		EventStore: string(s.store), Namespace: string(s.namespace), EventSequenceId: string(s.id),
		SequenceNumber: uint64(position), Reason: string(reason),
		Causation: causationContract(metadata.CausationChain(ctx)), CausedBy: identityContract(metadata.Identity(ctx)),
	})
	return mutationOutcome(response, err)
}

// RedactForEventSource requests irreversible redaction of all matching events for
// source in this sequence, across ALL stream/source types. With no eventTypes it
// targets every type; supplied IDs must be registered and select all generations.
// The kernel chooses matching events when it executes the asynchronous request,
// not at call time. This is not a prohibition on future appends. Reason and audit
// metadata follow Redact. Nil error means accepted, not completed. No retries,
// transaction staging, local append notifications, or client-side emulation occur.
func (s *Sequence) RedactForEventSource(ctx context.Context, source events.SourceID, reason events.RedactionReason, eventTypes ...events.TypeID) error {
	if err := validateMutation(ctx, 0, reason); err != nil {
		return err
	}
	if strings.TrimSpace(string(source)) == "" {
		return fmt.Errorf("%w: source is required", faults.ErrInvalidConfiguration)
	}
	ids := make([]string, len(eventTypes))
	for i, id := range eventTypes {
		if strings.TrimSpace(string(id)) == "" {
			return fmt.Errorf("%w: event type ID is required", faults.ErrInvalidConfiguration)
		}
		if _, ok := s.catalog.LookupID(id); !ok {
			return fmt.Errorf("%w: redaction event type %q", faults.ErrNotRegistered, id)
		}
		ids[i] = string(id)
	}
	response, err := s.service.RedactForEventSource(ctx, &sequences.RedactForEventSourceRequest{
		EventStore: string(s.store), Namespace: string(s.namespace), EventSequenceId: string(s.id),
		EventSourceId: string(source), Reason: string(reason), EventTypes: ids,
		Causation: causationContract(metadata.CausationChain(ctx)), CausedBy: identityContract(metadata.Identity(ctx)),
	})
	return mutationOutcome(response, err)
}

// Revise requests a new content revision of an existing event, retaining its
// original content and history. The registered replacement supplies the type ID,
// generation and shared serialization plan. The kernel requires the same type ID
// as the original; it may reject this asynchronously AFTER accepting the request.
// Revision is not schema migration or PII erasure. The pinned kernel does NOT
// apply append's PII/encryption processing to revisions or their system requests.
// Any protected schema in the selected catalog sharing the replacement's type ID,
// or failure to inspect that metadata, returns ErrUnsupported before serialization
// or dispatch (Chronicle#4525). This cannot detect undeclared sensitive data,
// audit metadata or unknown server-only classifications, nor repair old leaks.
// Zero is valid; the three highest positions are reserved and invalid targets.
// It has no reason field on the wire: record the non-sensitive reason in
// metadata.WithCausation. Nil error means
// accepted, not applied or replayed. Inputs must not be mutated during the call.
// No retries, local append notifications or unit-of-work staging occur.
func (s *Sequence) Revise(ctx context.Context, position events.SequenceNumber, replacement any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if position >= events.Unavailable-2 {
		return fmt.Errorf("%w: an event position is required", faults.ErrInvalidConfiguration)
	}
	descriptor, ok := s.catalog.Lookup(replacement)
	if !ok {
		return faults.ErrNotRegistered
	}
	// A historical unclassified generation must not bypass protection declared
	// on another generation of the same persisted identity. No target pre-read
	// can make the system request safe: the kernel persists its content first.
	for _, generation := range s.catalog.Descriptors() {
		if generation.Ref().ID == descriptor.Ref().ID {
			if err := validateRevisionSchema(generation.Schema()); err != nil {
				return err
			}
		}
	}
	content, err := descriptor.Marshal(replacement)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ref := descriptor.Ref()
	response, err := s.service.Revise(ctx, &sequences.ReviseRequest{
		EventStore: string(s.store), Namespace: string(s.namespace), EventSequenceId: string(s.id),
		SequenceNumber: uint64(position), EventType: &sequences.EventType{Id: string(ref.ID), Generation: uint32(ref.Generation)}, Content: string(content),
		Causation: causationContract(metadata.CausationChain(ctx)), CausedBy: identityContract(metadata.Identity(ctx)),
	})
	return mutationOutcome(response, err)
}

// Keep both protection and metadata-inspection failures stable and payload-free.
// Neither is an ambiguous mutation outcome: nothing has been dispatched.
var errProtectedRevisionUnsupported = fmt.Errorf("%w: protected revision is unsupported", faults.ErrUnsupported)

func validateRevisionSchema(schema string) error {
	roots, err := serialization.ProtectionRoots(schema)
	if err != nil || len(roots) != 0 {
		return errProtectedRevisionUnsupported
	}
	return nil
}

func validateMutation(ctx context.Context, position events.SequenceNumber, reason events.RedactionReason) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if position >= events.Unavailable-2 || strings.TrimSpace(string(reason)) == "" {
		return fmt.Errorf("%w: event position and nonblank redaction reason are required", faults.ErrInvalidConfiguration)
	}
	return nil
}

func mutationOutcome(response proto.Message, err error) error {
	if err != nil {
		var local *faults.BeforeDispatch
		if errors.As(err, &local) {
			return local.Cause
		}
		return &MutationOutcomeUnknownError{Cause: err}
	}
	if err := wire.CheckEnvelope(response); err != nil {
		var rejected *wire.EnvelopeError
		if errors.As(err, &rejected) && len(rejected.ExceptionMessages) == 0 {
			return err
		}
		return &MutationOutcomeUnknownError{Cause: err}
	}
	return nil
}
