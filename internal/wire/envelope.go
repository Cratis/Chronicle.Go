// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package wire

import (
	"fmt"

	"github.com/cratis/chronicle.go/internal/faults"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// ValidationResult preserves kernel field validation diagnostics.
type ValidationResult struct {
	// Severity preserves the wire severity, including future values.
	Severity int32
	// Message is the kernel's diagnostic text.
	Message string
	// Members contains the affected field paths.
	Members []string
}

// EnvelopeError reports authorization, validation or execution failure despite OK gRPC status.
type EnvelopeError struct {
	// Authorized says whether the kernel authorized the operation.
	Authorized bool
	// AuthorizationFailureReason preserves the kernel's reason, if supplied.
	AuthorizationFailureReason string
	// ValidationResults preserves every validation diagnostic.
	ValidationResults []ValidationResult
	// ExceptionMessages preserves kernel exception messages (not stack traces).
	ExceptionMessages []string
}

func (e *EnvelopeError) Error() string { return "chronicle: kernel command/query envelope failed" }

// CheckEnvelope uses protobuf reflection because each upstream package defines
// a distinct envelope type with the same field contract. It never treats absence as success.
func CheckEnvelope(value proto.Message) error {
	if value == nil || !value.ProtoReflect().IsValid() {
		return faults.ErrProtocol
	}
	m := value.ProtoReflect()
	fields := m.Descriptor().Fields()
	authorized := fields.ByName("IsAuthorized")
	validation, exceptions := fields.ByName("ValidationResults"), fields.ByName("ExceptionMessages")
	if authorized == nil || validation == nil || exceptions == nil {
		return fmt.Errorf("%w: expected command/query envelope", faults.ErrProtocol)
	}
	err := &EnvelopeError{Authorized: m.Get(authorized).Bool()}
	if reason := fields.ByName("AuthorizationFailureReason"); reason != nil {
		err.AuthorizationFailureReason = m.Get(reason).String()
	}
	failure := err.readValidation(m.Get(validation).List(), !err.Authorized)
	err.readExceptions(m.Get(exceptions).List())
	if failure || len(err.ExceptionMessages) != 0 {
		return err
	}
	return nil
}

func (e *EnvelopeError) readExceptions(list protoreflect.List) {
	for i := 0; i < list.Len(); i++ {
		e.ExceptionMessages = append(e.ExceptionMessages, list.Get(i).String())
	}
}

// readValidation records every result and returns the seeded failure state, now true on any fatal result.
func (e *EnvelopeError) readValidation(list protoreflect.List, failure bool) bool {
	for i := 0; i < list.Len(); i++ {
		result := decodeValidation(list.Get(i).Message())
		e.ValidationResults = append(e.ValidationResults, result)
		failure = failure || result.isFatal()
	}
	return failure
}

func (r ValidationResult) isFatal() bool {
	// Only Information (1) and Warning (2) are non-fatal across the envelopes.
	return r.Severity != 1 && r.Severity != 2
}

func decodeValidation(item protoreflect.Message) ValidationResult {
	f := item.Descriptor().Fields()
	result := ValidationResult{Severity: int32(item.Get(f.ByName("Severity")).Enum()), Message: item.Get(f.ByName("Message")).String()}
	members := item.Get(f.ByName("Members")).List()
	for i := 0; i < members.Len(); i++ {
		result.Members = append(result.Members, members.Get(i).String())
	}
	return result
}

// RequireMessage checks presence, not a proto3 getter's default value.
func RequireMessage(value proto.Message, name protoreflect.Name) error {
	if value == nil || !value.ProtoReflect().IsValid() {
		return faults.ErrProtocol
	}
	m := value.ProtoReflect()
	field := m.Descriptor().Fields().ByName(name)
	if field == nil || !m.Has(field) {
		return fmt.Errorf("%w: missing %s", faults.ErrProtocol, name)
	}
	return nil
}
