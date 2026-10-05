// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"slices"

	"github.com/cratis/chronicle.go/constraints"
	constraintcontracts "github.com/cratis/chronicle.go/contracts/events/constraints"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
)

// Constraints returns the frozen definitions registered for this logical store.
// The slice is a copy. Enforcement and removal are kernel operations at append time.
func (s *EventStore) Constraints() []constraints.Definition { return slices.Clone(s.constraints) }

func (s *EventStore) registerConstraints(ctx context.Context, g *generation) error {
	if len(s.constraints) == 0 {
		return nil
	}
	request := &constraintcontracts.RegisterConstraintsRequest{EventStore: string(s.name)}
	for _, definition := range s.constraints {
		request.Constraints = append(request.Constraints, constraintContract(definition))
	}
	// This RPC returns Empty, not a CommandResult envelope. Its gRPC status is
	// the acknowledgement; absence of a response is not successful registration.
	result, err := constraintcontracts.NewConstraintsClient(g.transport).Register(ctx, request)
	if err != nil {
		return err
	}
	if result == nil {
		return faults.ErrProtocol
	}
	return nil
}

func constraintContract(d constraints.Definition) *constraintcontracts.Constraint {
	result := &constraintcontracts.Constraint{Name: d.Name(), Type: constraintcontracts.ConstraintType(d.Kind()),
		Definition: &constraintcontracts.OneOf_UniqueConstraintDefinition_UniqueEventTypeConstraintDefinition{}}
	for _, removal := range d.RemovalTypes() {
		result.RemovedWith = append(result.RemovedWith, string(removal.Ref().ID))
	}
	for _, sequence := range d.EventSequences() {
		result.EventSequences = append(result.EventSequences, string(sequence))
	}
	scope := d.Scope()
	if scope != (constraints.Scope{}) {
		result.Scope = &constraintcontracts.ConstraintScope{}
		if scope.PerEventSourceType {
			result.Scope.EventSourceType = "_scoped_"
		}
		if scope.PerEventStreamType {
			result.Scope.EventStreamType = "_scoped_"
		}
		if scope.PerEventStreamID {
			result.Scope.EventStreamId = "_scoped_"
		}
	}
	if d.Kind() == constraints.Unique {
		unique := &constraintcontracts.UniqueConstraintDefinition{IgnoreCasing: d.IgnoresCasing()}
		for _, field := range d.Fields() {
			unique.EventDefinitions = append(unique.EventDefinitions, &constraintcontracts.UniqueConstraintEventDefinition{EventTypeId: string(field.Event.Ref().ID), Properties: field.Properties})
		}
		result.Definition.Value0 = unique
	} else {
		unique := &constraintcontracts.UniqueEventTypeConstraintDefinition{}
		for _, event := range d.EventTypes() {
			unique.EventTypeIds = append(unique.EventTypeIds, string(event.Ref().ID))
		}
		result.Definition.Value1 = unique
	}
	return result
}

func (s *EventStore) resolveConstraintMessages(reply any) {
	var violations []*sequences.ConstraintViolation
	switch result := reply.(type) {
	case *sequences.CommandResult_AppendResponse:
		if wire.CheckEnvelope(result) != nil {
			return // The sequence decoder reports envelope failures, without calling providers.
		}
		violations = result.GetResponse().GetConstraintViolations()
	case *sequences.CommandResult_AppendManyResponse:
		if wire.CheckEnvelope(result) != nil {
			return
		}
		violations = result.GetResponse().GetConstraintViolations()
	}
	for _, violation := range violations {
		if violation == nil {
			continue // The sequence decoder rejects malformed responses.
		}
		for _, definition := range s.constraints {
			if definition.Name() == violation.ConstraintName {
				resolved := definition.ResolveMessage(constraints.Violation{
					EventTypeID: events.TypeID(violation.EventTypeId), SequenceNumber: events.SequenceNumber(violation.SequenceNumber),
					Type: constraints.Type(violation.ConstraintType), ConstraintName: violation.ConstraintName,
					Message: violation.Message, Details: violation.Details,
				})
				violation.Message = resolved.Message
				break
			}
		}
	}
}
