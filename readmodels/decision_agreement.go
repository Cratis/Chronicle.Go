// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"slices"

	projections "github.com/cratis/chronicle.go/contracts/projections"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/grpc"
)

// Compare the C# selected fields, not complete client/server protobuf equality.
// Re-admitting the server shape (including parents/All), model generation,
// protection and actual key schema are deliberate fail-closed additions. There
// is no definition-bound wire proof.
func checkDecisionAgreement(ctx context.Context, conn grpc.ClientConnInterface, store string, admitted admittedDecision) error {
	models, err := contracts.NewReadModelsClient(conn).GetDefinitions(ctx, &contracts.GetDefinitionsRequest{EventStore: store})
	if err != nil {
		return wire.RPCError(err)
	}
	if models == nil {
		return faults.ErrProtocol
	}
	var matches []*contracts.ReadModelDefinition
	for _, model := range models.ReadModels {
		if model.GetType().GetIdentifier() == string(admitted.descriptor.Identifier()) {
			matches = append(matches, model)
		}
	}
	definitions, err := projections.NewProjectionsClient(conn).GetAllDefinitions(ctx, &projections.GetAllDefinitionsRequest{EventStore: store})
	if err != nil {
		return wire.RPCError(err)
	}
	if definitions == nil {
		return faults.ErrProtocol
	}
	var producers []*projections.ProjectionDefinition
	for _, definition := range definitions.Items {
		if definition.GetIdentifier() == admitted.projection.Identifier {
			producers = append(producers, definition)
		}
	}
	refused := &DecisionReadRefused{Model: admitted.descriptor.Identifier(), Reason: DecisionDefinitionMismatch}
	if len(matches) != 1 || len(producers) != 1 {
		return refused
	}
	model, projection := matches[0], producers[0]
	if model.GetType().GetGeneration() != uint32(admitted.descriptor.Generation()) {
		return refused
	}
	// A locally plain model is not proof the latest server schema is plain.
	// Namespace/global roots have false subject flags but still need release;
	// session decisions must reject all scopes, unknown metadata and parse errors.
	roots, err := serialization.ProtectionRoots(model.Schema)
	if err != nil || len(roots) != 0 {
		return refused
	}
	if model.ObserverType != contracts.ReadModelObserverType_Projection || model.ObserverIdentifier != admitted.projection.Identifier || projection.ReadModel != admitted.projection.ReadModel || projection.EventSequenceId != admitted.projection.EventSequenceId {
		return refused
	}
	key, ok := decisionKeySchema(model.Schema)
	if !ok || key != admitted.key {
		return refused
	}
	types, reason := assessProjection(projection, admitted.catalog.Events)
	if reason != "" || !slices.EqualFunc(types, admitted.types, func(a, b events.TypeRef) bool { return a.ID == b.ID }) {
		return refused
	}
	return nil
}
