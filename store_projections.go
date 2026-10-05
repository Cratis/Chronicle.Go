// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"slices"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/projections"
	"google.golang.org/grpc"
)

// Projections returns a detached list of this store's immutable compiled
// definitions. Use QueryProjection for kernel-executed, unregistered PDL queries.
func (s *EventStore) Projections() []projections.Definition {
	return slices.Clone(s.projectionDefinitions())
}
func (s *EventStore) projectionDefinitions() []projections.Definition {
	if s.definitions != nil {
		return s.definitionRoot().snapshot.projections
	}
	if s.projectionSnapshot != nil {
		return s.projectionSnapshot
	}
	if definitions, ok := s.client.storeProjections[s.name]; ok {
		return definitions
	}
	return s.client.projections
}
func (s *EventStore) registerProjections(ctx context.Context, g *generation, root *definitionRoot, full bool) error {
	return s.definitionStage(ctx, g, root, "projections", full, func(ctx context.Context) error {
		request := &contracts.RegisterRequest{EventStore: string(s.name), Owner: contracts.ProjectionOwner_PROJECTION_OWNER_Client, FullSet: full}
		definitions := root.snapshot.projections
		if !full {
			definitions = []projections.Definition{root.delta}
		}
		for _, definition := range definitions {
			request.Projections = append(request.Projections, definition.KernelDefinition())
		}
		response, err := contracts.NewProjectionsClient(s.definitionTransport(g, root, full)).Register(ctx, request, grpc.ForceCodec(projectionRegistrationCodec{}))
		if err != nil {
			return wire.RPCError(err)
		}
		if response == nil {
			return ErrProtocol
		}
		return nil
	})
}
