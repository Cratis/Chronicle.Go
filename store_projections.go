// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"slices"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/projections"
)

// Projections returns a detached list of this store's immutable compiled
// definitions. Query/preview and runtime registration are later projection slices.
func (s *EventStore) Projections() []projections.Definition {
	return slices.Clone(s.projectionDefinitions())
}
func (s *EventStore) projectionDefinitions() []projections.Definition {
	if definitions, ok := s.client.storeProjections[s.name]; ok {
		return definitions
	}
	return s.client.projections
}
func (s *EventStore) registerProjections(ctx context.Context, g *generation) error {
	return s.sharedStage(ctx, g, "projections", func(ctx context.Context) error {
		request := &contracts.RegisterRequest{EventStore: string(s.name), Owner: contracts.ProjectionOwner_PROJECTION_OWNER_Client, FullSet: true}
		for _, definition := range s.projectionDefinitions() {
			request.Projections = append(request.Projections, definition.KernelDefinition())
		}
		response, err := contracts.NewProjectionsClient(g.transport).Register(ctx, request)
		if err != nil {
			return wire.RPCError(err)
		}
		if response == nil {
			return ErrProtocol
		}
		return nil
	})
}
