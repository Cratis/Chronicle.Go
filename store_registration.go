// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"

	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	"github.com/cratis/chronicle.go/internal/wire"
)

func (s *EventStore) registerStages(ctx context.Context, g *generation) ([]ArtifactRegistration, error) {
	var artifacts []ArtifactRegistration
	stages := []struct {
		name string
		run  func(context.Context) error
	}{
		{"store", func(ctx context.Context) error {
			return s.sharedStage(ctx, g, "store", func(ctx context.Context) error {
				result, err := eventstores.NewEventStoresClient(g.transport).EnsureEventStore(ctx, &eventstores.EnsureEventStoreRequest{Name: string(s.name)})
				if err != nil {
					return err
				}
				return wire.CheckEnvelope(result)
			})
		}},
		{"namespace", func(ctx context.Context) error {
			result, err := namespaces.NewNamespacesClient(g.transport).EnsureNamespace(ctx, &namespaces.EnsureNamespaceRequest{EventStore: string(s.name), Namespace: string(s.namespace)})
			if err != nil {
				return err
			}
			return wire.CheckEnvelope(result)
		}},
		{"event-types", func(ctx context.Context) error {
			return s.sharedStage(ctx, g, "event-types", func(ctx context.Context) error { return s.registerEventTypes(ctx, g) })
		}},
	}
	// C# EventStore.RegisterAllArtifacts registers event types and read models
	// first, then constraints and observers.
	if s.readModels != nil && len(s.readModels.Catalog().Descriptors()) > 0 {
		stages = append(stages, struct {
			name string
			run  func(context.Context) error
		}{"read-models", func(ctx context.Context) error { return s.registerReadModels(ctx, g) }})
	}
	stages = append(stages, struct {
		name string
		run  func(context.Context) error
	}{"constraints", func(ctx context.Context) error {
		return s.sharedStage(ctx, g, "constraints", func(ctx context.Context) error { return s.registerConstraints(ctx, g) })
	}})
	if len(s.projectionDefinitions()) > 0 {
		stages = append(stages, struct {
			name string
			run  func(context.Context) error
		}{"projections", func(ctx context.Context) error { return s.registerProjections(ctx, g) }})
	}
	for _, stage := range stages {
		if err := ctx.Err(); err != nil {
			return artifacts, err
		}
		err := stage.run(ctx)
		artifacts = append(artifacts, ArtifactRegistration{Name: stage.name, Failure: err})
		if err != nil {
			return artifacts, err
		}
	}
	return artifacts, nil
}

func (s *EventStore) sharedStage(ctx context.Context, g *generation, name string, run func(context.Context) error) error {
	policy := s.client.config.registrationRetry
	policy.MaxAttempts = 1 // The outer namespace pass owns backoff; no multiplied retry loop.
	outcome := g.registrations.For(fmt.Sprintf("%s:%q", name, s.name)).Run(ctx, g.number, policy, retryRegistration, func(ctx context.Context) ([]ArtifactRegistration, error) {
		err := run(ctx)
		return []ArtifactRegistration{{Name: name, Failure: err}}, err
	})
	return outcome.Failure
}

func (s *EventStore) registerEventTypes(ctx context.Context, g *generation) error {
	request := &eventtypes.RegisterEventTypesRequest{EventStore: string(s.name), DisableValidation: !s.client.config.validateEventTypes}
	for _, descriptor := range s.catalog.Descriptors() {
		ref := descriptor.Ref()
		request.Types = append(request.Types, &eventtypes.EventTypeRegistration{
			Type: &eventtypes.EventType{Id: string(ref.ID), Generation: uint32(ref.Generation)}, Schema: descriptor.Schema(),
			Generations: []*eventtypes.EventTypeGenerationDefinition{{Generation: uint32(ref.Generation), Schema: descriptor.Schema()}},
		})
	}
	if len(request.Types) == 0 {
		return nil
	}
	result, err := eventtypes.NewEventTypesClient(g.transport).RegisterEventTypes(ctx, request)
	if err != nil {
		return err
	}
	return wire.CheckEnvelope(result)
}
