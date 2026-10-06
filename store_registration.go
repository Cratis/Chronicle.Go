// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/kernelcapability"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/serialization"
)

func (s *EventStore) registerStages(ctx context.Context, g *generation, root *definitionRoot) ([]ArtifactRegistration, error) {
	full := s.cumulativeRegistration(g, root)
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
	if len(root.snapshot.models.Descriptors()) > 0 {
		stages = append(stages, struct {
			name string
			run  func(context.Context) error
		}{"read-models", func(ctx context.Context) error { return s.registerReadModels(ctx, g, root, full) }})
	}
	stages = append(stages, struct {
		name string
		run  func(context.Context) error
	}{"constraints", func(ctx context.Context) error {
		return s.sharedStage(ctx, g, "constraints", func(ctx context.Context) error { return s.registerConstraints(ctx, g) })
	}})
	if len(root.snapshot.projections) > 0 {
		stages = append(stages, struct {
			name string
			run  func(context.Context) error
		}{"projections", func(ctx context.Context) error { return s.registerProjections(ctx, g, root, full) }})
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
	registrations := make(map[events.TypeID]*eventtypes.EventTypeRegistration)
	descriptors := s.catalog.Descriptors()
	for _, descriptor := range descriptors {
		if err := nestedProtectionAdmission(g, descriptor.Schema()); err != nil {
			return err
		}
	}
	for _, descriptor := range descriptors {
		if descriptor.IsHistorical() {
			continue
		}
		ref := descriptor.Ref()
		registration := &eventtypes.EventTypeRegistration{
			Type: &eventtypes.EventType{Id: string(ref.ID), Generation: uint32(ref.Generation), Tombstone: false}, Schema: descriptor.Schema(), EventStore: descriptor.SourceStore(),
		}
		registrations[ref.ID] = registration
		request.Types = append(request.Types, registration)
	}
	for _, descriptor := range descriptors {
		registration := registrations[descriptor.Ref().ID]
		registration.Generations = append(registration.Generations, &eventtypes.EventTypeGenerationDefinition{Generation: uint32(descriptor.Ref().Generation), Schema: descriptor.Schema()})
	}
	for _, registration := range request.Types {
		slices.SortFunc(registration.Generations, func(a, b *eventtypes.EventTypeGenerationDefinition) int {
			return cmp.Compare(a.Generation, b.Generation)
		})
	}
	for _, migration := range s.catalog.Migrations() {
		registration := registrations[migration.EventType]
		registration.Migrations = append(registration.Migrations, &eventtypes.EventTypeMigrationDefinition{FromGeneration: uint32(migration.From), ToGeneration: uint32(migration.To), UpcastJmesPath: migration.UpcastJSON, DowncastJmesPath: migration.DowncastJSON})
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

// Kernels before 19.32.2 skip protection beneath unprotected maps and on
// collection-valued array elements (Chronicle#4551, #4552) and would store those
// values unprotected. Refuse registration, before any RPC, unless the
// generation's kernel is known to apply that metadata. The refusal is
// deterministic and is not retried.
func nestedProtectionAdmission(g *generation, schema string) error {
	if g.capabilities.ProtectedRelease || !serialization.NestedCollectionProtection(schema) {
		return nil
	}
	return kernelcapability.Require(false, "protection beneath maps or on collection-valued array elements", kernelcapability.ProtectedReleaseVersion)
}
