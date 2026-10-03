// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
)

// ReadModels returns a namespace-bound service and its immutable model catalog.
// Reads retain the store's registration barrier across connection generations.
func (s *EventStore) ReadModels() *readmodels.Service {
	s.client.mu.Lock()
	defer s.client.mu.Unlock()
	if s.latestReaders != nil {
		return s.latestReaders
	}
	return s.readModels
}

func (s *EventStore) initializeReadModels() error {
	snapshot, err := s.client.selectedStoreSnapshot(s.name)
	if err != nil {
		return err
	}
	s.client.mu.Lock()
	s.definitions = s.client.definitions[s.name]
	s.client.mu.Unlock()
	return s.initializeReadModelsFromSnapshot(snapshot)
}

func (s *EventStore) initializeReadModelsFromSnapshot(snapshot registrySnapshot) error {
	s.projectionSnapshot = snapshot.projections
	s.reactorSnapshot = snapshot.reactors
	s.reducerSnapshot = snapshot.reducers
	s.readModelReactorSnapshot = snapshot.readModelReactors
	s.initializeDecisions(snapshot)
	replayValidator, err := projectionReplayValidatorFor(snapshot)
	if err != nil {
		return err
	}
	service, err := readmodels.New(s.name, s.namespace, snapshot.models, &clientTransport{client: s.client, store: s, decisionSnapshot: s.decisionCatalog}, readmodels.WithReleasedPassiveReader(s.readPassiveReducer), readmodels.WithReducerCollectionReader(s.readReducerCollection), readmodels.WithProjectionReplayValidator(replayValidator), readmodels.WithSnapshotEventCatalog(snapshot.events), readmodels.WithReductionChanges(&s.readModelChanges))
	if err != nil {
		return err
	}
	s.readModels, s.latestReaders = service, service
	if s.definitions != nil {
		s.readerRoot = s.definitions.root
	}
	return nil
}
func (s *EventStore) registerReadModels(ctx context.Context, g *generation, root *definitionRoot, full bool) error {
	return s.definitionStage(ctx, g, root, "read-models", full, func(ctx context.Context) error {
		request := &contracts.RegisterManyRequest{EventStore: string(s.name), Owner: contracts.ReadModelOwner_Client, Source: contracts.ReadModelSource_Code}
		models := root.snapshot.models.Descriptors()
		if !full {
			models = []readmodels.Descriptor{root.delta.Model()}
		}
		for _, d := range models {
			sink := d.Sink()
			// Admission already validated canonical UUID text, without exporting a UUID dependency.
			configuration, _ := metadata.ParseCorrelationID(sink.ConfigurationID)
			kind, observer := d.Observer()
			definition := &contracts.ReadModelDefinition{Type: &contracts.ReadModelType{Identifier: string(d.Identifier()), Generation: uint32(d.Generation())}, ContainerName: d.ContainerName(), DisplayName: d.DisplayName(), Schema: d.Schema(), Sink: &contracts.SinkDefinition{ConfigurationId: wire.Guid(configuration), TypeId: string(sink.Type)}, ObserverType: contracts.ReadModelObserverType(kind), ObserverIdentifier: observer}
			for _, path := range d.Indexes() {
				definition.Indexes = append(definition.Indexes, &contracts.IndexDefinition{PropertyPath: path})
			}
			request.ReadModels = append(request.ReadModels, definition)
		}
		result, err := contracts.NewReadModelsClient(s.definitionTransport(g, root, false)).RegisterMany(ctx, request)
		if err != nil {
			return wire.RPCError(err)
		}
		if result == nil {
			return ErrProtocol
		}
		return nil
	})
}
