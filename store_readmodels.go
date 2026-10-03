// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"slices"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
)

// ReadModels returns a namespace-bound service and its immutable model catalog.
// Reads retain the store's registration barrier across connection generations.
func (s *EventStore) ReadModels() *readmodels.Service { return s.readModels }

func (s *EventStore) initializeReadModels() error {
	catalog := s.client.readModelCatalog
	if selected, ok := s.client.readModelCatalogs[s.name]; ok {
		catalog = selected
	}
	models := catalog.Descriptors()
	definitions := s.projectionDefinitions()
	s.projectionSnapshot = slices.Clone(definitions)
	for i, definition := range definitions {
		bound, err := definition.ForStore(string(s.name))
		if err != nil {
			return err
		}
		s.projectionSnapshot[i] = bound
		for j, model := range models {
			if model.Identifier() == bound.Model().Identifier() {
				models[j] = bound.Model()
			}
		}
	}
	catalog, err := readmodels.NewCatalog(models...)
	if err != nil {
		return err
	}
	service, err := readmodels.New(s.name, s.namespace, catalog, &clientTransport{client: s.client, store: s}, readmodels.WithPassiveReader(s.readPassiveReducer))
	if err != nil {
		return err
	}
	s.readModels = service
	return nil
}
func (s *EventStore) registerReadModels(ctx context.Context, g *generation) error {
	return s.sharedStage(ctx, g, "read-models", func(ctx context.Context) error {
		request := &contracts.RegisterManyRequest{EventStore: string(s.name), Owner: contracts.ReadModelOwner_Client, Source: contracts.ReadModelSource_Code}
		for _, d := range s.readModels.Catalog().Descriptors() {
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
		result, err := contracts.NewReadModelsClient(g.transport).RegisterMany(ctx, request)
		if err != nil {
			return wire.RPCError(err)
		}
		if result == nil {
			return ErrProtocol
		}
		return nil
	})
}
