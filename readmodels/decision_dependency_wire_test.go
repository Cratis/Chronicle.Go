// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	projections "github.com/cratis/chronicle.go/contracts/projections"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type dependencyAgreementServer struct {
	contracts.UnimplementedReadModelsServer
	projections.UnimplementedProjectionsServer
	model      *contracts.ReadModelDefinition
	projection *projections.ProjectionDefinition
}

func (s *dependencyAgreementServer) GetDefinitions(context.Context, *contracts.GetDefinitionsRequest) (*contracts.GetDefinitionsResponse, error) {
	return &contracts.GetDefinitionsResponse{ReadModels: []*contracts.ReadModelDefinition{s.model}}, nil
}

func (s *dependencyAgreementServer) GetAllDefinitions(context.Context, *projections.GetAllDefinitionsRequest) (*projections.IEnumerable_ProjectionDefinition, error) {
	return &projections.IEnumerable_ProjectionDefinition{Items: []*projections.ProjectionDefinition{s.projection}}, nil
}

func TestDecisionDependencyAgreementAcrossProtobufTransport(t *testing.T) {
	for _, tc := range []struct {
		name, schema string
		refused      bool
	}{
		{"conditional namespace protection", `{"properties":{"id":{"type":"string"},"name":{"type":"string"}},"dependencies":{"id":{"properties":{"name":{"security":[{"metadataType":"EncryptedNamespace"}]}}}}}`, true},
		{"plain property and schema dependencies", `{"properties":{"id":{"type":"string"},"name":{"type":"string"}},"dependencies":{"id":["name"],"name":{"properties":{"extra":{"type":"number","default":{"security":null}}}}}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newDecisionFixture(t)
			admitted, err := f.reader.assess()
			if err != nil {
				t.Fatal(err)
			}
			definition := f.respond(&contracts.GetDefinitionsRequest{}).(*contracts.GetDefinitionsResponse).ReadModels[0]
			definition.Schema = tc.schema
			listener := bufconn.Listen(1 << 20)
			server := grpc.NewServer()
			fixture := &dependencyAgreementServer{model: definition, projection: admitted.projection}
			contracts.RegisterReadModelsServer(server, fixture)
			projections.RegisterProjectionsServer(server, fixture)
			served := make(chan error, 1)
			go func() {
				defer close(served)
				served <- server.Serve(listener)
			}()
			t.Cleanup(func() {
				server.Stop()
				if err := listener.Close(); err != nil {
					t.Error(err)
				}
				for err := range served {
					if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
						t.Error(err)
					}
				}
			})
			conn, err := grpc.NewClient("passthrough:///decision-dependencies",
				grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := conn.Close(); err != nil {
					t.Error(err)
				}
			})
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			// The real GetDefinitions RPC carries Schema as protobuf field 5.
			// Verify exact bytes, then use that same transport for agreement.
			response, err := contracts.NewReadModelsClient(conn).GetDefinitions(ctx, &contracts.GetDefinitionsRequest{EventStore: "store"})
			if err != nil || len(response.GetReadModels()) != 1 || response.ReadModels[0].Schema != tc.schema {
				t.Fatal("definition schema did not survive protobuf transport", err)
			}
			err = checkDecisionAgreement(ctx, conn, "store", admitted)
			if tc.refused {
				var refused *DecisionReadRefused
				if !errors.As(err, &refused) || refused.Reason != DecisionDefinitionMismatch {
					t.Fatal("wire dependency protection admitted", err)
				}
			} else if err != nil {
				t.Fatal("plain wire dependency variation refused", err)
			}
		})
	}
}
