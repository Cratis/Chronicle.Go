// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/internal/clientoptions"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/protobuf/types/known/emptypb"
)

type projectedPlannedModel struct {
	ID         string
	ExternalID string `json:"ID"`
}
type plannedReplayResponse struct {
	contracts.UnimplementedReadModelsServer
	data string
}

func (*plannedReplayResponse) RegisterMany(context.Context, *contracts.RegisterManyRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
func (k *plannedReplayResponse) GetAllInstances(context.Context, *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
	return &contracts.GetAllInstancesResponse{Instances: []string{k.data}}, nil
}

// This isolates scenario decoding, not projection execution. The pinned kernel
// rejects a projection schema with both Id and ID; do not claim kernel projection
// support for that shape merely because the Go descriptor supports decoding it.
func TestProjectionScenarioUsesFrozenNamingPlan(t *testing.T) {
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		t.Run([]string{"preserve", "camel", "legacy"}[policy], func(t *testing.T) {
			r := chronicle.NewRegistry()
			m, err := chronicle.RegisterReadModel[projectedPlannedModel](r, readmodels.WithObserver(readmodels.Projection, "planned"))
			if err != nil {
				t.Fatal(err)
			}
			transport := substituteConnection()
			t.Cleanup(func() {
				if err := transport.Close(); err != nil {
					t.Error(err)
				}
			})
			response := &plannedReplayResponse{}
			contracts.RegisterReadModelsServer(transport, response)
			client, err := chronicle.NewClient(chronicle.WithRegistry(r), chronicle.WithNamingPolicy(policy), clientoptions.Connection[chronicle.ClientOption](transport), chronicle.WithNoAuthentication())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
			})
			artifacts, err := client.Artifacts("store")
			if err != nil {
				t.Fatal(err)
			}
			d, _ := artifacts.ReadModels.LookupIdentifier(m.Identifier())
			want := projectedPlannedModel{ID: "person", ExternalID: "external"}
			data, err := d.Marshal(want)
			if err != nil {
				t.Fatal(err)
			}
			response.data = string(data)
			store, err := client.EventStore(t.Context(), "store")
			if err != nil {
				t.Fatal(err)
			}
			s := &ReadModelScenario[projectedPlannedModel]{model: d, eventScenario: &EventScenario{Client: client, Store: store}, history: []reducers.Event{{}}}
			// Distinct public paths: all instances and single-instance replay.
			values, err := s.Instances(t.Context())
			if err != nil || len(values) != 1 || values["person"] != want {
				t.Errorf("Instances = %+v, %v; want %+v", values, err, want)
			}
			value, err := s.Instance(t.Context())
			if err != nil || !value.Exists || value.Value != want {
				t.Errorf("Instance = %+v, %v; want %+v", value, err, want)
			}
			t.Run("independent ID is not a missing key alias", func(t *testing.T) {
				response.data = `{"ID":"external"}`
				values, err := s.Instances(t.Context())
				if values != nil || !errors.Is(err, ErrFidelityUnavailable) {
					t.Errorf("keyless replay = %+v, %v; want missing-key rejection", values, err)
				}
				selected, err := s.InstanceFor(t.Context(), "external")
				if selected.Exists || !errors.Is(err, ErrFidelityUnavailable) {
					t.Errorf("external-key selection = %+v, %v", selected, err)
				}
			})
			t.Run("genuine sink alias is normalized first", func(t *testing.T) {
				response.data = `{"_id":"person","ID":"external"}`
				values, err := s.Instances(t.Context())
				if err != nil || len(values) != 1 || values["person"] != want {
					t.Errorf("sink-key replay = %+v, %v; want %+v", values, err, want)
				}
			})
		})
	}
}
