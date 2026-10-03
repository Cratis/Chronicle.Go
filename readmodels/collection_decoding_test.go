// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	explorer "github.com/cratis/chronicle.go/contracts/readmodelexplorer"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type historyTypedModel struct {
	ID         string
	ExternalID string `json:"ID"`
	Name       *conceptfixtures.Name
	Items      []string
	Optional   *[]string
}

func TestCollectionAndSnapshotsUseFrozenNamingAndPointerConcepts(t *testing.T) {
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase, serialization.LegacyGoCamelCase} {
		r := chronicle.NewRegistry()
		m, err := chronicle.RegisterReadModel[historyTypedModel](r, readmodels.WithObserver(readmodels.Projection, "history"))
		if err != nil {
			t.Fatal(err)
		}
		client, err := chronicle.NewClient(chronicle.WithRegistry(r), chronicle.WithNamingPolicy(policy))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := client.Close(); err != nil {
				t.Error(err)
			}
		})
		_, catalog, err := client.Catalogs("store")
		if err != nil {
			t.Fatal(err)
		}
		d, _ := catalog.LookupIdentifier(m.Identifier())
		name := conceptfixtures.Name("fixture")
		want := historyTypedModel{ID: "owner", ExternalID: "separate", Name: &name, Items: []string{}}
		data, err := d.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		service, ctx := serviceFixture(t, &modelKernel{options: []readmodels.ServiceOption{readmodels.WithProjectionReplayValidator(allowReplay)},
			replay: func(context.Context, *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
				return &contracts.GetAllInstancesResponse{Instances: []string{string(data)}}, nil
			},
			snapshots: func(context.Context, *explorer.AllSnapshotsForReadModelRequest) (*explorer.QueryResult_IEnumerable_ReadModelSnapshotResponse, error) {
				return &explorer.QueryResult_IEnumerable_ReadModelSnapshotResponse{IsAuthorized: true, Data: []*explorer.ReadModelSnapshotResponse{{Instance: string(data), Occurred: historyTime()}}}, nil
			},
		}, d)
		reader := readmodels.For(service, m)
		collection, err := reader.GetAll(ctx, nil)
		if err != nil || len(collection.Instances) != 1 || !reflect.DeepEqual(collection.Instances[0].Value, want) {
			t.Fatalf("collection=%+v err=%v", collection, err)
		}
		snapshots, err := reader.GetSnapshots(ctx, "owner")
		if err != nil || len(snapshots) != 1 || !reflect.DeepEqual(snapshots[0].Instance, want) {
			t.Fatalf("snapshots=%+v err=%v", snapshots, err)
		}
	}
}
