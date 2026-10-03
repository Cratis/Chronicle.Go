// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	explorer "github.com/cratis/chronicle.go/contracts/readmodelexplorer"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/grpc"
)

func TestDerivedHistoryUsesFrozenPlanAndRejectsAmbiguousRawDocuments(t *testing.T) {
	codecs, err := derivedfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[derivedModel](readmodels.WithCodecs(codecs), readmodels.WithObserver(readmodels.Projection, "derived"))
	if err != nil {
		t.Fatal(err)
	}
	d, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	const good = `{"_id":"owner","member":{"count":42,"_derivedTypeId":"robot"}}`
	for name, document := range map[string]string{
		"valid":                                good,
		"duplicate parent hides discriminator": `{"_id":"owner","member":{"children":[{"_derivedTypeId":"robot","_derivedTypeId":"PRIVATE"}],"children":[],"_derivedTypeId":"human"}}`,
		"duplicate root":                       `{"_id":"owner","member":{"_derivedTypeId":"PRIVATE"},"member":{"_derivedTypeId":"robot"}}`,
		"escaped map key in unknown array":     `{"_id":"owner","unknown":[{"PRIVATE":1,"\u0050RIVATE":2}]}`,
		"unknown discriminator":                `{"_id":"owner","member":{"_derivedTypeId":"PRIVATE"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			service, ctx := serviceFixture(t, &modelKernel{
				options: []readmodels.ServiceOption{readmodels.WithProjectionReplayValidator(allowReplay)},
				replay: func(context.Context, *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
					return &contracts.GetAllInstancesResponse{Instances: []string{good, document}, ProcessedEventsCount: 2}, nil
				},
				snapshots: func(context.Context, *explorer.AllSnapshotsForReadModelRequest) (*explorer.QueryResult_IEnumerable_ReadModelSnapshotResponse, error) {
					return &explorer.QueryResult_IEnumerable_ReadModelSnapshotResponse{IsAuthorized: true, Data: []*explorer.ReadModelSnapshotResponse{{Instance: good, Occurred: historyTime()}, {Instance: document, Occurred: historyTime()}}}, nil
				},
			}, d)
			reader := readmodels.For(service, model) // Original handle, rebound catalog.
			collection, collectionErr := reader.GetAll(ctx, nil)
			snapshots, snapshotErr := reader.GetSnapshots(ctx, "owner")
			if name == "valid" {
				want := derivedModel{ID: "owner", Member: derivedfixtures.RobotValue{Count: 42}, Members: []derivedfixtures.Member{}}
				if collectionErr != nil || snapshotErr != nil || len(collection.Instances) != 2 || len(snapshots) != 2 || collection.ProcessedEventsCount != 2 || !reflect.DeepEqual(collection.Instances[1].Value, want) || !reflect.DeepEqual(snapshots[1].Instance, want) {
					t.Fatalf("frozen family history: %+v %+v %v %v", collection, snapshots, collectionErr, snapshotErr)
				}
				return
			}
			for _, failure := range []error{collectionErr, snapshotErr} {
				if !errors.Is(failure, faults.ErrProtocol) || strings.Contains(failure.Error(), "PRIVATE") {
					t.Fatal("unsafe history failure", failure)
				}
			}
			if collection.Instances != nil || collection.ProcessedEventsCount != 0 || snapshots != nil {
				t.Fatal("partial typed history returned")
			}
			if name == "unknown discriminator" {
				return // Raw unprotected reads validate JSON, not typed family membership.
			}
			rawCollection, err := service.GetAll(ctx, model.Identifier(), nil)
			if !errors.Is(err, faults.ErrProtocol) || rawCollection.Instances != nil || rawCollection.ProcessedEventsCount != 0 {
				t.Fatal("ambiguous raw collection returned", err)
			}
			rawSnapshots, err := service.GetSnapshots(ctx, model.Identifier(), "owner")
			if !errors.Is(err, faults.ErrProtocol) || rawSnapshots != nil {
				t.Fatal("ambiguous raw snapshots returned", err)
			}
		})
	}
}

func TestDerivedSnapshotUnknownContributionRejectsHiddenDuplicateDiscriminator(t *testing.T) {
	model := person(t, readmodels.WithObserver(readmodels.Projection, "history"))
	contribution := historyContribution() // Unregistered event must still be scanned.
	contribution.Content = `{"parent":{"_derivedTypeId":"PRIVATE","_derivedTypeId":"robot"},"parent":{}}`
	service, ctx := serviceFixture(t, &modelKernel{
		options: []readmodels.ServiceOption{readmodels.WithProjectionReplayValidator(allowReplay)},
		snapshots: func(context.Context, *explorer.AllSnapshotsForReadModelRequest) (*explorer.QueryResult_IEnumerable_ReadModelSnapshotResponse, error) {
			return &explorer.QueryResult_IEnumerable_ReadModelSnapshotResponse{IsAuthorized: true, Data: []*explorer.ReadModelSnapshotResponse{{Instance: `{}`, Occurred: historyTime()}, {Instance: `{}`, Occurred: historyTime(), Events: []*explorer.Event{contribution}}}}, nil
		},
	}, model.Descriptor())
	result, err := service.GetSnapshots(ctx, model.Identifier(), "owner")
	if !errors.Is(err, faults.ErrProtocol) || result != nil || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("ambiguous contribution returned", err)
	}
}

func TestDerivedTypedWatchRejectsUnknownFamilyWithoutDeliveringValue(t *testing.T) {
	codecs, err := derivedfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[derivedModel](readmodels.WithCodecs(codecs), readmodels.WithObserver(readmodels.Projection, "derived"))
	if err != nil {
		t.Fatal(err)
	}
	service, ctx := watchFixture(t, &watchKernel{watch: func(_ *contracts.WatchRequest, stream grpc.ServerStreamingServer[contracts.ReadModelChangeset]) error {
		for _, message := range []*contracts.ReadModelChangeset{{Subscribed: true}, sendChange("tenant-a", contracts.ReadModelChangeType_Added, `{"_id":"owner","Member":{"_derivedTypeId":"PRIVATE"}}`)} {
			if err := stream.Send(message); err != nil {
				return err
			}
		}
		<-stream.Context().Done()
		return stream.Context().Err()
	}}, model.Descriptor())
	sub, err := readmodels.For(service, model).Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() }) // Recv owns the expected terminal failure.
	change, err := sub.Recv()
	if !errors.Is(err, faults.ErrProtocol) || change.HasValue || change.Value.Member != nil || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("invalid family delivered", err)
	}
}
