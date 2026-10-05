// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
)

type plannedWatchModel struct {
	ID         string
	ExternalID string `json:"ID"`
}

func TestWatchesAndWindowsRoundTripRegisteredNamingPlan(t *testing.T) {
	model, err := readmodels.Define[plannedWatchModel](readmodels.WithObserver(readmodels.Projection, "planned"))
	if err != nil {
		t.Fatal(err)
	}
	want := plannedWatchModel{ID: "person", ExternalID: "external"}
	data, err := model.Descriptor().Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]string
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if fields["Id"] != want.ID || fields["ID"] != want.ExternalID {
		t.Fatalf("serialized naming plan = %s", data)
	}
	t.Run("descriptor and differ", func(t *testing.T) {
		decoded, err := model.Descriptor().Unmarshal(data)
		if err != nil || !reflect.DeepEqual(decoded, &want) {
			t.Fatalf("decoded = %+v, error = %v", decoded, err)
		}
		differ := &readmodels.WindowDiffer{}
		changes, err := differ.Diff(model.Descriptor(), []json.RawMessage{data})
		if err != nil || len(changes) != 1 || changes[0].Key != "person" || changes[0].Type != readmodels.Added {
			t.Fatalf("diff = %+v, error = %v", changes, err)
		}
		decoded, err = model.Descriptor().Unmarshal(changes[0].Value)
		if err != nil || !reflect.DeepEqual(decoded, &want) {
			t.Fatalf("diff model = %+v, error = %v", decoded, err)
		}
		changes, err = differ.Diff(model.Descriptor(), []json.RawMessage{data})
		if err != nil || len(changes) != 0 {
			t.Fatalf("unchanged diff = %+v, error = %v", changes, err)
		}
	})
	t.Run("typed watch", func(t *testing.T) {
		k := &watchKernel{watch: func(_ *contracts.WatchRequest, stream grpc.ServerStreamingServer[contracts.ReadModelChangeset]) error {
			for _, message := range []*contracts.ReadModelChangeset{{Subscribed: true}, sendChange("tenant-a", contracts.ReadModelChangeType_Added, string(data))} {
				if err := stream.Send(message); err != nil {
					return err
				}
			}
			<-stream.Context().Done()
			return stream.Context().Err()
		}}
		s, ctx := watchFixture(t, k, model.Descriptor())
		sub, err := readmodels.For(s, model).Watch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := sub.Close(); err != nil {
				t.Error(err)
			}
		}()
		change, err := sub.Recv()
		if err != nil || !change.HasValue || change.Value != want {
			t.Fatalf("change = %+v, error = %v", change, err)
		}
	})
	t.Run("typed materialized windows", func(t *testing.T) {
		k := &watchKernel{
			get: func(context.Context, *contracts.GetInstancesRequest) (*contracts.GetInstancesResponse, error) {
				return &contracts.GetInstancesResponse{Instances: []string{string(data)}}, nil
			},
			observe: func(_ *contracts.ObserveInstancesRequest, stream grpc.ServerStreamingServer[contracts.ObserveInstancesResponse]) error {
				if err := stream.Send(&contracts.ObserveInstancesResponse{Instances: []string{string(data)}}); err != nil {
					return err
				}
				<-stream.Context().Done()
				return stream.Context().Err()
			},
		}
		s, ctx := watchFixture(t, k, model.Descriptor())
		reader := readmodels.For(s, model).Materialized()
		values, err := reader.GetInstances(ctx, nil)
		if err != nil || !reflect.DeepEqual(values, []plannedWatchModel{want}) {
			t.Fatalf("values = %+v, error = %v", values, err)
		}
		sub, err := reader.ObserveInstances(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := sub.Close(); err != nil {
				t.Error(err)
			}
		}()
		values, err = sub.Recv()
		if err != nil || !reflect.DeepEqual(values, []plannedWatchModel{want}) {
			t.Fatalf("window = %+v, error = %v", values, err)
		}
	})
}

type booleanKeyModel struct {
	ID bool `json:"id" chronicle:"key"`
}

func TestMaterializedDifferAddsAndRemovesBooleanKeys(t *testing.T) {
	model, err := readmodels.Define[booleanKeyModel]()
	if err != nil {
		t.Fatal(err)
	}
	differ := &readmodels.WindowDiffer{}
	changes, err := differ.Diff(model.Descriptor(), []json.RawMessage{json.RawMessage(`{"id":true}`), json.RawMessage(`{"id":false}`)})
	if err != nil || len(changes) != 2 {
		t.Fatalf("initial = %+v, error = %v", changes, err)
	}
	for i, key := range []readmodels.Key{"true", "false"} {
		if changes[i].Key != key || changes[i].Type != readmodels.Added || !changes[i].HasValue {
			t.Fatalf("addition = %+v, want key %q", changes[i], key)
		}
	}
	changes, err = differ.Diff(model.Descriptor(), nil)
	if err != nil || len(changes) != 2 {
		t.Fatalf("empty window = %+v, error = %v", changes, err)
	}
	for i, key := range []readmodels.Key{"true", "false"} {
		if changes[i].Key != key || changes[i].Type != readmodels.Removed || changes[i].HasValue {
			t.Fatalf("removal = %+v, want key %q", changes[i], key)
		}
	}
}
