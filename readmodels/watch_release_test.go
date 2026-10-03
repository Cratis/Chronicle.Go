// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/contracts/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
)

func TestWatchAndMaterializedReleaseOrderedAndFailClosed(t *testing.T) {
	for _, materialized := range []bool{false, true} {
		t.Run(map[bool]string{false: "changes", true: "windows"}[materialized], func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			model := person(t, readmodels.WithObserver(readmodels.Projection, "people"), readmodels.WithPII("name"))
			k := &watchKernel{release: func(ctx context.Context, r *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
				if r.Namespace != "tenant-a" || r.EventStore != "store" {
					t.Error("release crossed namespace")
				}
				if strings.Contains(r.Payload, "first") {
					close(started)
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
					return &compliance.ReleaseResponse{Payload: `{"id":"person","name":"clear"}`}, nil
				}
				return &compliance.ReleaseResponse{HasError: true}, nil
			}}
			k.watch = func(_ *contracts.WatchRequest, stream grpc.ServerStreamingServer[contracts.ReadModelChangeset]) error {
				for _, m := range []*contracts.ReadModelChangeset{{Subscribed: true}, sendChange("tenant-a", contracts.ReadModelChangeType_Added, `{"id":"person","name":"first-ciphertext"}`), sendChange("tenant-a", contracts.ReadModelChangeType_Modified, `{"id":"person","name":"second-ciphertext"}`)} {
					if err := stream.Send(m); err != nil {
						return err
					}
				}
				<-stream.Context().Done()
				return stream.Context().Err()
			}
			k.observe = func(_ *contracts.ObserveInstancesRequest, stream grpc.ServerStreamingServer[contracts.ObserveInstancesResponse]) error {
				for _, value := range []string{`{"id":"person","name":"first-ciphertext"}`, `{"id":"person","name":"second-ciphertext"}`} {
					if err := stream.Send(&contracts.ObserveInstancesResponse{Instances: []string{value}}); err != nil {
						return err
					}
				}
				<-stream.Context().Done()
				return stream.Context().Err()
			}
			s, ctx := watchFixture(t, k, model.Descriptor())
			go func() {
				select {
				case <-started:
					close(release)
				case <-ctx.Done():
				}
			}()
			reader := readmodels.For(s, model)
			if materialized {
				sub, err := reader.Materialized().ObserveInstances(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := sub.Close(); err != nil {
						t.Error(err)
					}
				}()
				value, err := sub.Recv()
				if err != nil || len(value) != 1 || value[0].Name != "clear" {
					t.Fatalf("released window: %+v %v", value, err)
				}
				value, err = sub.Recv()
				if !errors.Is(err, readmodels.ErrRelease) || len(value) != 0 {
					t.Fatalf("unsafe window: %+v %v", value, err)
				}
			} else {
				sub, err := reader.Watch(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := sub.Close(); err != nil {
						t.Error(err)
					}
				}()
				value, err := sub.Recv()
				if err != nil || value.Value.Name != "clear" {
					t.Fatalf("released change: %+v %v", value, err)
				}
				value, err = sub.Recv()
				if !errors.Is(err, readmodels.ErrRelease) || value.HasValue {
					t.Fatalf("unsafe change: %+v %v", value, err)
				}
			}
		})
	}
}
func TestLocalReducerWatchReleaseRemovalAndGenerationIsolation(t *testing.T) {
	model := person(t, readmodels.WithObserver(readmodels.Reducer, "people"), readmodels.WithPII("name"))
	source := &readmodels.ReductionChanges{}
	k := &watchKernel{release: func(_ context.Context, r *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
		return &compliance.ReleaseResponse{Payload: r.Payload}, nil
	}}
	s, ctx := watchFixture(t, k, model.Descriptor(), readmodels.WithReductionChanges(source))
	generation, cancel := context.WithCancel(ctx)
	defer cancel()
	source.BindGeneration(generation)
	sub, err := readmodels.For(s, model).Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Close(); err != nil {
			t.Error(err)
		}
	}()
	source.Publish(generation, model.Identifier(), "person", json.RawMessage(`{"id":"person","name":"Ada"}`))
	change, err := sub.Recv()
	if err != nil || change.Type != readmodels.Modified || change.Value.Name != "Ada" {
		t.Fatalf("local: %+v %v", change, err)
	}
	source.Publish(generation, model.Identifier(), "person", nil)
	change, err = sub.Recv()
	if err != nil || change.Type != readmodels.Removed || change.HasValue {
		t.Fatalf("removal: %+v %v", change, err)
	}
	cancel()
	<-sub.Done()
	if !errors.Is(sub.Err(), readmodels.ErrInterrupted) {
		t.Fatal(sub.Err())
	}
	nextGeneration, nextCancel := context.WithCancel(ctx)
	defer nextCancel()
	source.BindGeneration(nextGeneration)
	next, err := readmodels.For(s, model).Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := next.Close(); err != nil {
			t.Error(err)
		}
	}()
	source.Publish(generation, model.Identifier(), "old", json.RawMessage(`{"id":"old"}`))
	source.Publish(nextGeneration, model.Identifier(), "new", json.RawMessage(`{"id":"new"}`))
	change, err = next.Recv()
	if err != nil || change.Key != "new" {
		t.Fatalf("retired generation leaked: %+v %v", change, err)
	}
}
