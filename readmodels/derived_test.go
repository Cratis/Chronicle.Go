// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/grpc"
)

type derivedModel struct {
	ID      string `json:"id"`
	Member  derivedfixtures.Member
	Members []derivedfixtures.Member
}

func TestDerivedModelReadsReleaseWatchWindowsAndFrozenProviders(t *testing.T) {
	codecs, err := derivedfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	option := readmodels.WithCodecs(codecs)
	*codecs = serialization.Codecs{}
	calls := 0
	model, err := readmodels.Define[derivedModel](option, readmodels.WithObserver(readmodels.Projection, "derived"), readmodels.WithProtection(compliance.Using(func(compliance.Target) (compliance.Classification, error) {
		calls++
		return compliance.Classification{}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	frozenCalls := calls
	d, err := model.Descriptor().WithNamingPolicy(serialization.CamelCase)
	if err != nil || calls != frozenCalls || calls == 0 {
		t.Fatalf("provider replay: %d %d %v", calls, frozenCalls, err)
	}
	want := derivedModel{ID: "person", Member: derivedfixtures.Sample().Primary, Members: []derivedfixtures.Member{derivedfixtures.RobotValue{Count: 42}}}
	data, err := d.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("read release injection", func(t *testing.T) {
		kernel := &modelKernel{get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
			return &contracts.GetInstanceByKeyResponse{ReadModel: string(data), LastHandledEventSequenceNumber: 7}, nil
		}}
		service, ctx := serviceFixture(t, kernel, d)
		reader := readmodels.For(service, model)
		got, err := reader.Get(ctx, "person")
		if err != nil || !got.Exists || !reflect.DeepEqual(got.Value, want) {
			t.Fatalf("Get: %+v %v", got, err)
		}
		released, err := reader.Release(ctx, want)
		if err != nil || !reflect.DeepEqual(released, want) {
			t.Fatalf("Release: %+v %v", released, err)
		}
		injected, err := service.GetValue(ctx, reflect.TypeFor[derivedModel](), "person")
		if err != nil || !reflect.DeepEqual(injected, want) {
			t.Fatalf("injected: %+v %v", injected, err)
		}
	})
	t.Run("watch", func(t *testing.T) {
		kernel := &watchKernel{watch: func(_ *contracts.WatchRequest, stream grpc.ServerStreamingServer[contracts.ReadModelChangeset]) error {
			for _, message := range []*contracts.ReadModelChangeset{{Subscribed: true}, sendChange("tenant-a", contracts.ReadModelChangeType_Added, string(data))} {
				if err := stream.Send(message); err != nil {
					return err
				}
			}
			<-stream.Context().Done()
			return stream.Context().Err()
		}}
		service, ctx := watchFixture(t, kernel, d)
		sub, err := readmodels.For(service, model).Watch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := sub.Close(); err != nil {
				t.Error(err)
			}
		})
		change, err := sub.Recv()
		if err != nil || !change.HasValue || !reflect.DeepEqual(change.Value, want) {
			t.Fatalf("watch: %+v %v", change, err)
		}
	})
	t.Run("materialized windows", func(t *testing.T) {
		kernel := &watchKernel{
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
		service, ctx := watchFixture(t, kernel, d)
		reader := readmodels.For(service, model).Materialized()
		values, err := reader.GetInstances(ctx, nil)
		if err != nil || !reflect.DeepEqual(values, []derivedModel{want}) {
			t.Fatalf("window get: %+v %v", values, err)
		}
		sub, err := reader.ObserveInstances(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := sub.Close(); err != nil {
				t.Error(err)
			}
		})
		values, err = sub.Recv()
		if err != nil || !reflect.DeepEqual(values, []derivedModel{want}) {
			t.Fatalf("window subscription: %+v %v", values, err)
		}
		var differ readmodels.WindowDiffer
		changes, err := differ.Diff(d, []json.RawMessage{data})
		if err != nil || len(changes) != 1 {
			t.Fatalf("diff: %+v %v", changes, err)
		}
		if _, err := differ.Diff(d, []json.RawMessage{json.RawMessage(`{"id":"person","member":{"_derivedTypeId":"unknown"}}`)}); !errors.Is(err, faults.ErrProtocol) {
			t.Fatal("bad variant accepted", err)
		}
		changes, err = differ.Diff(d, []json.RawMessage{data})
		if err != nil || len(changes) != 0 {
			t.Fatal("failed diff changed prior state", err)
		}
	})
	t.Run("variant collection normalization", func(t *testing.T) {
		value, err := d.Unmarshal([]byte(`{"id":"person","member":{"name":"Ada","_derivedTypeId":"human"}}`))
		if err != nil {
			t.Fatal(err)
		}
		if value.(*derivedModel).Member.(*derivedfixtures.HumanValue).Children == nil || value.(*derivedModel).Members == nil {
			t.Fatal("variant collections not normalized through plan")
		}
	})
}
