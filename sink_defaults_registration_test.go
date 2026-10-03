// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"encoding/hex"
	"reflect"
	"testing"

	projectioncontracts "github.com/cratis/chronicle.go/contracts/projections"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestDefaultSinkRegistrationFrozenReconnectAndOriginalTypedHandle(t *testing.T) {
	r := NewRegistry()
	event, err := RegisterEvent[sinkDefaultEvent](r)
	if err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[sinkDefaultModel](r)
	if err != nil {
		t.Fatal(err)
	}
	prepared := 0
	if err := RegisterProjectionFactory(r, "p", model.Descriptor(), func() *sinkFactory { prepared++; return &sinkFactory{} }, func(context.Context, *sinkFactory) (projections.Declaration, error) {
		return projections.ModelBound(model, projections.WithIdentifier("p"), projections.FromEvent(event)), nil
	}); err != nil {
		t.Fatal(err)
	}
	requests := make(chan *contracts.RegisterManyRequest, 2)
	kernel := &supervisedKernel{
		readModels: &readModelKernel{
			register: func(_ context.Context, request *contracts.RegisterManyRequest) error {
				requests <- proto.CloneOf(request)
				return nil
			},
			get: func(context.Context, *contracts.GetInstanceByKeyRequest) (*contracts.GetInstanceByKeyResponse, error) {
				return &contracts.GetInstanceByKeyResponse{ReadModel: `{"ID":"source","Name":"read"}`}, nil
			},
		},
		projections: &projectionKernel{register: func(context.Context, *projectioncontracts.RegisterRequest) error { return nil }},
	}
	client, ctx := supervisionClient(t, kernel, WithRegistry(r), WithDefaultSinkType(readmodels.SQL))
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	first := <-requests
	if first.ReadModels[0].Sink.TypeId != "SQL" {
		t.Fatal("wire default not selected")
	}
	_, offline, err := client.Catalogs("store")
	if err != nil || !reflect.DeepEqual(offline.Descriptors(), store.ReadModels().Catalog().Descriptors()) {
		t.Fatal("catalog/handle mismatch", err)
	}
	instance, err := readmodels.For(store.ReadModels(), model).Get(ctx, "source")
	if err != nil || !instance.Exists || instance.Value.Name != "read" {
		t.Fatal("original typed handle rejected", err)
	}
	kernel.endStream <- status.Error(codes.Unavailable, "replace generation")
	select {
	case second := <-requests:
		firstBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(first)
		if err != nil {
			t.Fatal(err)
		}
		secondBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(second)
		if err != nil || !reflect.DeepEqual(firstBytes, secondBytes) {
			t.Fatal("reconnect changed registration bytes", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if prepared != 1 || model.Descriptor().Sink().Type != readmodels.MongoDB {
		t.Fatal("reprepared or mutated declaration")
	}
}

func TestDefaultSinkCSharpHandDerivedWireGoldens(t *testing.T) {
	// Hand-derived from C# 2e31b0dfb Contracts.ReadModels.SinkDefinition:
	// field 1 is protobuf-net bcl.Guid, field 2 is the provider string. These
	// are not captured C# runtime output or live backend qualification.
	for _, tc := range []struct {
		kind        readmodels.SinkType
		config, hex string
	}{
		{readmodels.MongoDB, "00000000-0000-0000-0000-000000000000", "0a0012074d6f6e676f4442"},
		{readmodels.SQL, "00000000-0000-0000-0000-000000000000", "0a00120353514c"},
		{readmodels.InMemory, "00000000-0000-0000-0000-000000000000", "0a001208496e4d656d6f7279"},
		{readmodels.NoSink, "00000000-0000-0000-0000-000000000000", "0a0012044e6f6e65"},
		{readmodels.MongoDB, "00112233-4455-6677-8899-aabbccddeeff", "0a12093322110055447766118899aabbccddeeff12074d6f6e676f4442"},
	} {
		configuration, err := metadata.ParseCorrelationID(tc.config)
		if err != nil {
			t.Fatal(err)
		}
		got, err := proto.Marshal(&contracts.SinkDefinition{TypeId: string(tc.kind), ConfigurationId: wire.Guid(configuration)})
		if err != nil || hex.EncodeToString(got) != tc.hex {
			t.Fatalf("%s: wire=%x want=%s err=%v", tc.kind, got, tc.hex, err)
		}
	}
}
