// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type passiveIdentityFold struct {
	identity identities.Identity
}

func (f *passiveIdentityFold) Fold(ctx context.Context, e FoldChanged, current *FoldTotal, ec events.Context) (*FoldTotal, error) {
	if !reflect.DeepEqual(metadata.Identity(ctx), f.identity) {
		return nil, errors.New("passive fold lost caller identity")
	}
	return sumFold(ctx, e, current, ec)
}

type passiveHistoryTransport struct {
	grpc.ClientConnInterface
	identity identities.Identity
}

func (p passiveHistoryTransport) Invoke(ctx context.Context, method string, _ any, reply any, _ ...grpc.CallOption) error {
	if method != sequences.EventSequences_ForEventSourceIdAndEventTypes_FullMethodName || !reflect.DeepEqual(metadata.Identity(ctx), p.identity) {
		return errors.New("unexpected passive history request or identity")
	}
	proto.Merge(reply.(proto.Message), &sequences.QueryResult_IEnumerable_AppendedEventResponse{
		IsAuthorized: true,
		Data: []*sequences.AppendedEventResponse{{
			Content: `{"amount":3}`,
			Context: &sequences.EventContext{
				EventSourceId: "source", SequenceNumber: 7,
				EventType: &sequences.EventType{Id: "FoldChanged", Generation: 1},
				Occurred:  &sequences.SerializableDateTimeOffset{Value: "2026-01-01T00:00:00Z"},
			},
		}},
	})
	return nil
}

func TestPassiveReducerReadPreservesCallerIdentityForConstructionAndFold(t *testing.T) {
	registry := NewRegistry()
	if _, err := RegisterEvent[FoldChanged](registry); err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[FoldTotal](registry)
	if err != nil {
		t.Fatal(err)
	}
	identity := identities.Identity{Subject: "reader", Name: "Reader"}
	constructed := false
	if err := RegisterReducer[*passiveIdentityFold](registry, model, func(ctx context.Context) *passiveIdentityFold {
		if !reflect.DeepEqual(metadata.Identity(ctx), identity) {
			t.Fatalf("constructor identity = %+v, want %+v", metadata.Identity(ctx), identity)
		}
		constructed = true
		return &passiveIdentityFold{identity: identity}
	}, reducers.Passive()); err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	// No kernel connection or materialization is needed for a passive fold.
	store := &EventStore{client: client, name: "store", namespace: DefaultNamespace, catalog: client.catalog}
	store.log, err = eventsequences.New(store.name, store.namespace, events.EventLog, store.catalog, passiveHistoryTransport{identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.initializeReadModels(); err != nil {
		t.Fatal(err)
	}
	ctx := metadata.WithIdentity(t.Context(), identity)
	instance, err := readmodels.For(store.ReadModels(), model).Get(ctx, "source")
	if err != nil || !constructed || !instance.Exists || instance.Value.Amount != 3 || instance.LastHandled == nil || *instance.LastHandled != 7 {
		t.Fatalf("instance = %+v, constructed = %v, error = %v", instance, constructed, err)
	}
}
