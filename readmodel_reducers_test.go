// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/observation/reducers"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestReadModelReactorInfersFirstSeenLocalReducerAddition(t *testing.T) {
	registry := foldRegistry(t, sumFold)
	model, err := readmodels.Define[FoldTotal]()
	if err != nil {
		t.Fatal(err)
	}
	changes := make(chan readmodels.ChangeType, 8)
	handlers := []reactors.ReadModelHandler{
		reactors.ReadModelOn(readmodels.Added, func(*FoldTotal) { changes <- readmodels.Added }),
		reactors.ReadModelOn(readmodels.Modified, func(*FoldTotal) { changes <- readmodels.Modified }),
		reactors.ReadModelOn(readmodels.Removed, func(values []*FoldTotal) {
			if len(values) != 0 {
				t.Error("removed collection not empty")
			}
			changes <- readmodels.Removed
		}),
	}
	if err := RegisterReadModelReactorHandlers(registry, "local", model, handlers); err != nil {
		t.Fatal(err)
	}
	client, store, ctx, k, _ := reducerFixture(t, registry)
	sub, err := store.ReadModels().Watch(ctx, model.Identifier())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Close(); err != nil {
			t.Error(err)
		}
	}()
	for i, want := range []readmodels.ChangeType{readmodels.Added, readmodels.Modified, readmodels.Removed, readmodels.Added} {
		event := foldEvent("FoldChanged", uint64(i), `{"amount":1}`)
		if want == readmodels.Removed {
			event = foldEvent("FoldDeleted", uint64(i), `{}`)
		}
		k.operations <- &contracts.ReduceOperationMessage{Partition: "source", Events: []*contracts.AppendedEvent{event}}
		select {
		case result := <-k.results:
			if result.State != contracts.ObservationState_Success {
				t.Fatalf("fold: %v", result)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		select {
		case got := <-changes:
			if got != want {
				t.Fatalf("reactor kind %v want %v", got, want)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		raw, err := sub.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if want != readmodels.Removed && raw.Type != readmodels.Modified {
			t.Fatal("Watch invented Added instead of local Modified")
		}
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sub.Done():
	case <-ctx.Done():
		t.Fatal("local subscription outlived generation")
	}
}
