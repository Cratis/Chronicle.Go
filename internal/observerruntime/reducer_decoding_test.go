// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime

import (
	"context"
	"errors"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/observation/reducers"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

func decodingReducer(t *testing.T) *Reducer {
	t.Helper()
	model, err := readmodels.Define[Balance]()
	if err != nil {
		t.Fatal(err)
	}
	models, err := readmodels.NewCatalog(model.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	declaration, err := reducers.DefineHandlers(model, "balances", []reducers.Handler{reducers.On(func(_ context.Context, _ PageInput, current *Balance, _ events.Context) (*Balance, error) {
		return current, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := reducers.Compile(declaration, pageCatalog(t), models, nil)
	if err != nil {
		t.Fatal(err)
	}
	return &Reducer{plan: plan, store: "store", namespace: "Default"}
}

func TestReducerDecodeRejectsInvalidInitialStateBeforeEvents(t *testing.T) {
	r := decodingReducer(t)
	for _, initial := range []string{"[]", "{", `{"amount":"invalid"}`} {
		t.Run(initial, func(t *testing.T) {
			decoded := r.decodePartition(&contracts.ReducerPartition{Partition: "source", InitialState: initial, Events: []*contracts.AppendedEvent{reducerAppended("source", 1)}})
			if !errors.Is(decoded.failure, faults.ErrProtocol) || decoded.initial != nil || len(decoded.events) != 0 || decoded.partition != "source" {
				t.Fatalf("decoded = %+v", decoded)
			}
		})
	}
}

func TestReducerDecodeAcceptsEmptyAndNullInitialState(t *testing.T) {
	r := decodingReducer(t)
	for _, initial := range []string{"", "  ", " null \n"} {
		decoded := r.decodePartition(&contracts.ReducerPartition{InitialState: initial})
		if decoded.failure != nil || decoded.initial != nil {
			t.Fatalf("initial %q: %+v", initial, decoded)
		}
	}
}

func TestReducerDecodeNilPartitionFailsProtocol(t *testing.T) {
	decoded := new(Reducer).decodePartition(nil)
	if !errors.Is(decoded.failure, faults.ErrProtocol) {
		t.Fatal(decoded)
	}
}
