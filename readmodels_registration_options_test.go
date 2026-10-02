// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestReadModelRegistrationOverrides(t *testing.T) {
	registry := NewRegistry()
	_, err := RegisterReadModel[Person](registry,
		readmodels.WithIdentifier("persisted-model"), readmodels.WithContainerName("people-v2"), readmodels.WithDisplayName("People"), readmodels.WithGeneration(2),
		readmodels.WithSink(readmodels.Sink{Type: readmodels.SQL, ConfigurationID: "00112233-4455-6677-8899-aabbccddeeff"}), readmodels.WithObserver(readmodels.Reducer, "person-reducer"))
	if err != nil {
		t.Fatal(err)
	}
	requests := make(chan *contracts.RegisterManyRequest, 1)
	kernel := &supervisedKernel{readModels: &readModelKernel{register: func(_ context.Context, r *contracts.RegisterManyRequest) error { requests <- r; return nil }}}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
	if _, err = client.EventStore(ctx, "store"); err != nil {
		t.Fatal(err)
	}
	actual := (<-requests).ReadModels[0]
	if actual.Type.Identifier != "persisted-model" || actual.Type.Generation != 2 || actual.ContainerName != "people-v2" || actual.DisplayName != "People" || actual.ObserverType != contracts.ReadModelObserverType_Reducer || actual.ObserverIdentifier != "person-reducer" || actual.Sink.TypeId != "SQL" || actual.Sink.ConfigurationId.Lo != 0x6677445500112233 || actual.Sink.ConfigurationId.Hi != 0xffeeddccbbaa9988 {
		t.Fatalf("overrides: %+v", actual)
	}
}
