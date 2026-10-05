// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

type historyAdmissionEvent struct {
	Name string
	Key  string
}
type historyAdmissionModel struct {
	ID   string
	Name string
}

func TestProjectionHistoryRefusesDefaultsAndCustomKeysWithoutRPC(t *testing.T) {
	for _, defaults := range []bool{false, true} {
		r := NewRegistry()
		e, err := RegisterEvent[historyAdmissionEvent](r)
		if err != nil {
			t.Fatal(err)
		}
		m, err := RegisterReadModel[historyAdmissionModel](r)
		if err != nil {
			t.Fatal(err)
		}
		options := []projections.Option{projections.FromEvent(e)}
		if defaults {
			options = append(options, projections.WithInitialValues(historyAdmissionModel{Name: "initial"}))
		} else {
			options = []projections.Option{projections.FromEvent(e, projections.UsingKey(projections.Path[historyAdmissionEvent, string]("Key")))}
		}
		if err = r.AddProjection(projections.ModelBound(m, options...)); err != nil {
			t.Fatal(err)
		}
		client, err := NewClient(WithRegistry(r))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := client.Close(); err != nil {
				t.Error(err)
			}
		})
		store := &EventStore{storeOwner: &storeOwner{client: client, name: "store", namespace: DefaultNamespace, catalog: client.catalog}}
		if err = store.initializeReadModels(); err != nil {
			t.Fatal(err)
		}
		reader := readmodels.For(store.ReadModels(), m)
		if result, err := reader.GetAll(t.Context(), new(events.Count(1))); result.Instances != nil || !errors.Is(err, ErrUnsupported) {
			t.Fatal("unfaithful collection", err)
		}
		if result, err := reader.GetSnapshots(t.Context(), "key"); result != nil || !errors.Is(err, ErrUnsupported) {
			t.Fatal("unfaithful snapshots", err)
		}
		if result, err := reader.GetAll(t.Context(), new(events.Count(0))); err != nil || result.Instances == nil || len(result.Instances) != 0 {
			t.Fatal("zero ran producer admission", err)
		}
	}
}
