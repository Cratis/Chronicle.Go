// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/readmodelexplorer"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestSnapshotEventsPreserveEventSourceAttribution(t *testing.T) {
	for _, name := range []string{"", "Billing"} {
		t.Run(name, func(t *testing.T) {
			model := person(t, readmodels.WithObserver(readmodels.Projection, "projection"))
			event := historyContribution()
			event.Context.EventSource, event.Context.EventSourceType = name, "Explicit"
			service, ctx := serviceFixture(t, &modelKernel{
				options: []readmodels.ServiceOption{readmodels.WithProjectionReplayValidator(allowReplay)},
				snapshots: func(context.Context, *contracts.AllSnapshotsForReadModelRequest) (*contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse, error) {
					return &contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse{IsAuthorized: true, Data: []*contracts.ReadModelSnapshotResponse{{Instance: `{"_id":"owner","name":"Ada"}`, Occurred: historyTime(), Events: []*contracts.Event{event}}}}, nil
				},
			}, model.Descriptor())
			result, err := readmodels.For(service, model).GetSnapshots(ctx, "owner")
			if err != nil {
				t.Fatal(err)
			}
			if len(result) != 1 {
				t.Fatal(result)
			}
			if len(result[0].Events) != 1 {
				t.Fatal(result[0].Events)
			}
			c := result[0].Events[0].Context
			if string(c.EventSource) != name {
				t.Fatal(c)
			}
			if c.SourceType != "Explicit" || c.SourceID != "owner" {
				t.Fatal(c)
			}
		})
	}
}
