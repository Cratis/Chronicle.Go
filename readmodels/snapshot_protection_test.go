// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"errors"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/readmodelexplorer"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
)

type protectedHistoryNumber struct {
	Number int `json:"number" chronicle:"pii"`
}

func TestSnapshotsValidateKnownProtectedContributionsWithoutSecondRelease(t *testing.T) {
	model := person(t, readmodels.WithObserver(readmodels.Projection, "projection"))
	event, err := events.Define[protectedHistoryNumber]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	for _, content := range []string{`{"number":42}`, `{"number":"PRIVATE"}`} {
		contribution := historyContribution()
		contribution.Context.EventType = &contracts.EventType{Id: string(event.Ref().ID), Generation: 1}
		contribution.Content = content
		service, ctx := serviceFixture(t, &modelKernel{options: []readmodels.ServiceOption{readmodels.WithProjectionReplayValidator(allowReplay), readmodels.WithSnapshotEventCatalog(catalog)}, snapshots: func(context.Context, *contracts.AllSnapshotsForReadModelRequest) (*contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse, error) {
			return &contracts.QueryResult_IEnumerable_ReadModelSnapshotResponse{IsAuthorized: true, Data: []*contracts.ReadModelSnapshotResponse{
				{Instance: `{}`, Occurred: historyTime()},
				{Instance: `{}`, Occurred: historyTime(), Events: []*contracts.Event{contribution}},
			}}, nil
		}}, model.Descriptor())
		result, err := service.GetSnapshots(ctx, model.Identifier(), "owner")
		if content == `{"number":42}` {
			if err != nil || len(result) != 2 {
				t.Fatal("plaintext contribution rejected", err)
			}
		} else if !errors.Is(err, readmodels.ErrRelease) || result != nil {
			t.Fatal("protected contribution or partial history escaped", err)
		}
	}
}
