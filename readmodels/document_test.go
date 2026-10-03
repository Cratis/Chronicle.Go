// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	compliance "github.com/cratis/chronicle.go/contracts/compliance"
	explorer "github.com/cratis/chronicle.go/contracts/readmodelexplorer"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestReadDocumentsRejectDuplicateMembersBeforePublication(t *testing.T) {
	model := person(t, readmodels.WithObserver(readmodels.Projection, "projection"))
	for _, document := range []string{
		`{"count":"PRIVATE","count":1}`,
		`{"unknown":{"count":"PRIVATE","count":1}}`,
		`{"unknown":[{"count":"PRIVATE","count":1}]}`,
		`{"count":"PRIVATE","\u0063ount":1}`,
	} {
		for _, route := range []string{"collection-raw", "collection-typed", "snapshot-raw", "snapshot-typed", "contribution", "release-input", "release-output"} {
			t.Run(route+document, func(t *testing.T) {
				kernel := &modelKernel{options: []readmodels.ServiceOption{readmodels.WithProjectionReplayValidator(allowReplay)},
					replay: func(context.Context, *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
						return &contracts.GetAllInstancesResponse{Instances: []string{`{}`, document}, ProcessedEventsCount: 2}, nil
					},
					snapshots: func(context.Context, *explorer.AllSnapshotsForReadModelRequest) (*explorer.QueryResult_IEnumerable_ReadModelSnapshotResponse, error) {
						last := &explorer.ReadModelSnapshotResponse{Instance: document, Occurred: historyTime()}
						if route == "contribution" {
							last.Instance = `{}`
							e := historyContribution()
							e.Content = document
							last.Events = []*explorer.Event{e}
						}
						return &explorer.QueryResult_IEnumerable_ReadModelSnapshotResponse{IsAuthorized: true, Data: []*explorer.ReadModelSnapshotResponse{{Instance: `{}`, Occurred: historyTime()}, last}}, nil
					},
					release: func(context.Context, *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
						if route != "release-output" {
							t.Error("duplicate input reached release RPC")
						}
						return &compliance.ReleaseResponse{Payload: document}, nil
					},
				}
				declaration := model
				if route == "release-output" {
					declaration = person(t, readmodels.WithPII("count"))
				}
				service, ctx := serviceFixture(t, kernel, declaration.Descriptor())
				reader := readmodels.For(service, declaration)
				var err error
				switch route {
				case "collection-raw":
					v, e := service.GetAll(ctx, declaration.Identifier(), nil)
					err = e
					if v.Instances != nil || v.ProcessedEventsCount != 0 {
						t.Fatal("partial collection")
					}
				case "collection-typed":
					v, e := reader.GetAll(ctx, nil)
					err = e
					if v.Instances != nil || v.ProcessedEventsCount != 0 {
						t.Fatal("partial collection")
					}
				case "snapshot-typed":
					v, e := reader.GetSnapshots(ctx, "owner")
					err = e
					if v != nil {
						t.Fatal("partial history")
					}
				case "snapshot-raw", "contribution":
					v, e := service.GetSnapshots(ctx, declaration.Identifier(), "owner")
					err = e
					if v != nil {
						t.Fatal("partial history")
					}
				default:
					input := document
					if route == "release-output" {
						input = `{"id":"owner","count":"cipher"}`
					}
					v, e := service.Release(ctx, declaration.Identifier(), json.RawMessage(input))
					err = e
					if v != nil {
						t.Fatal("partial release")
					}
				}
				if err == nil || strings.Contains(err.Error(), "PRIVATE") {
					t.Fatal("unsafe error", err)
				}
			})
		}
	}
}

func TestReadDocumentsPreserveOrdinaryUnknownProperties(t *testing.T) {
	model := person(t)
	document := `{"unknown":[{"name":1},{"name":2}],"count":1}`
	service, ctx := serviceFixture(t, &modelKernel{replay: func(context.Context, *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
		return &contracts.GetAllInstancesResponse{Instances: []string{document}}, nil
	}}, model.Descriptor())
	result, err := service.GetAll(ctx, model.Identifier(), nil)
	if err != nil || len(result.Instances) != 1 || string(result.Instances[0].Value) != document {
		t.Fatal("unknown fields changed", err)
	}
}
