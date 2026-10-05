// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	explorer "github.com/cratis/chronicle.go/contracts/readmodelexplorer"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
)

type historyFailureConcept string

func (v historyFailureConcept) ConceptValue() string         { return string(v) }
func (v historyFailureConcept) MarshalJSON() ([]byte, error) { return json.Marshal(string(v)) }
func (v historyFailureConcept) MarshalText() ([]byte, error) { return []byte(v), nil }
func (v *historyFailureConcept) UnmarshalText(b []byte) error {
	*v = historyFailureConcept(b)
	return nil
}
func (v *historyFailureConcept) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case `"panic"`:
		panic("PRIVATE panic payload")
	case `"sentinel"`:
		return errCodecSentinel
	case `"typed"`:
		return &typedCodecFailure{}
	}
	return json.Unmarshal(data, (*string)(v))
}

type historyFailureModel struct {
	ID    string                `json:"id"`
	Value historyFailureConcept `json:"value"`
}
type historyFailureEvent struct {
	Value historyFailureConcept `json:"value" chronicle:"pii"`
}

func TestHistoryCodecFailuresArePayloadFreeAndAtomic(t *testing.T) {
	for _, failure := range []string{"panic", "sentinel", "typed"} {
		for _, route := range []string{"collection", "snapshot", "protected-collection", "contribution"} {
			t.Run(route+"/"+failure, func(t *testing.T) {
				options := []readmodels.ModelOption{readmodels.WithObserver(readmodels.Projection, "history")}
				if route == "protected-collection" {
					options = append(options, readmodels.WithPII("value"))
				}
				model, err := readmodels.Define[historyFailureModel](options...)
				if err != nil {
					t.Fatal(err)
				}
				event, err := events.Define[historyFailureEvent]()
				if err != nil {
					t.Fatal(err)
				}
				catalog, err := events.NewCatalog(event.Descriptor())
				if err != nil {
					t.Fatal(err)
				}
				bad := `{"id":"owner","value":"` + failure + `"}`
				service, ctx := serviceFixture(t, &modelKernel{
					options: []readmodels.ServiceOption{readmodels.WithProjectionReplayValidator(allowReplay), readmodels.WithSnapshotEventCatalog(catalog)},
					replay: func(context.Context, *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
						return &contracts.GetAllInstancesResponse{Instances: []string{`{"value":"ok"}`, bad}, ProcessedEventsCount: 2}, nil
					},
					snapshots: func(context.Context, *explorer.AllSnapshotsForReadModelRequest) (*explorer.QueryResult_IEnumerable_ReadModelSnapshotResponse, error) {
						last := &explorer.ReadModelSnapshotResponse{Instance: bad, Occurred: historyTime()}
						if route == "contribution" {
							last.Instance = `{}`
							e := historyContribution()
							e.Content = bad
							e.Context.EventType = &explorer.EventType{Id: string(event.Ref().ID), Generation: uint32(event.Ref().Generation)}
							last.Events = []*explorer.Event{e}
						}
						return &explorer.QueryResult_IEnumerable_ReadModelSnapshotResponse{IsAuthorized: true, Data: []*explorer.ReadModelSnapshotResponse{{Instance: `{}`, Occurred: historyTime()}, last}}, nil
					},
				}, model.Descriptor())
				reader := readmodels.For(service, model)
				if route == "collection" || route == "protected-collection" {
					result, failure := reader.GetAll(ctx, nil)
					err = failure
					if result.Instances != nil || result.ProcessedEventsCount != 0 {
						t.Fatal("partial collection")
					}
				} else {
					result, failure := reader.GetSnapshots(ctx, "owner")
					err = failure
					if result != nil {
						t.Fatal("partial snapshots")
					}
				}
				var panicError *readmodels.CodecPanicError
				var typed *typedCodecFailure
				if err == nil || strings.Contains(strings.ToLower(err.Error()), "private") || errors.As(err, &panicError) != (failure == "panic") || errors.Is(err, errCodecSentinel) != (failure == "sentinel") || errors.As(err, &typed) != (failure == "typed") {
					t.Fatal("unsafe codec failure", err)
				}
			})
		}
	}
}
