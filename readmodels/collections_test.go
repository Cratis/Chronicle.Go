// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/compliance"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
)

func allowReplay(context.Context, readmodels.Descriptor) (bool, error) { return true, nil }

func TestCollectionCountsRoutingAndReportedProgress(t *testing.T) {
	for _, kind := range []readmodels.ObserverType{readmodels.Projection, readmodels.Reducer} {
		for _, passive := range []bool{false, true} {
			t.Run(string(rune('0'+kind))+map[bool]string{false: "active", true: "passive"}[passive], func(t *testing.T) {
				options := []readmodels.ModelOption{readmodels.WithObserver(kind, "producer"), readmodels.WithEventSequence("inbox-origin")}
				if passive {
					options = append(options, readmodels.WithSink(readmodels.Sink{Type: readmodels.NoSink}))
				}
				model := person(t, options...)
				rpc, fold := 0, 0
				var want events.Count
				kernel := &modelKernel{options: []readmodels.ServiceOption{
					readmodels.WithProjectionReplayValidator(allowReplay),
					readmodels.WithReducerCollectionReader(func(_ context.Context, d readmodels.Descriptor, count events.Count) (readmodels.Collection[json.RawMessage], error) {
						fold++
						if count != want || d.EventSequence() != "inbox-origin" {
							t.Error("fold coordinates/count")
						}
						position := events.SequenceNumber(97)
						return readmodels.Collection[json.RawMessage]{Instances: []readmodels.Instance[json.RawMessage]{{Value: json.RawMessage(`{"id":"one"}`), Exists: true, LastHandled: &position}}, ProcessedEventsCount: 1}, nil
					}),
				}, replay: func(_ context.Context, r *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
					rpc++
					if r.EventStore != "store" || r.Namespace != "tenant-a" || r.EventSequenceId != "inbox-origin" || r.ReadModelIdentifier != string(model.Identifier()) || r.EventCount != uint64(want) {
						t.Error("RPC coordinates/count")
					}
					return &contracts.GetAllInstancesResponse{Instances: []string{`{"_id":"one","__lastHandledEventSequenceNumber":97}`}, ProcessedEventsCount: 1}, nil
				}}
				service, ctx := serviceFixture(t, kernel, model.Descriptor())
				reader := readmodels.For(service, model)
				for _, count := range []*events.Count{nil, new(events.UnlimitedCount), new(events.Count(1)), new(events.Count(math.MaxInt32))} {
					want = events.UnlimitedCount
					if count != nil {
						want = *count
					}
					rpc, fold = 0, 0
					result, err := reader.GetAll(ctx, count)
					local := kind == readmodels.Reducer && (count != nil || passive)
					if err != nil || len(result.Instances) != 1 || result.ProcessedEventsCount != 1 || result.Instances[0].Value.ID != "one" || result.Instances[0].LastHandled == nil || *result.Instances[0].LastHandled != 97 || fold != map[bool]int{true: 1}[local] || rpc != map[bool]int{false: 1}[local] {
						t.Fatalf("collection=%+v err=%v rpc=%d fold=%d", result, err, rpc, fold)
					}
				}
				rpc, fold = 0, 0
				for _, count := range []events.Count{0, math.MaxInt32 + 1, math.MaxUint64 - 1} {
					result, err := reader.GetAll(ctx, &count)
					if count == 0 {
						if err != nil || result.Instances == nil || len(result.Instances) != 0 {
							t.Fatal("zero not empty")
						}
					} else if !errors.Is(err, chronicle.ErrInvalidConfiguration) || result.Instances != nil {
						t.Fatal("overflow not refused")
					}
				}
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				if _, err := reader.GetAll(canceled, new(events.Count(0))); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				if rpc != 0 || fold != 0 {
					t.Fatal("zero/invalid/canceled read performed work")
				}
			})
		}
	}
}

func TestCollectionRejectsLateErrorsWithoutPartialProgress(t *testing.T) {
	model := person(t)
	for _, document := range []string{`null`, `[]`, `invalid PRIVATE`, `{"__lastHandledEventSequenceNumber":18446744073709551614}`, `{"__lastHandledEventSequenceNumber":18446744073709551613}`, `{"__lastHandledEventSequenceNumber":null}`, `{"count":"PRIVATE"}`} {
		t.Run(document, func(t *testing.T) {
			service, ctx := serviceFixture(t, &modelKernel{replay: func(context.Context, *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
				return &contracts.GetAllInstancesResponse{Instances: []string{`{"id":"one"}`, document}, ProcessedEventsCount: 123}, nil
			}}, model.Descriptor())
			result, err := readmodels.For(service, model).GetAll(ctx, nil)
			if err == nil || result.Instances != nil || result.ProcessedEventsCount != 0 || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatalf("result=%+v error=%v", result, err)
			}
		})
	}
}

func TestCollectionServerReleaseIsNotRepeated(t *testing.T) {
	model := person(t, readmodels.WithPII("name"))
	service, ctx := serviceFixture(t, &modelKernel{
		replay: func(context.Context, *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
			return &contracts.GetAllInstancesResponse{Instances: []string{`{"id":"owner","name":"plaintext"}`}}, nil
		},
		release: func(context.Context, *compliance.ReleaseRequest) (*compliance.ReleaseResponse, error) {
			t.Error("double decryption")
			return nil, errors.New("unexpected release")
		},
	}, model.Descriptor())
	result, err := readmodels.For(service, model).GetAll(ctx, nil)
	if err != nil || len(result.Instances) != 1 || result.Instances[0].Value.Name != "plaintext" || result.Instances[0].LastHandled != nil || result.ProcessedEventsCount != 0 {
		t.Fatalf("result=%+v error=%v", result, err)
	}
}

func TestCollectionEmptyAndUnavailableProgress(t *testing.T) {
	model := person(t)
	calls := 0
	service, ctx := serviceFixture(t, &modelKernel{replay: func(context.Context, *contracts.GetAllInstancesRequest) (*contracts.GetAllInstancesResponse, error) {
		calls++
		if calls == 1 {
			return &contracts.GetAllInstancesResponse{}, nil
		}
		return &contracts.GetAllInstancesResponse{Instances: []string{`{"__lastHandledEventSequenceNumber":18446744073709551615}`}}, nil
	}}, model.Descriptor())
	empty, err := service.GetAll(ctx, model.Identifier(), nil)
	if err != nil || empty.Instances == nil || len(empty.Instances) != 0 {
		t.Fatal("empty collection", err)
	}
	present, err := readmodels.For(service, model).GetAll(ctx, nil)
	if err != nil || len(present.Instances) != 1 || !present.Instances[0].Exists || present.Instances[0].LastHandled != nil {
		t.Fatal("present zero model", err)
	}
}

func TestCollectionUnknownReplayFidelityAndMissingLocalReducerRefused(t *testing.T) {
	for _, kind := range []readmodels.ObserverType{readmodels.Projection, readmodels.Reducer} {
		model := person(t, readmodels.WithObserver(kind, "remote"))
		service, ctx := serviceFixture(t, &modelKernel{}, model.Descriptor())
		result, err := service.GetAll(ctx, model.Identifier(), new(events.Count(1)))
		if !errors.Is(err, chronicle.ErrUnsupported) || result.Instances != nil {
			t.Fatalf("result=%+v error=%v", result, err)
		}
	}
}
