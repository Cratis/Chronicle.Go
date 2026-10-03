// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
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

type collectionHistoryTransport struct {
	grpc.ClientConnInterface
	history   []*sequences.AppendedEventResponse
	calls     int
	namespace Namespace
}

func (p *collectionHistoryTransport) Invoke(_ context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	p.calls++
	r, ok := args.(*sequences.FromSequenceNumberRequest)
	if !ok || method != sequences.EventSequences_FromSequenceNumber_FullMethodName || r.EventStore != "store" || r.Namespace != string(p.namespace) || r.EventSequenceId != "event-log" || r.FromEventSequenceNumber != 0 || r.EventSourceId != "" || r.EventTypeIds != "FoldChanged,FoldDeleted" {
		return errors.New("unexpected collection history coordinates or filters")
	}
	proto.Merge(reply.(proto.Message), &sequences.QueryResult_IEnumerable_AppendedEventResponse{IsAuthorized: true, Data: p.history})
	return nil
}

type latestFoldChanged struct{ Value int }

func collectionEvent(source string, position uint64, amount int, deleted bool) *sequences.AppendedEventResponse {
	e := &sequences.AppendedEventResponse{Content: `{"Value":999}`, Context: &sequences.EventContext{EventType: &sequences.EventType{Id: "FoldChanged", Generation: 2}, EventSourceId: source, SequenceNumber: position, Occurred: &sequences.SerializableDateTimeOffset{Value: "2026-01-01T00:00:00Z"}}, GenerationalContent: []*sequences.KeyValuePair_Int32_String{{Key: 1, Value: fmt.Sprintf(`{"amount":%d}`, amount)}}}
	if deleted {
		e.Context.EventType = &sequences.EventType{Id: "FoldDeleted", Generation: 1}
		e.Content = `{}`
		e.GenerationalContent = nil
	}
	return e
}

func TestReducerCollectionsGloballyBoundHistoricalFoldsAndKeepActualPositions(t *testing.T) {
	for _, passive := range []bool{true, false} {
		for _, namespace := range []Namespace{"tenant-a", "tenant-b"} {
			r := NewRegistry()
			current, err := RegisterEvent[latestFoldChanged](r, events.WithID("FoldChanged"), events.WithGeneration(2))
			if err != nil {
				t.Fatal(err)
			}
			if _, err = RegisterEventGeneration[FoldChanged](r, current, 1); err != nil {
				t.Fatal(err)
			}
			if _, err = RegisterEvent[FoldDeleted](r); err != nil {
				t.Fatal(err)
			}
			m, err := RegisterReadModel[FoldTotal](r)
			if err != nil {
				t.Fatal(err)
			}
			var visited []string
			identity := identities.Identity{Subject: "reader"}
			failure := errors.New("PRIVATE fold failure")
			fail := false
			var options []reducers.Option
			if passive {
				options = append(options, reducers.Passive())
			}
			err = RegisterReducerHandlers(r, m, "fold", []reducers.Handler{
				reducers.On(func(ctx context.Context, e FoldChanged, state *FoldTotal, ec events.Context) (*FoldTotal, error) {
					if metadata.Identity(ctx).Subject != identity.Subject || ec.EventType.Generation != 1 || ec.Store != "store" || ec.Namespace != namespace || ec.Sequence != events.EventLog {
						return nil, errors.New("lost caller identity, historical generation or coordinates")
					}
					batch, ok := reducers.BatchFromContext(ctx)
					if !ok || batch.Store != "store" || batch.Namespace != namespace || batch.Sequence != events.EventLog {
						return nil, errors.New("lost batch")
					}
					visited = append(visited, string(ec.SourceID))
					if fail && ec.SourceID == "b" {
						return nil, failure
					}
					if state == nil {
						state = &FoldTotal{ID: string(ec.SourceID)}
					}
					state.Amount += e.Amount
					return state, nil
				}),
				reducers.On(func(context.Context, FoldDeleted, *FoldTotal, events.Context) (*FoldTotal, error) { return nil, nil }),
			}, options...)
			if err != nil {
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
			transport := &collectionHistoryTransport{namespace: namespace, history: []*sequences.AppendedEventResponse{
				collectionEvent("a", 90, 3, false), collectionEvent("b", 11, 0, false), collectionEvent("a", 4, 1, false), collectionEvent("a", 100, 0, true), collectionEvent("c", 150, 0, false),
			}}
			store := &EventStore{client: client, name: "store", namespace: namespace, catalog: client.catalog}
			store.log, err = eventsequences.New(store.name, store.namespace, events.EventLog, store.catalog, transport)
			if err != nil {
				t.Fatal(err)
			}
			if err = store.initializeReadModels(); err != nil {
				t.Fatal(err)
			}
			reader := readmodels.For(store.ReadModels(), m)
			ctx := metadata.WithIdentity(t.Context(), identity)
			zero, err := reader.GetAll(ctx, new(events.Count(0)))
			if err != nil || zero.Instances == nil || transport.calls != 0 || visited != nil {
				t.Fatal("zero activated or dispatched")
			}
			bounded, err := reader.GetAll(ctx, new(events.Count(3)))
			if err != nil || len(bounded.Instances) != 2 || bounded.ProcessedEventsCount != 3 || bounded.Instances[0].Value != (FoldTotal{ID: "a", Amount: 4}) || *bounded.Instances[0].LastHandled != 90 || bounded.Instances[1].Value != (FoldTotal{ID: "b"}) || *bounded.Instances[1].LastHandled != 11 || !reflect.DeepEqual(visited, []string{"a", "a", "b"}) {
				t.Fatalf("bounded=%+v visited=%v err=%v", bounded, visited, err)
			}
			unlimited, err := reader.GetAll(ctx, new(events.UnlimitedCount))
			if err != nil || len(unlimited.Instances) != 2 || unlimited.ProcessedEventsCount != 5 || unlimited.Instances[0].Value.ID != "b" || unlimited.Instances[1].Value.ID != "c" || !unlimited.Instances[1].Exists {
				t.Fatalf("deleted/zero=%+v err=%v", unlimited, err)
			}
			fail = true
			result, err := reader.GetAll(ctx, new(events.UnlimitedCount))
			if !errors.Is(err, failure) || result.Instances != nil || result.ProcessedEventsCount != 0 || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatal("late error exposed partial data", err)
			}
			fail = false
			transport.history = append(transport.history, collectionEvent("a", 180, 9, false))
			result, err = reader.GetAll(ctx, new(events.UnlimitedCount))
			if err != nil || len(result.Instances) != 3 || result.Instances[0].Value.Amount != 9 || *result.Instances[0].LastHandled != 180 {
				t.Fatal("recreation", err)
			}
			transport.history = append(transport.history, collectionEvent("a", 180, 9, false))
			if result, err = reader.GetAll(ctx, new(events.UnlimitedCount)); !errors.Is(err, ErrProtocol) || result.Instances != nil {
				t.Fatal("duplicate accepted", err)
			}
			transport.history = nil
			visited = nil
			result, err = reader.GetAll(ctx, new(events.UnlimitedCount))
			if err != nil || result.Instances == nil || len(result.Instances) != 0 || visited != nil {
				t.Fatal("empty activated", err)
			}
		}
	}
}
