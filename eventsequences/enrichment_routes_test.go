// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/outgoing"
	"google.golang.org/grpc"
)

type enrichmentRouteConnection struct {
	grpc.ClientConnInterface
	config   outgoing.Config
	contents []string
	method   string
}

func (c *enrichmentRouteConnection) OutgoingConfiguration() outgoing.Config { return c.config }
func (c *enrichmentRouteConnection) Invoke(_ context.Context, method string, input, output any, _ ...grpc.CallOption) error {
	c.method = method
	switch request := input.(type) {
	case *sequences.AppendRequest:
		c.contents = append(c.contents, request.Content)
	case *sequences.AppendWithNamedTagsRequest:
		c.contents = append(c.contents, request.Content)
	case *sequences.AppendManyRequest:
		for _, event := range request.Events {
			c.contents = append(c.contents, event.Content)
		}
	case *sequences.AppendManyWithNamedTagsRequest:
		for _, event := range request.Events {
			c.contents = append(c.contents, event.Content)
		}
	case *sequences.AppendManyForEventSourcesRequest:
		for _, event := range request.Events {
			c.contents = append(c.contents, event.Content)
		}
	case *sequences.AppendManyForEventSourcesWithNamedTagsRequest:
		for _, event := range request.Events {
			c.contents = append(c.contents, event.Content)
		}
	}
	switch response := output.(type) {
	case *sequences.CommandResult_AppendResponse:
		response.IsAuthorized = true
		response.Response = &sequences.AppendResponse{IsSuccess: true}
	case *sequences.CommandResult_AppendManyResponse:
		positions := make([]uint64, len(c.contents))
		for i := range positions {
			positions[i] = uint64(i)
		}
		response.IsAuthorized = true
		response.Response = &sequences.AppendManyResponse{IsSuccess: true, SequenceNumbers: positions}
	}
	return nil
}

func TestEveryOutgoingAppendRouteEnrichesExactlyOnceInInputOrder(t *testing.T) {
	for _, route := range []string{"Append", "AppendWithNamedTags", "AppendMany", "AppendManyWithNamedTags", "AppendManyForEventSources", "AppendManyForEventSourcesWithNamedTags", "alternate-many", "prepared"} {
		t.Run(route, func(t *testing.T) {
			one, err := events.Define[opened]()
			if err != nil {
				t.Fatal(err)
			}
			two, err := events.Define[changed]()
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := events.NewCatalog(one.Descriptor(), two.Descriptor())
			if err != nil {
				t.Fatal(err)
			}
			var trace []string
			makeProvider := func(suffix string) events.EventEnricher {
				return func(_ context.Context, _ events.TypeRef, content *events.EventContent) error {
					raw, _, err := content.Get("value")
					if err != nil {
						return err
					}
					var value string
					if err := json.Unmarshal(raw, &value); err != nil {
						return err
					}
					trace = append(trace, value+suffix)
					return content.Set("value", value+suffix)
				}
			}
			connection := &enrichmentRouteConnection{config: outgoing.Config{Enrichers: []events.EventEnricher{makeProvider("-p1"), makeProvider("-p2")}}}
			sequence, err := eventsequences.New("store", "namespace", events.EventLog, catalog, connection)
			if err != nil {
				t.Fatal(err)
			}
			entries := []eventsequences.Entry{{Source: "A", Event: opened{"A1"}}, {Source: "B", Event: changed{"B1"}}, {Source: "A", Event: opened{"A2"}}}
			values := []any{entries[0].Event, entries[1].Event, entries[2].Event}
			appendOptions := []eventsequences.AppendOption{eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})}
			batchOptions := []eventsequences.BatchOption{eventsequences.WithScopes(eventsequences.LabeledScope{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck()}}, eventsequences.LabeledScope{Label: "B", Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck()}})}
			if strings.Contains(route, "NamedTags") {
				appendOptions = append(appendOptions, eventsequences.WithNamedTags(events.NamedTag{Name: "key", Value: "value"}))
				batchOptions = append(batchOptions, eventsequences.WithBatchNamedTags(events.NamedTag{Name: "key", Value: "value"}))
			}
			expected := route
			count := 3
			switch route {
			case "Append", "AppendWithNamedTags":
				count = 1
				_, err = sequence.Append(t.Context(), "A", values[0], appendOptions...)
			case "AppendMany", "AppendManyWithNamedTags":
				_, err = sequence.AppendMany(t.Context(), "A", values, appendOptions...)
			case "alternate-many":
				expected = "AppendManyForEventSources"
				appendOptions = append(appendOptions, eventsequences.WithRoute(eventsequences.Route{StreamID: "alternate"}))
				_, err = sequence.AppendMany(t.Context(), "A", values, appendOptions...)
			case "prepared":
				expected = "AppendManyForEventSources"
				var snapshot *eventsequences.PreparedBatch
				snapshot, err = sequence.PrepareBatch(t.Context(), entries, batchOptions...)
				if err == nil {
					_, err = sequence.AppendPreparedBatch(t.Context(), snapshot)
				}
			default:
				_, err = sequence.AppendBatch(t.Context(), entries, batchOptions...)
			}
			if err != nil {
				t.Fatal(err)
			}
			wantTrace := []string{"A1-p1", "A1-p1-p2", "B1-p1", "B1-p1-p2", "A2-p1", "A2-p1-p2"}
			wantContent := []string{`{"value":"A1-p1-p2"}`, `{"value":"B1-p1-p2"}`, `{"value":"A2-p1-p2"}`}
			if !reflect.DeepEqual(trace, wantTrace[:count*2]) || !reflect.DeepEqual(connection.contents, wantContent[:count]) || !strings.HasSuffix(connection.method, "/"+expected) {
				t.Fatal(trace, connection.contents, connection.method)
			}
		})
	}
}
