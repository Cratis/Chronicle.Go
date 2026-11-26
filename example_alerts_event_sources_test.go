// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"fmt"
	"net"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/alerts"
	alertcontracts "github.com/cratis/chronicle.go/contracts/alerts"
	"github.com/cratis/chronicle.go/contracts/clients"
	sourcecontracts "github.com/cratis/chronicle.go/contracts/eventsources"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/eventsources"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type sourceExampleEvent struct{ Value string }

// These examples use an in-process kernel-shaped fixture, not a running kernel.
func newAlertsSourcesExampleClient(ctx context.Context) (*chronicle.Client, func(), error) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(alertsSourcesExampleRPC))
	clients.RegisterConnectionServiceServer(server, &clients.UnimplementedConnectionServiceServer{})
	eventstores.RegisterEventStoresServer(server, &eventstores.UnimplementedEventStoresServer{})
	namespaces.RegisterNamespacesServer(server, &namespaces.UnimplementedNamespacesServer{})
	eventtypes.RegisterEventTypesServer(server, &eventtypes.UnimplementedEventTypesServer{})
	sourcecontracts.RegisterEventSourcesServer(server, &sourcecontracts.UnimplementedEventSourcesServer{})
	alertcontracts.RegisterAlertsServer(server, &alertcontracts.UnimplementedAlertsServer{})
	sequences.RegisterEventSequencesServer(server, &sequences.UnimplementedEventSequencesServer{})
	served := make(chan struct{})
	go func() { defer close(served); _ = server.Serve(listener) }()
	stop := func() { server.Stop(); _ = listener.Close(); <-served }
	conn, err := grpc.NewClient("passthrough:///alerts-sources-example", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		stop()
		return nil, nil, err
	}
	registry := chronicle.NewRegistry()
	_, err = chronicle.RegisterEvent[sourceExampleEvent](registry, events.WithID("source-example"))
	if err != nil {
		_ = conn.Close()
		stop()
		return nil, nil, err
	}
	client, err := chronicle.NewClient(chronicle.WithGRPCConnection(conn), chronicle.WithNoAuthentication(), chronicle.WithSkipKeepAlive(), chronicle.WithRegistry(registry))
	if err != nil {
		_ = conn.Close()
		stop()
		return nil, nil, err
	}
	return client, func() { _ = client.Close(); _ = conn.Close(); stop() }, nil
}

func alertsSourcesExampleRPC(_ context.Context, request any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
	switch r := request.(type) {
	case *clients.CompatibilityRequest:
		return &clients.CompatibilityResponse{IsCompatible: true}, nil
	case *emptypb.Empty:
		return &clients.IEnumerable_ConnectedClient{}, nil
	case *eventstores.EnsureEventStoreRequest:
		return &eventstores.CommandResult{IsAuthorized: true}, nil
	case *namespaces.EnsureNamespaceRequest:
		return &namespaces.CommandResult{IsAuthorized: true}, nil
	case *eventtypes.RegisterEventTypesRequest:
		return &eventtypes.CommandResult{IsAuthorized: true}, nil
	case *sourcecontracts.RegisterEventSourcesRequest:
		return &sourcecontracts.CommandResult{IsAuthorized: true}, nil
	case *sequences.AppendRequest:
		return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true, CorrelationId: r.CorrelationId}}, nil
	case *alertcontracts.GetOpenIncidentRequest:
		return &alertcontracts.QueryResult_AlertIncidentLookupResponse{IsAuthorized: true, Data: &alertcontracts.AlertIncidentLookupResponse{Status: alertcontracts.AlertIncidentsReadinessState_Ready}}, nil
	case *alertcontracts.GetOpenIncidentsRequest:
		return exampleIncidentPage(r), nil
	default:
		return nil, fmt.Errorf("unexpected example RPC %T", request)
	}
}

func exampleIncidentPage(request *alertcontracts.GetOpenIncidentsRequest) *alertcontracts.QueryResult_AlertIncidentPageResponse {
	page := &alertcontracts.AlertIncidentPageResponse{Status: alertcontracts.AlertIncidentsReadinessState_Ready}
	if request.AfterIncidentId == nil {
		id := wire.Guid(metadata.CorrelationID(uuid.MustParse("11111111-1111-1111-1111-111111111111")))
		at := &alertcontracts.SerializableDateTimeOffset{Value: "2024-01-02T03:04:05Z"}
		page.Items = []*alertcontracts.AlertIncidentDetails{{Id: id, Target: &alertcontracts.AlertTarget{EventStore: "store", Namespace: "Default", ObserverId: "observer", EventSequenceId: "event-log"}, Condition: "failing", Evidence: &alertcontracts.AlertEvidence{FirstFailure: at, LastFailure: at}, RaisedAt: at, LastChangedAt: at, RaisedSequenceNumber: 1, LastTransitionSequenceNumber: 1}}
		page.Next = &alertcontracts.AlertIncidentContinuation{RaisedSequenceNumber: 1, IncidentId: id}
	}
	return &alertcontracts.QueryResult_AlertIncidentPageResponse{IsAuthorized: true, Data: page}
}

func ExampleEventStore_Alerts() {
	ctx := context.Background()
	client, closeClient, err := newAlertsSourcesExampleClient(ctx)
	if err != nil {
		panic(err)
	}
	defer closeClient()
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		panic(err)
	}
	service := store.Alerts()
	lookup, err := service.GetOpenIncident(ctx, uuid.MustParse("11111111-1111-1111-1111-111111111111"))
	if err != nil {
		panic(err)
	}
	// A missing incident is authoritative only once readiness is Ready.
	fmt.Println("missing:", lookup.Readiness == alerts.Ready && lookup.Incident == nil)
	query, count := alerts.Query{Limit: 1}, 0
	for {
		page, err := service.GetOpenIncidents(ctx, query)
		if err != nil {
			panic(err)
		}
		if page.Readiness != alerts.Ready {
			break
		} // Catching up is not an empty result.
		count += len(page.Incidents)
		if page.Next == nil {
			break
		}
		query.After = page.Next
	}
	fmt.Println("incidents:", count)
	// Subscription is explicitly unsupported; use polling when needed.
	err = service.Subscribe(ctx, func(alerts.Incident) {})
	fmt.Println("subscription unsupported:", errors.Is(err, chronicle.ErrUnsupported))
	// Output:
	// missing: true
	// incidents: 1
	// subscription unsupported: true
}

func ExampleEventStore_EventSources() {
	ctx := context.Background()
	client, closeClient, err := newAlertsSourcesExampleClient(ctx)
	if err != nil {
		panic(err)
	}
	defer closeClient()
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		panic(err)
	}
	// Registrations describe sources/streams; they do not change append concurrency.
	err = store.EventSources().Register(ctx, eventsources.Definition{Name: "Billing", Streams: []eventsources.Stream{{Name: "changes"}}})
	if err != nil {
		panic(err)
	}
	// Attribution is metadata, separate from the explicit route and concurrency scope.
	result, err := store.EventLog().Append(ctx, "account-1", sourceExampleEvent{Value: "opened"}, eventsequences.WithEventSource("Billing"), eventsequences.WithRoute(eventsequences.Route{StreamType: "changes"}), eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	if err != nil {
		panic(err)
	}
	fmt.Println("committed:", result.Disposition == eventsequences.Committed)
	// Output: committed: true
}
