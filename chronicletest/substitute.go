// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cratis/chronicle.go/contracts/clients"
	constraintcontracts "github.com/cratis/chronicle.go/contracts/events/constraints"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	projectioncontracts "github.com/cratis/chronicle.go/contracts/projections"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type sequenceKey struct{ store, namespace, sequence string }
type substituteKernel struct {
	clients.UnimplementedConnectionServiceServer
	eventstores.UnimplementedEventStoresServer
	eventtypes.UnimplementedEventTypesServer
	namespaces.UnimplementedNamespacesServer
	sequences.UnimplementedEventSequencesServer
	constraintcontracts.UnimplementedConstraintsServer
	mu     sync.Mutex
	events map[sequenceKey][]*sequences.AppendedEventResponse
}

func substituteConnection() (*grpc.ClientConn, func() error, error) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	kernel := &substituteKernel{events: map[sequenceKey][]*sequences.AppendedEventResponse{}}
	clients.RegisterConnectionServiceServer(server, kernel)
	eventstores.RegisterEventStoresServer(server, kernel)
	eventtypes.RegisterEventTypesServer(server, kernel)
	namespaces.RegisterNamespacesServer(server, kernel)
	sequences.RegisterEventSequencesServer(server, kernel)
	constraintcontracts.RegisterConstraintsServer(server, kernel)
	modelcontracts.RegisterReadModelsServer(server, &substituteModels{})
	projectioncontracts.RegisterProjectionsServer(server, &substituteProjections{})
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	conn, err := grpc.NewClient("passthrough:///chronicle-scenario", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	closeTransport := func() error {
		var closeErr error
		if conn != nil {
			closeErr = conn.Close()
		}
		server.Stop()
		listenerErr := listener.Close()
		serveErr := <-done
		if errors.Is(serveErr, grpc.ErrServerStopped) {
			serveErr = nil
		}
		return errors.Join(closeErr, listenerErr, serveErr)
	}
	if err != nil {
		return nil, nil, errors.Join(err, closeTransport())
	}
	return conn, closeTransport, nil
}
func unsupported(layer string) error {
	return status.Error(codes.Unimplemented, "substituted scenario does not implement "+layer+"; use a real kernel")
}
func (*substituteKernel) CheckCompatibility(context.Context, *clients.CompatibilityRequest) (*clients.CompatibilityResponse, error) {
	return &clients.CompatibilityResponse{IsCompatible: true, ServerVersion: "substituted-scenario-not-a-kernel"}, nil
}
func (*substituteKernel) Connect(request *clients.ConnectRequest, stream grpc.ServerStreamingServer[clients.ConnectionKeepAlive]) error {
	if err := stream.Send(&clients.ConnectionKeepAlive{ConnectionId: request.ConnectionId}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
func (*substituteKernel) ConnectionKeepAlive(context.Context, *clients.ConnectionKeepAlive) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
func (*substituteKernel) EnsureEventStore(context.Context, *eventstores.EnsureEventStoreRequest) (*eventstores.CommandResult, error) {
	return &eventstores.CommandResult{IsAuthorized: true}, nil
}
func (*substituteKernel) EnsureNamespace(context.Context, *namespaces.EnsureNamespaceRequest) (*namespaces.CommandResult, error) {
	return &namespaces.CommandResult{IsAuthorized: true}, nil
}
func (*substituteKernel) RegisterEventTypes(context.Context, *eventtypes.RegisterEventTypesRequest) (*eventtypes.CommandResult, error) {
	return &eventtypes.CommandResult{IsAuthorized: true}, nil
}
func (*substituteKernel) Register(_ context.Context, r *constraintcontracts.RegisterConstraintsRequest) (*emptypb.Empty, error) {
	if len(r.Constraints) != 0 {
		return nil, unsupported("constraint enforcement")
	}
	return &emptypb.Empty{}, nil
}

type substituteModels struct {
	modelcontracts.UnimplementedReadModelsServer
}

func (*substituteModels) RegisterMany(_ context.Context, r *modelcontracts.RegisterManyRequest) (*emptypb.Empty, error) {
	if len(r.ReadModels) != 0 {
		return nil, unsupported("read-model registration")
	}
	return &emptypb.Empty{}, nil
}

type substituteProjections struct {
	projectioncontracts.UnimplementedProjectionsServer
}

func (*substituteProjections) Register(_ context.Context, r *projectioncontracts.RegisterRequest) (*emptypb.Empty, error) {
	if len(r.Projections) != 0 {
		return nil, unsupported("projection execution")
	}
	return &emptypb.Empty{}, nil
}
func checkScope(scope *sequences.ConcurrencyScope) error {
	if scope != nil && (scope.ExpectsNoMatchingEvent || scope.SequenceNumber != ^uint64(0)) {
		return unsupported("concurrency enforcement")
	}
	return nil
}
func (k *substituteKernel) append(r *sequences.AppendRequest) uint64 {
	key := sequenceKey{r.EventStore, r.Namespace, r.EventSequenceId}
	position := uint64(len(k.events[key]))
	occurred := r.Occurred
	if occurred == nil || occurred.Value == "" {
		occurred = &sequences.SerializableDateTimeOffset{Value: time.Unix(0, int64(position)).UTC().Format(time.RFC3339Nano)}
	}
	event := &sequences.AppendedEventResponse{Id: strconv.FormatUint(position, 10), Content: r.Content, OriginalContent: r.Content,
		Context: &sequences.EventContext{EventType: r.EventType, EventSourceId: r.EventSourceId, EventSourceType: r.EventSourceType, EventStreamType: r.EventStreamType, EventStreamId: r.EventStreamId,
			SequenceNumber: position, Occurred: occurred, CorrelationId: r.CorrelationId, Causation: r.Causation, CausedBy: r.CausedBy, Tags: r.Tags, Subject: r.Subject}}
	k.events[key] = append(k.events[key], proto.Clone(event).(*sequences.AppendedEventResponse))
	return position
}
func (k *substituteKernel) Append(_ context.Context, r *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
	if err := checkScope(r.ConcurrencyScope); err != nil {
		return nil, err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	position := k.append(r)
	return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true, CorrelationId: r.CorrelationId, SequenceNumber: position}}, nil
}
func (k *substituteKernel) AppendMany(_ context.Context, r *sequences.AppendManyRequest) (*sequences.CommandResult_AppendManyResponse, error) {
	if err := checkScope(r.ConcurrencyScope); err != nil {
		return nil, err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	positions := make([]uint64, 0, len(r.Events))
	for _, event := range r.Events {
		positions = append(positions, k.append(&sequences.AppendRequest{EventStore: r.EventStore, Namespace: r.Namespace, EventSequenceId: r.EventSequenceId, EventSourceId: r.EventSourceId,
			EventType: event.EventType, Content: event.Content, Subject: event.Subject, Occurred: r.Occurred, CorrelationId: r.CorrelationId, Tags: r.Tags, Causation: r.Causation, CausedBy: r.CausedBy, EventSourceType: "Default", EventStreamType: "All", EventStreamId: "Default"}))
	}
	return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{IsSuccess: true, CorrelationId: r.CorrelationId, SequenceNumbers: positions}}, nil
}
func (k *substituteKernel) selectEvents(key sequenceKey, source, sourceType, streamType, streamID string, types string, from uint64) []*sequences.AppendedEventResponse {
	k.mu.Lock()
	defer k.mu.Unlock()
	var result []*sequences.AppendedEventResponse
	for _, event := range k.events[key] {
		c := event.Context
		if c.SequenceNumber < from || source != "" && source != c.EventSourceId || sourceType != "" && sourceType != c.EventSourceType || streamType != "" && streamType != "All" && streamType != c.EventStreamType || streamID != "" && streamID != c.EventStreamId || len(types) > 0 && !slices.Contains(strings.Split(types, ","), c.EventType.Id) {
			continue
		}
		result = append(result, proto.Clone(event).(*sequences.AppendedEventResponse))
	}
	return result
}
func (k *substituteKernel) ForEventSourceIdAndEventTypes(_ context.Context, r *sequences.ForEventSourceIdAndEventTypesRequest) (*sequences.QueryResult_IEnumerable_AppendedEventResponse, error) {
	return &sequences.QueryResult_IEnumerable_AppendedEventResponse{IsAuthorized: true, Data: k.selectEvents(sequenceKey{r.EventStore, r.Namespace, r.EventSequenceId}, r.EventSourceId, r.EventSourceType, r.EventStreamType, r.EventStreamId, r.EventTypeIds, 0)}, nil
}
func (k *substituteKernel) FromSequenceNumber(_ context.Context, r *sequences.FromSequenceNumberRequest) (*sequences.QueryResult_IEnumerable_AppendedEventResponse, error) {
	return &sequences.QueryResult_IEnumerable_AppendedEventResponse{IsAuthorized: true, Data: k.selectEvents(sequenceKey{r.EventStore, r.Namespace, r.EventSequenceId}, r.EventSourceId, "", "", "", r.EventTypeIds, r.FromEventSequenceNumber)}, nil
}
func (k *substituteKernel) TailSequenceNumber(_ context.Context, r *sequences.TailSequenceNumberRequest) (*sequences.QueryResult_EventSequenceTailResponse, error) {
	loaded := k.selectEvents(sequenceKey{r.EventStore, r.Namespace, r.EventSequenceId}, r.EventSourceId, r.EventSourceType, r.EventStreamType, r.EventStreamId, r.EventTypeIds, 0)
	position := ^uint64(0)
	if len(loaded) > 0 {
		position = loaded[len(loaded)-1].Context.SequenceNumber
	}
	return &sequences.QueryResult_EventSequenceTailResponse{IsAuthorized: true, Data: &sequences.EventSequenceTailResponse{SequenceNumber: position}}, nil
}
func (k *substituteKernel) HasEventsForEventSourceId(_ context.Context, r *sequences.HasEventsForEventSourceIdRequest) (*sequences.QueryResult_EventSourceEventsResponse, error) {
	loaded := k.selectEvents(sequenceKey{r.EventStore, r.Namespace, r.EventSequenceId}, r.EventSourceId, "", "", "", "", 0)
	return &sequences.QueryResult_EventSourceEventsResponse{IsAuthorized: true, Data: &sequences.EventSourceEventsResponse{HasEvents: len(loaded) > 0}}, nil
}
