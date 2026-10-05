// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/clients"
	constraintcontracts "github.com/cratis/chronicle.go/contracts/events/constraints"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type fakeKernel struct {
	clients.UnimplementedConnectionServiceServer
	eventstores.UnimplementedEventStoresServer
	eventtypes.UnimplementedEventTypesServer
	namespaces.UnimplementedNamespacesServer
	sequences.UnimplementedEventSequencesServer
	constraintcontracts.UnimplementedConstraintsServer
	incompatible     bool
	registrations    atomic.Int32
	failRegistration atomic.Bool
	appendCalls      atomic.Int32
	connectCalls     atomic.Int32
	register         func(*eventtypes.RegisterEventTypesRequest)
	appendMany       func(*sequences.AppendManyRequest) *sequences.CommandResult_AppendManyResponse
	appendBatch      func(*sequences.AppendManyForEventSourcesRequest) *sequences.CommandResult_AppendManyResponse
	append           func(context.Context, *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error)
	named            func(context.Context, *sequences.AppendWithNamedTagsRequest) (*sequences.CommandResult_AppendResponse, error)
	tail             func(context.Context, *sequences.TailSequenceNumberRequest) (*sequences.QueryResult_EventSequenceTailResponse, error)
	compatibility    func(context.Context, *clients.CompatibilityRequest)
}

func (s *fakeKernel) CheckCompatibility(ctx context.Context, request *clients.CompatibilityRequest) (*clients.CompatibilityResponse, error) {
	if s.compatibility != nil {
		s.compatibility(ctx, request)
	}
	return &clients.CompatibilityResponse{IsCompatible: !s.incompatible, ServerVersion: "test-kernel"}, nil
}
func (s *fakeKernel) Connect(request *clients.ConnectRequest, stream grpc.ServerStreamingServer[clients.ConnectionKeepAlive]) error {
	s.connectCalls.Add(1)
	if err := stream.Send(&clients.ConnectionKeepAlive{ConnectionId: request.ConnectionId}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
func (*fakeKernel) ConnectionKeepAlive(context.Context, *clients.ConnectionKeepAlive) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
func (*fakeKernel) EnsureEventStore(context.Context, *eventstores.EnsureEventStoreRequest) (*eventstores.CommandResult, error) {
	return &eventstores.CommandResult{IsAuthorized: true}, nil
}
func (*fakeKernel) AllEventStores(context.Context, *emptypb.Empty) (*eventstores.QueryResult_IEnumerable_EventStoreNamesResponse, error) {
	return &eventstores.QueryResult_IEnumerable_EventStoreNamesResponse{IsAuthorized: true, Data: []*eventstores.EventStoreNamesResponse{{Name: "customers"}}}, nil
}
func (*fakeKernel) EnsureNamespace(context.Context, *namespaces.EnsureNamespaceRequest) (*namespaces.CommandResult, error) {
	return &namespaces.CommandResult{IsAuthorized: true}, nil
}
func (*fakeKernel) AllNamespaces(context.Context, *namespaces.AllNamespacesRequest) (*namespaces.QueryResult_IEnumerable_NamespaceNamesResponse, error) {
	return &namespaces.QueryResult_IEnumerable_NamespaceNamesResponse{IsAuthorized: true, Data: []*namespaces.NamespaceNamesResponse{{Name: "default"}}}, nil
}
func (s *fakeKernel) RegisterEventTypes(_ context.Context, request *eventtypes.RegisterEventTypesRequest) (*eventtypes.CommandResult, error) {
	s.registrations.Add(1)
	if s.register != nil {
		s.register(request)
	}
	if s.failRegistration.Swap(false) {
		return &eventtypes.CommandResult{IsAuthorized: true, ExceptionMessages: []string{"registration failed"}}, nil
	}
	return &eventtypes.CommandResult{IsAuthorized: true}, nil
}
func (*fakeKernel) Register(context.Context, *constraintcontracts.RegisterConstraintsRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
func (s *fakeKernel) AppendMany(_ context.Context, request *sequences.AppendManyRequest) (*sequences.CommandResult_AppendManyResponse, error) {
	if s.appendMany != nil {
		return s.appendMany(request), nil
	}
	return nil, status.Error(codes.Unimplemented, "test requires an explicit handler")
}
func (s *fakeKernel) AppendManyForEventSources(_ context.Context, request *sequences.AppendManyForEventSourcesRequest) (*sequences.CommandResult_AppendManyResponse, error) {
	if s.appendBatch != nil {
		return s.appendBatch(request), nil
	}
	return nil, status.Error(codes.Unimplemented, "test requires an explicit handler")
}
func (s *fakeKernel) Append(ctx context.Context, request *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
	s.appendCalls.Add(1)
	if s.append != nil {
		return s.append(ctx, request)
	}
	return success(request, 0), nil
}
func (s *fakeKernel) AppendWithNamedTags(ctx context.Context, request *sequences.AppendWithNamedTagsRequest) (*sequences.CommandResult_AppendResponse, error) {
	s.appendCalls.Add(1)
	if s.named != nil {
		return s.named(ctx, request)
	}
	return nil, status.Error(codes.Unimplemented, "test requires an explicit handler")
}
func (s *fakeKernel) TailSequenceNumber(ctx context.Context, request *sequences.TailSequenceNumberRequest) (*sequences.QueryResult_EventSequenceTailResponse, error) {
	if s.tail != nil {
		return s.tail(ctx, request)
	}
	return &sequences.QueryResult_EventSequenceTailResponse{IsAuthorized: true, Data: &sequences.EventSequenceTailResponse{SequenceNumber: ^uint64(0)}}, nil
}

func success(request *sequences.AppendRequest, position uint64) *sequences.CommandResult_AppendResponse {
	checked := request.ConcurrencyScope.ExpectsNoMatchingEvent || request.ConcurrencyScope.SequenceNumber != ^uint64(0)
	return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true, CorrelationId: request.CorrelationId, SequenceNumber: position, ConcurrencyCheckPerformed: checked}}
}

func kernelConnection(t *testing.T, kernel *fakeKernel) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	clients.RegisterConnectionServiceServer(server, kernel)
	eventstores.RegisterEventStoresServer(server, kernel)
	eventtypes.RegisterEventTypesServer(server, kernel)
	constraintcontracts.RegisterConstraintsServer(server, kernel)
	namespaces.RegisterNamespacesServer(server, kernel)
	sequences.RegisterEventSequencesServer(server, kernel)
	done := make(chan struct{})
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	go serveKernelFixture(server, listener, done, func(args ...any) { t.Errorf("serve: %v", args[0]) })
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	return conn
}

func serveKernelFixture(server *grpc.Server, listener net.Listener, served chan struct{}, report func(...any)) {
	defer close(served)
	// Cleanup can stop the server before this goroutine starts serving.
	if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
		report(err)
	}
}

func testClient(t *testing.T, kernel *fakeKernel, options ...chronicle.ClientOption) (*chronicle.Client, *grpc.ClientConn) {
	t.Helper()
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[CustomerRegistered](registry); err != nil {
		t.Fatal(err)
	}
	conn := kernelConnection(t, kernel)
	base := []chronicle.ClientOption{chronicle.WithGRPCConnection(conn), chronicle.WithNoAuthentication(), chronicle.WithRegistry(registry)}
	client, err := chronicle.NewClient(append(base, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return client, conn
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}
