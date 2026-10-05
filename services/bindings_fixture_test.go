// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	"github.com/cratis/chronicle.go/services"
	"github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type facadeKernel struct {
	clients.UnimplementedConnectionServiceServer
	eventstores.UnimplementedEventStoresServer
	namespaces.UnimplementedNamespacesServer
	calls         atomic.Int32
	failNamespace atomic.Bool
	mu            sync.Mutex
	coordinates   [][2]string
}

func (k *facadeKernel) CheckCompatibility(context.Context, *clients.CompatibilityRequest) (*clients.CompatibilityResponse, error) {
	k.calls.Add(1)
	return &clients.CompatibilityResponse{IsCompatible: true, ServerVersion: "facade-test"}, nil
}
func (*facadeKernel) Connect(request *clients.ConnectRequest, stream grpc.ServerStreamingServer[clients.ConnectionKeepAlive]) error {
	if err := stream.Send(&clients.ConnectionKeepAlive{ConnectionId: request.ConnectionId}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
func (*facadeKernel) ConnectionKeepAlive(context.Context, *clients.ConnectionKeepAlive) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
func (*facadeKernel) EnsureEventStore(context.Context, *eventstores.EnsureEventStoreRequest) (*eventstores.CommandResult, error) {
	return &eventstores.CommandResult{IsAuthorized: true}, nil
}
func (k *facadeKernel) EnsureNamespace(_ context.Context, request *namespaces.EnsureNamespaceRequest) (*namespaces.CommandResult, error) {
	k.mu.Lock()
	k.coordinates = append(k.coordinates, [2]string{request.EventStore, request.Namespace})
	k.mu.Unlock()
	if k.failNamespace.Swap(false) {
		return nil, status.Error(codes.PermissionDenied, "fixture registration denied")
	}
	return &namespaces.CommandResult{IsAuthorized: true}, nil
}

func facadeConnection(t *testing.T, kernel *facadeKernel) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(grpc.WaitForHandlers(true))
	clients.RegisterConnectionServiceServer(server, kernel)
	eventstores.RegisterEventStoresServer(server, kernel)
	namespaces.RegisterNamespacesServer(server, kernel)
	done := make(chan struct{})
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	go func() {
		defer close(done)
		if err := server.Serve(listener); err != nil && err != grpc.ErrServerStopped {
			t.Errorf("serve: %v", err)
		}
	}()
	conn, err := grpc.NewClient("passthrough:///facade-test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
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

func captureFacadeClient(t *testing.T, kernel *facadeKernel, options ...chronicle.ClientOption) *chronicle.ClientPreparation {
	t.Helper()
	conn := facadeConnection(t, kernel)
	p, err := chronicle.CaptureClient(append([]chronicle.ClientOption{chronicle.WithGRPCConnection(conn), chronicle.WithNoAuthentication()}, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Client().Close(); err != nil {
			t.Error(err)
		}
	})
	return p
}

func facadeContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}
func bindFacades(t *testing.T, bindings dependencyinjection.Registrar, p *chronicle.ClientPreparation, selector services.StoreSelector) {
	t.Helper()
	if err := services.BindClient(bindings, p.Client()); err != nil {
		t.Fatal(err)
	}
	if err := services.BindEventStore(bindings, selector); err != nil {
		t.Fatal(err)
	}
	for _, bind := range []func(dependencyinjection.Registrar) error{services.BindEventLog, services.BindEventTypes, services.BindReadModels, services.BindCompliance} {
		if err := bind(bindings); err != nil {
			t.Fatal(err)
		}
	}
}
func facadeProvider(t *testing.T, bindings *container.Registry, client *chronicle.Client, options ...container.Option) dependencyinjection.Provider {
	t.Helper()
	provider, err := bindings.Build(options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
		if err := provider.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return provider
}
func facadeScope(t *testing.T, ctx context.Context, provider dependencyinjection.Provider) dependencyinjection.Scope {
	t.Helper()
	scope, err := provider.NewScope(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := scope.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return scope
}
func resolveFacade[T any](t *testing.T, ctx context.Context, scope dependencyinjection.Resolver) T {
	t.Helper()
	value, err := dependencyinjection.Resolve[T](ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
