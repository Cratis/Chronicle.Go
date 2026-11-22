// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package otelgrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type session struct{ id, address string }

type kernelFixture struct {
	clients.UnimplementedConnectionServiceServer
	eventstores.UnimplementedEventStoresServer
	namespaces.UnimplementedNamespacesServer
	eventtypes.UnimplementedEventTypesServer
	sequences.UnimplementedEventSequencesServer
	sessions  chan session
	endStream chan struct{}
	appends   atomic.Int32
}

func (k *kernelFixture) Connect(request *clients.ConnectRequest, stream grpc.ServerStreamingServer[clients.ConnectionKeepAlive]) error {
	if err := stream.Send(&clients.ConnectionKeepAlive{ConnectionId: request.ConnectionId}); err != nil {
		return err
	}
	remote, ok := peer.FromContext(stream.Context())
	if !ok {
		return status.Error(codes.Internal, "missing peer")
	}
	select {
	case k.sessions <- session{request.ConnectionId, remote.Addr.String()}:
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
	select {
	case <-k.endStream:
		return status.Error(codes.Unavailable, "fixture replaced session")
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}

func (*kernelFixture) ConnectionKeepAlive(context.Context, *clients.ConnectionKeepAlive) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (*kernelFixture) EnsureEventStore(context.Context, *eventstores.EnsureEventStoreRequest) (*eventstores.CommandResult, error) {
	return &eventstores.CommandResult{IsAuthorized: true}, nil
}

func (*kernelFixture) EnsureNamespace(context.Context, *namespaces.EnsureNamespaceRequest) (*namespaces.CommandResult, error) {
	return &namespaces.CommandResult{IsAuthorized: true}, nil
}

func (*kernelFixture) RegisterEventTypes(context.Context, *eventtypes.RegisterEventTypesRequest) (*eventtypes.CommandResult, error) {
	return &eventtypes.CommandResult{IsAuthorized: true}, nil
}

func (k *kernelFixture) Append(ctx context.Context, _ *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
	k.appends.Add(1)
	// The server may already have committed. Lose the session while the write
	// is admitted, without sending a response that could establish its outcome.
	select {
	case k.endStream <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func startFixture(t *testing.T) (*kernelFixture, string, *tls.Config) {
	t.Helper()
	// Same validating loopback TLS pattern as the root boundsServer fixture.
	certificateServer := httptest.NewTLSServer(nil)
	serverTLS := certificateServer.TLS.Clone()
	roots := x509.NewCertPool()
	roots.AddCert(certificateServer.Certificate())
	certificateServer.Close()
	policy := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(serverTLS)), grpc.WaitForHandlers(true))
	kernel := &kernelFixture{sessions: make(chan session, 8), endStream: make(chan struct{}, 1)}
	clients.RegisterConnectionServiceServer(server, kernel)
	eventstores.RegisterEventStoresServer(server, kernel)
	namespaces.RegisterNamespacesServer(server, kernel)
	eventtypes.RegisterEventTypesServer(server, kernel)
	sequences.RegisterEventSequencesServer(server, kernel)
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-served; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	})
	return kernel, "chronicle://" + listener.Addr().String(), policy
}

func await[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatal("waiting for fixture event:", ctx.Err())
		var zero T
		return zero
	}
}
