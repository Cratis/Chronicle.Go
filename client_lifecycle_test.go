// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

type lifecycleKernel struct {
	clients.UnimplementedConnectionServiceServer
	sequences.UnimplementedEventSequencesServer
	endStream chan error
	appends   atomic.Int32
}

func (*lifecycleKernel) CheckCompatibility(context.Context, *clients.CompatibilityRequest) (*clients.CompatibilityResponse, error) {
	return &clients.CompatibilityResponse{IsCompatible: true}, nil
}
func (k *lifecycleKernel) Connect(request *clients.ConnectRequest, stream grpc.ServerStreamingServer[clients.ConnectionKeepAlive]) error {
	if err := stream.Send(&clients.ConnectionKeepAlive{ConnectionId: request.ConnectionId}); err != nil {
		return err
	}
	select {
	case err := <-k.endStream:
		return err
	case <-stream.Context().Done():
		return stream.Context().Err()
	}
}
func (*lifecycleKernel) ConnectionKeepAlive(context.Context, *clients.ConnectionKeepAlive) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}
func (k *lifecycleKernel) Append(_ context.Context, request *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
	k.appends.Add(1)
	return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true, CorrelationId: request.CorrelationId}}, nil
}

type lifecycleEvent struct{ Name string }

func TestConnectionLossAndRejectedOAuthRecover(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.Unauthenticated} {
		t.Run(code.String(), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			var accepted, tokenCalls atomic.Int32
			accepted.Store(1)
			tokenServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				tokenCalls.Add(1)
				_, _ = fmt.Fprintf(w, `{"access_token":"token-%d","expires_in":3600}`, accepted.Load())
			}))
			defer tokenServer.Close()
			kernel := &lifecycleKernel{endStream: make(chan error, 1)}
			listener := bufconn.Listen(1024 * 1024)
			server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
				md, _ := metadata.FromIncomingContext(ctx)
				values := md.Get("authorization")
				if len(values) != 1 || values[0] != fmt.Sprintf("Bearer token-%d", accepted.Load()) {
					return nil, status.Error(codes.Unauthenticated, "rejected token")
				}
				return handler(ctx, req)
			}))
			clients.RegisterConnectionServiceServer(server, kernel)
			sequences.RegisterEventSequencesServer(server, kernel)
			served := make(chan struct{})
			go serveSupervisionFixture(server, listener, served, t.Error)
			defer func() { server.Stop(); _ = listener.Close(); <-served }()
			conn, err := grpc.NewClient("passthrough:///lifecycle", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := conn.Close(); err != nil {
					t.Error(err)
				}
			}()
			client, err := NewClient(WithGRPCConnection(conn), WithConnectionString("chronicle://"+strings.TrimPrefix(tokenServer.URL, "https://")), WithDevelopmentDefaults())
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
			}()
			if err = client.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			descriptor, err := events.Define[lifecycleEvent]()
			if err != nil {
				t.Fatal(err)
			}
			catalog, err := events.NewCatalog(descriptor.Descriptor())
			if err != nil {
				t.Fatal(err)
			}
			log, err := eventsequences.New("test", "default", events.EventLog, catalog, client.transport)
			if err != nil {
				t.Fatal(err)
			}
			appendEvent := func() error {
				result, err := log.Append(ctx, "source", lifecycleEvent{Name: "Ada"}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
				return errors.Join(err, result.Err())
			}
			if err = appendEvent(); err != nil {
				t.Fatal(err)
			}
			client.mu.Lock()
			old, supervisor := client.current, client.supervisor
			client.mu.Unlock()
			accepted.Store(2)
			kernel.endStream <- status.Error(code, "connection ended")
			select {
			case <-old.ctx.Done():
			case <-ctx.Done():
				t.Fatal("old generation did not cancel")
			}
			if err = appendEvent(); err == nil || kernel.appends.Load() != 1 {
				t.Fatalf("lost generation admitted append: %v", err)
			}
			if code == codes.Unauthenticated {
				select {
				case <-supervisor.done:
				case <-ctx.Done():
					t.Fatal("terminal supervisor did not join")
				}
				if err = client.Ready(ctx); status.Code(err) != codes.Unauthenticated {
					t.Fatalf("terminal auth readiness: %v", err)
				}
				if err = client.Connect(ctx); err != nil {
					t.Fatal(err)
				}
			} else if err = client.Ready(ctx); err != nil {
				t.Fatalf("automatic reconnect: %v", err)
			}
			if err = appendEvent(); err != nil || kernel.appends.Load() != 2 || tokenCalls.Load() != 2 {
				t.Fatalf("recovery: appends=%d tokens=%d error=%v", kernel.appends.Load(), tokenCalls.Load(), err)
			}
		})
	}
}

type invalidatingSource struct{ invalidations atomic.Int32 }

func (*invalidatingSource) Token(context.Context) (Token, error) {
	return Token{AccessToken: "external"}, nil
}
func (s *invalidatingSource) Invalidate() { s.invalidations.Add(1) }

func TestExternalTokenInvalidation(t *testing.T) {
	source := &invalidatingSource{}
	invalidateRejectedToken(source, status.Error(codes.Unavailable, "transport"))
	invalidateRejectedToken(source, status.Error(codes.Unauthenticated, "auth"))
	if source.invalidations.Load() != 1 {
		t.Fatal("external invalidation contract not honored")
	}
}
