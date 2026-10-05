// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/clients"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type boundsKernel struct {
	clients.UnimplementedConnectionServiceServer
}

func (*boundsKernel) GetConnectedClients(context.Context, *emptypb.Empty) (*clients.IEnumerable_ConnectedClient, error) {
	return &clients.IEnumerable_ConnectedClient{}, nil
}

type boundsEcho interface {
	Echo(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error)
}

func (*boundsKernel) Echo(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
	if request.Value == "large" {
		return wrapperspb.String(strings.Repeat("x", 63)), nil
	}
	return wrapperspb.String(strings.Repeat("x", 62)), nil
}

func boundsServer(t *testing.T) (string, *tls.Config, *grpc.ClientConn) {
	t.Helper()
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
	kernel := &boundsKernel{}
	clients.RegisterConnectionServiceServer(server, kernel)
	server.RegisterService(&grpc.ServiceDesc{ServiceName: "bounds.Echo", HandlerType: (*boundsEcho)(nil),
		Methods: []grpc.MethodDesc{{MethodName: "Unary", Handler: func(srv any, ctx context.Context, decode func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			r := &wrapperspb.StringValue{}
			if err := decode(r); err != nil {
				return nil, err
			}
			return srv.(boundsEcho).Echo(ctx, r)
		}}},
		Streams: []grpc.StreamDesc{{StreamName: "Stream", ServerStreams: true, Handler: func(srv any, stream grpc.ServerStream) error {
			r := &wrapperspb.StringValue{}
			if err := stream.RecvMsg(r); err != nil {
				return err
			}
			response, err := srv.(boundsEcho).Echo(stream.Context(), r)
			if err != nil {
				return err
			}
			return stream.SendMsg(response)
		}}},
	}, kernel)
	served := make(chan struct{})
	go serveSupervisionFixture(server, listener, served, t.Error)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(policy)), grpc.WithDisableRetry(),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(32), grpc.MaxCallRecvMsgSize(32)))
	if err != nil {
		server.Stop()
		<-served
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
		server.Stop()
		<-served
	})
	return listener.Addr().String(), policy, conn
}

func boundsCall(ctx context.Context, conn grpc.ClientConnInterface, stream bool, request *wrapperspb.StringValue) error {
	if !stream {
		return conn.Invoke(ctx, "/bounds.Echo/Unary", request, &wrapperspb.StringValue{})
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	s, err := conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, "/bounds.Echo/Stream")
	if err != nil {
		return err
	}
	if err := s.SendMsg(request); err != nil {
		return err
	}
	if err := s.CloseSend(); err != nil {
		return err
	}
	return s.RecvMsg(&wrapperspb.StringValue{})
}

func TestMessageBoundsExactProtobufUnaryAndStreamingOwnedAndBorrowed(t *testing.T) {
	address, policy, borrowed := boundsServer(t)
	for _, ownership := range []string{"owned", "borrowed"} {
		t.Run(ownership, func(t *testing.T) {
			options := []ClientOption{WithNoAuthentication(), WithSkipKeepAlive(), WithSkipCompatibilityCheck(), WithMaxSendMessageSize(64), WithMaxReceiveMessageSize(64)}
			if ownership == "owned" {
				options = append(options, WithConnectionString("chronicle://"+address), WithTLS(policy))
			} else {
				options = append(options, WithGRPCConnection(borrowed))
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			client, err := Dial(ctx, options...)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
			}()
			for _, generationName := range []string{"initial", "replacement"} {
				if generationName == "replacement" {
					client.mu.Lock()
					old := client.current
					old.cancel()
					client.mu.Unlock()
					if err := client.Ready(ctx); err != nil {
						t.Fatal(err)
					}
					client.mu.Lock()
					if client.current.number <= old.number {
						t.Error("no replacement generation")
					}
					client.mu.Unlock()
				}
				client.mu.Lock()
				transport := client.current.transport
				client.mu.Unlock()
				for _, streaming := range []bool{false, true} {
					kind := "unary"
					if streaming {
						kind = "stream"
					}
					for _, tc := range []struct {
						name, value string
						size        int
						want        codes.Code
					}{
						{"exact send and receive", strings.Repeat("x", 62), 64, codes.OK},
						{"send one over", strings.Repeat("x", 63), 65, codes.ResourceExhausted},
						{"receive one over", "large", 7, codes.ResourceExhausted},
					} {
						t.Run(generationName+"/"+kind+"/"+tc.name, func(t *testing.T) {
							request := wrapperspb.String(tc.value)
							if proto.Size(request) != tc.size {
								t.Fatal("incorrect exact protobuf fixture")
							}
							if err := boundsCall(ctx, transport, streaming, request); status.Code(err) != tc.want {
								t.Fatalf("code=%v want=%v error=%v", status.Code(err), tc.want, err)
							}
						})
					}
				}
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			// Explicit RPC overrides neither modify nor close the borrowed channel.
			if _, err := clients.NewConnectionServiceClient(borrowed).GetConnectedClients(ctx, &emptypb.Empty{}); err != nil {
				t.Fatal(err)
			}
			for _, stream := range []bool{false, true} {
				if err := boundsCall(ctx, borrowed, stream, wrapperspb.String("small")); status.Code(err) != codes.ResourceExhausted {
					t.Fatalf("owner receive limit mutated: %v", err)
				}
			}
		})
	}
}

func TestExplicitMessageBoundDoesNotOverrideOtherDirection(t *testing.T) {
	_, _, borrowed := boundsServer(t)
	for _, tc := range []struct {
		name    string
		option  ClientOption
		request *wrapperspb.StringValue
	}{
		{"send only retains owner receive bound", WithMaxSendMessageSize(64), wrapperspb.String("small")},
		{"receive only retains owner send bound", WithMaxReceiveMessageSize(64), wrapperspb.String(strings.Repeat("x", 62))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			client, err := Dial(ctx, WithGRPCConnection(borrowed), WithNoAuthentication(), WithSkipCompatibilityCheck(), WithSkipKeepAlive(), tc.option)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
			}()
			client.mu.Lock()
			transport := client.current.transport
			client.mu.Unlock()
			for _, stream := range []bool{false, true} {
				if err := boundsCall(ctx, transport, stream, tc.request); status.Code(err) != codes.ResourceExhausted {
					t.Fatal("other direction was overridden", err)
				}
			}
		})
	}
}

func TestOmittedMessageBoundsPreserveBorrowedOwnerDefaults(t *testing.T) {
	address, policy, borrowed := boundsServer(t)
	for _, ownership := range []string{"owned", "borrowed"} {
		t.Run(ownership, func(t *testing.T) {
			options := []ClientOption{WithNoAuthentication(), WithSkipKeepAlive(), WithSkipCompatibilityCheck()}
			want := codes.OK
			if ownership == "owned" {
				options = append(options, WithConnectionString("chronicle://"+address), WithTLS(policy))
			} else {
				options = append(options, WithGRPCConnection(borrowed))
				want = codes.ResourceExhausted
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			client, err := Dial(ctx, options...)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
			}()
			client.mu.Lock()
			transport := client.current.transport
			client.mu.Unlock()
			if len(transport.callOptions) != 0 {
				t.Fatal("omitted options added borrowed overrides")
			}
			for _, stream := range []bool{false, true} {
				if err := boundsCall(ctx, transport, stream, wrapperspb.String("small")); status.Code(err) != want {
					t.Fatalf("defaults: code=%v want=%v error=%v", status.Code(err), want, err)
				}
			}
		})
	}
}
