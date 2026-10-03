// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cratis/chronicle.go/contracts/clients"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func streamForTest(t *testing.T, transport *substituteTransport, ctx context.Context, handler grpc.StreamHandler) grpc.ClientStream {
	t.Helper()
	desc := grpc.StreamDesc{StreamName: "Stream", Handler: handler, ServerStreams: true, ClientStreams: true}
	transport.RegisterService(&grpc.ServiceDesc{ServiceName: "test.Streaming", Streams: []grpc.StreamDesc{desc}}, nil)
	stream, err := transport.NewStream(ctx, &desc, "/test.Streaming/Stream")
	if err != nil {
		t.Fatal(err)
	}
	return stream
}

func TestSubstituteStreamCopiesMessagesAndDrainsBeforeEOF(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := transportForTest(t)
		stream := streamForTest(t, transport, t.Context(), func(_ any, stream grpc.ServerStream) error {
			if err := stream.SetHeader(metadata.Pairs("header", "value")); err != nil {
				return err
			}
			stream.SetTrailer(metadata.Pairs("trailer", "value"))
			for {
				request := new(clients.CompatibilityRequest)
				if err := stream.RecvMsg(request); err != nil {
					if errors.Is(err, io.EOF) {
						return nil
					}
					return err
				}
				if err := stream.SendMsg(request); err != nil {
					return err
				}
				request.DescriptorSet[0] = 99
			}
		})
		for i := byte(1); i <= 3; i++ {
			request := &clients.CompatibilityRequest{DescriptorSet: []byte{i}}
			if err := stream.SendMsg(request); err != nil {
				t.Fatal(err)
			}
			request.DescriptorSet[0] = 88
			synctest.Wait() // Handler sent the snapshot and mutated its copy.
			if i == 3 {
				if err := stream.CloseSend(); err != nil {
					t.Fatal(err)
				}
				synctest.Wait() // Complete with a response still queued.
			}
			var response clients.CompatibilityRequest
			if err := stream.RecvMsg(&response); err != nil || len(response.DescriptorSet) != 1 || response.DescriptorSet[0] != i {
				t.Fatalf("response = %v, %v", &response, err)
			}
		}
		for range 2 {
			if err := stream.RecvMsg(&clients.CompatibilityRequest{}); !errors.Is(err, io.EOF) {
				t.Fatalf("terminal receive = %v", err)
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
		}
		if err := stream.SendMsg(&clients.CompatibilityRequest{}); status.Code(err) != codes.Internal {
			t.Fatalf("send after half-close = %v", err)
		}
		header, err := stream.Header()
		if err != nil || len(header.Get("header")) != 1 || header.Get("header")[0] != "value" {
			t.Fatalf("header = %v, %v", header, err)
		}
		if trailer := stream.Trailer().Get("trailer"); len(trailer) != 1 || trailer[0] != "value" {
			t.Fatalf("trailer = %v", trailer)
		}
	})
}

func TestSubstituteStreamKeepsQueuedResponseBeforeFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := transportForTest(t)
		stream := streamForTest(t, transport, t.Context(), func(_ any, stream grpc.ServerStream) error {
			if err := stream.SendMsg(&clients.ConnectionKeepAlive{ConnectionId: "first"}); err != nil {
				return err
			}
			return status.Error(codes.Aborted, "failure after response")
		})
		synctest.Wait()
		if err := stream.SendMsg(&emptypb.Empty{}); !errors.Is(err, io.EOF) {
			t.Fatalf("send after completion = %v", err)
		}
		var response clients.ConnectionKeepAlive
		if err := stream.RecvMsg(&response); err != nil || response.ConnectionId != "first" {
			t.Fatalf("response = %v, %v", &response, err)
		}
		if err := stream.RecvMsg(&response); status.Code(err) != codes.Aborted {
			t.Fatalf("terminal receive = %v", err)
		}
	})
}

func TestSubstituteConnectSurvivesHalfCloseUntilCanceled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := transportForTest(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		stream, err := clients.NewConnectionServiceClient(transport).Connect(ctx, &clients.ConnectRequest{ConnectionId: "connection"})
		if err != nil {
			t.Fatal(err)
		}
		message, err := stream.Recv()
		if err != nil || message.ConnectionId != "connection" {
			t.Fatalf("keep-alive = %v, %v", message, err)
		}
		received := make(chan error, 1)
		go func() { _, err := stream.Recv(); received <- err }()
		synctest.Wait()
		select {
		case err := <-received:
			t.Fatalf("half-close ended the keep-alive stream: %v", err)
		default:
		}
		cancel()
		if err := <-received; status.Code(err) != codes.Canceled {
			t.Fatalf("canceled receive = %v", err)
		}
	})
}

func TestSubstituteStreamCancellationReleasesBlockedOperations(t *testing.T) {
	for _, operation := range []string{"client send", "client receive", "server send", "server receive", "headers"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				transport := transportForTest(t)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				exited := make(chan struct{})
				stream := streamForTest(t, transport, ctx, func(_ any, stream grpc.ServerStream) error {
					defer close(exited)
					switch operation {
					case "server send":
						if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
							return err
						}
						return stream.SendMsg(&emptypb.Empty{})
					case "server receive":
						return stream.RecvMsg(&emptypb.Empty{})
					default:
						<-stream.Context().Done()
						return stream.Context().Err()
					}
				})
				result := make(chan error, 1)
				switch operation {
				case "client send":
					if err := stream.SendMsg(&emptypb.Empty{}); err != nil {
						t.Fatal(err)
					}
					go func() { result <- stream.SendMsg(&emptypb.Empty{}) }()
				case "client receive":
					go func() { result <- stream.RecvMsg(&emptypb.Empty{}) }()
				case "headers":
					go func() { _, err := stream.Header(); result <- err }()
				}
				synctest.Wait()
				cancel()
				if err := transport.Close(); err != nil {
					t.Fatal(err)
				}
				select {
				case <-exited:
				default:
					t.Fatal("Close did not join the stream handler")
				}
				if operation == "client send" || operation == "client receive" || operation == "headers" {
					if err := <-result; status.Code(err) != codes.Canceled && !errors.Is(err, io.EOF) {
						t.Fatalf("blocked %s = %v", operation, err)
					}
				}
			})
		})
	}
}

func TestSubstituteStreamDeadlineAndCloseCancelKeepAlive(t *testing.T) {
	for _, closeTransport := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "transport close"}[closeTransport], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				transport := transportForTest(t)
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				defer cancel()
				stream, err := clients.NewConnectionServiceClient(transport).Connect(ctx, &clients.ConnectRequest{})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := stream.Recv(); err != nil {
					t.Fatal(err)
				}
				want := codes.DeadlineExceeded
				if closeTransport {
					want = codes.Canceled
					if err := transport.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := stream.Recv(); status.Code(err) != want {
					t.Fatalf("terminal status = %v, want %v", err, want)
				}
			})
		})
	}
}

func TestSubstituteStreamRejectsUnsupportedMethodsAndClosedTransport(t *testing.T) {
	transport := transportForTest(t)
	if _, err := transport.NewStream(t.Context(), &grpc.StreamDesc{}, "/missing/Stream"); status.Code(err) != codes.Unimplemented {
		t.Fatalf("unknown stream = %v", err)
	}
	stream, err := clients.NewConnectionServiceClient(transport).ObserveConnectedClients(t.Context(), &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.Unimplemented {
		t.Fatalf("unimplemented stream = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := clients.NewConnectionServiceClient(transport).Connect(ctx, &clients.ConnectRequest{}); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled stream = %v", err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.NewConnectionServiceClient(transport).Connect(t.Context(), &clients.ConnectRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("closed transport stream = %v", err)
	}
}
