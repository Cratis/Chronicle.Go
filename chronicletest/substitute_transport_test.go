// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/internal/clientoptions"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func transportForTest(t *testing.T) *substituteTransport {
	t.Helper()
	transport := substituteConnection()
	t.Cleanup(func() {
		if err := transport.Close(); err != nil {
			t.Error(err)
		}
	})
	return transport
}

func TestSubstituteUnaryCopiesRequestsAndResponses(t *testing.T) {
	transport := transportForTest(t)
	response := &clients.CompatibilityResponse{IsCompatible: true, Incompatibilities: []string{"first"}}
	var received *clients.CompatibilityRequest
	transport.RegisterService(&grpc.ServiceDesc{ServiceName: "test.Unary", Methods: []grpc.MethodDesc{{
		MethodName: "Copy",
		Handler: func(_ any, ctx context.Context, decode func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			received = new(clients.CompatibilityRequest)
			if err := decode(received); err != nil {
				return nil, err
			}
			md, _ := metadata.FromIncomingContext(ctx)
			if got := md.Get("scope"); len(got) != 1 || got[0] != "scenario" {
				t.Errorf("incoming metadata = %v", md)
			}
			received.DescriptorSet[0] = 9
			return response, nil
		},
	}}}, nil)
	request := &clients.CompatibilityRequest{DescriptorSet: []byte{1, 2}}
	reply := &clients.CompatibilityResponse{ServerVersion: "stale", Incompatibilities: []string{"stale"}}
	ctx := metadata.AppendToOutgoingContext(t.Context(), "scope", "scenario")
	if err := transport.Invoke(ctx, "/test.Unary/Copy", request, reply); err != nil {
		t.Fatal(err)
	}
	if request.DescriptorSet[0] != 1 || !proto.Equal(response, reply) {
		t.Fatalf("request/reply = %v / %v", request, reply)
	}
	request.DescriptorSet[1] = 8
	reply.Incompatibilities[0] = "changed"
	if received.DescriptorSet[1] != 2 || response.Incompatibilities[0] != "first" {
		t.Fatal("transport retained caller-owned protobuf data")
	}
}

func TestSubstituteUnaryReportsStatusAndRejectsInvalidMessages(t *testing.T) {
	transport := transportForTest(t)
	service := clients.NewConnectionServiceClient(transport)
	_, err := service.GetConnectedClients(t.Context(), &emptypb.Empty{})
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("unimplemented = %v", err)
	}
	tests := []struct {
		name   string
		method string
		args   any
		reply  any
		code   codes.Code
	}{
		{"unknown method", "/missing/Method", &emptypb.Empty{}, &emptypb.Empty{}, codes.Unimplemented},
		{"wrong request", clients.ConnectionService_CheckCompatibility_FullMethodName, &emptypb.Empty{}, &clients.CompatibilityResponse{}, codes.Internal},
		{"wrong reply", clients.ConnectionService_CheckCompatibility_FullMethodName, &clients.CompatibilityRequest{}, &emptypb.Empty{}, codes.Internal},
		{"nil request", clients.ConnectionService_CheckCompatibility_FullMethodName, (*clients.CompatibilityRequest)(nil), &clients.CompatibilityResponse{}, codes.Internal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := transport.Invoke(t.Context(), test.method, test.args, test.reply)
			if status.Code(err) != test.code {
				t.Fatalf("status = %v, want %v", err, test.code)
			}
		})
	}
}

func TestSubstituteUnaryPreservesStatusDetails(t *testing.T) {
	transport := transportForTest(t)
	failure, err := status.New(codes.Aborted, "rejected").WithDetails(&clients.ConnectionKeepAlive{ConnectionId: "detail"})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{"status", failure.Err(), codes.Aborted},
		{"ordinary error", errors.New("failure"), codes.Unknown},
		{"canceled", context.Canceled, codes.Canceled},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport.RegisterService(&grpc.ServiceDesc{ServiceName: "test.Failure", Methods: []grpc.MethodDesc{{
				MethodName: "Fail", Handler: func(any, context.Context, func(any) error, grpc.UnaryServerInterceptor) (any, error) {
					return nil, test.err
				},
			}}}, nil)
			err := transport.Invoke(t.Context(), "/test.Failure/Fail", &emptypb.Empty{}, &emptypb.Empty{})
			if status.Code(err) != test.code {
				t.Fatalf("status = %v, want %v", err, test.code)
			}
			if test.name == "status" && !proto.Equal(status.Convert(err).Proto(), failure.Proto()) {
				t.Fatal("status details lost")
			}
		})
	}
}

func TestSubstituteCanceledUnaryNeverDispatches(t *testing.T) {
	transport := transportForTest(t)
	transport.RegisterService(&grpc.ServiceDesc{ServiceName: "test.Canceled", Methods: []grpc.MethodDesc{{
		MethodName: "Call", Handler: func(any, context.Context, func(any) error, grpc.UnaryServerInterceptor) (any, error) {
			t.Fatal("canceled invocation reached handler")
			return nil, nil
		},
	}}}, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := transport.Invoke(ctx, "/test.Canceled/Call", &emptypb.Empty{}, &emptypb.Empty{}); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled = %v", err)
	}
}

func TestSubstituteCloseCancelsAndJoinsUnaryCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := transportForTest(t)
		finished := make(chan struct{})
		transport.RegisterService(&grpc.ServiceDesc{ServiceName: "test.Blocking", Methods: []grpc.MethodDesc{{
			MethodName: "Wait", Handler: func(_ any, ctx context.Context, _ func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				<-ctx.Done()
				defer close(finished)
				return nil, ctx.Err()
			},
		}}}, nil)
		result := make(chan error, 1)
		go func() {
			result <- transport.Invoke(t.Context(), "/test.Blocking/Wait", &emptypb.Empty{}, &emptypb.Empty{})
		}()
		synctest.Wait()
		if err := transport.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-finished:
		default:
			t.Fatal("Close did not join the handler")
		}
		if err := <-result; status.Code(err) != codes.Canceled {
			t.Fatalf("active call = %v", err)
		}
		if _, err := clients.NewConnectionServiceClient(transport).CheckCompatibility(t.Context(), &clients.CompatibilityRequest{}); status.Code(err) != codes.Unavailable {
			t.Fatalf("closed transport = %v", err)
		}
	})
}

func TestSubstituteUnaryDeadlineCancelsHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := transportForTest(t)
		transport.RegisterService(&grpc.ServiceDesc{ServiceName: "test.Deadline", Methods: []grpc.MethodDesc{{
			MethodName: "Wait", Handler: func(_ any, ctx context.Context, _ func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}}}, nil)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := transport.Invoke(ctx, "/test.Deadline/Wait", &emptypb.Empty{}, &emptypb.Empty{}); status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("deadline = %v", err)
		}
	})
}

func TestClientBorrowsSubstituteTransport(t *testing.T) {
	transport := transportForTest(t)
	client, err := chronicle.NewClient(clientoptions.Connection[chronicle.ClientOption](transport), chronicle.WithNoAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := client.Connect(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := clients.NewConnectionServiceClient(transport).CheckCompatibility(t.Context(), &clients.CompatibilityRequest{}); err != nil {
		t.Fatalf("client closed the borrowed transport: %v", err)
	}
}
