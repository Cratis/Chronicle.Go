// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/clients"
	constraintcontracts "github.com/cratis/chronicle.go/contracts/events/constraints"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	"github.com/cratis/chronicle.go/internal/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type namespaceRemovalRPC func(context.Context, *namespaces.RemoveNamespaceRequest) (*namespaces.CommandResult, error)

type namespaceRemovalFixture struct {
	client *chronicle.Client
	calls  atomic.Int32
}

func newNamespaceRemovalFixture(t *testing.T, remove namespaceRemovalRPC, options ...grpc.ServerOption) *namespaceRemovalFixture {
	t.Helper()
	fixture := &namespaceRemovalFixture{}
	listener := bufconn.Listen(1 << 20)
	options = append(options, grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod != clients.ConnectionService_ConnectionKeepAlive_FullMethodName {
			fixture.calls.Add(1)
		}
		if info.FullMethod == namespaces.Namespaces_RemoveNamespace_FullMethodName {
			return remove(ctx, request.(*namespaces.RemoveNamespaceRequest))
		}
		return handler(ctx, request)
	}))
	server := grpc.NewServer(options...)
	kernel := &fakeKernel{}
	clients.RegisterConnectionServiceServer(server, kernel)
	eventstores.RegisterEventStoresServer(server, kernel)
	eventtypes.RegisterEventTypesServer(server, kernel)
	constraintcontracts.RegisterConstraintsServer(server, kernel)
	namespaces.RegisterNamespacesServer(server, kernel)
	done := make(chan struct{})
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-done })
	go serveKernelFixture(server, listener, done, t.Error)
	conn, err := grpc.NewClient("passthrough:///namespace-removal", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	fixture.client, err = chronicle.NewClient(chronicle.WithGRPCConnection(conn), chronicle.WithNoAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fixture.client.Close(); err != nil {
			t.Error(err)
		}
	})
	return fixture
}

func (f *namespaceRemovalFixture) store(t *testing.T, name chronicle.StoreName, namespace chronicle.Namespace) *chronicle.EventStore {
	t.Helper()
	store, err := f.client.EventStore(testContext(t), name, chronicle.WithNamespace(namespace))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestNamespaceRemovalRoutesExplicitTargetWithoutLifecycleWork(t *testing.T) {
	var requests []*namespaces.RemoveNamespaceRequest
	fixture := newNamespaceRemovalFixture(t, func(ctx context.Context, request *namespaces.RemoveNamespaceRequest) (*namespaces.CommandResult, error) {
		if got := metadata.ValueFromIncomingContext(ctx, "removal-trace"); len(got) != 1 || got[0] != "test" {
			t.Errorf("caller metadata = %v", got)
		}
		requests = append(requests, proto.Clone(request).(*namespaces.RemoveNamespaceRequest))
		return &namespaces.CommandResult{IsAuthorized: true}, nil
	})
	admin := fixture.store(t, "first", chronicle.DefaultNamespace)
	target := fixture.store(t, "first", " tenant ") // Offline only: prove no local invalidation.
	other := fixture.store(t, "second", "unrelated")
	before := fixture.calls.Load()
	ctx := metadata.AppendToOutgoingContext(testContext(t), "removal-trace", "test")
	if err := admin.Namespaces().Remove(ctx, " tenant "); err != nil {
		t.Fatal(err)
	}
	if err := other.Namespaces().Remove(ctx, "another-target"); err != nil {
		t.Fatal(err)
	}
	if got := fixture.calls.Load() - before; got != 2 || len(requests) != 2 {
		t.Fatalf("RPCs = %d, removal requests = %d; want exactly two removals", got, len(requests))
	}
	equalOperation(t, requests[0], &namespaces.RemoveNamespaceRequest{EventStore: "first", Namespace: " tenant "})
	equalOperation(t, requests[1], &namespaces.RemoveNamespaceRequest{EventStore: "second", Namespace: "another-target"})
	if admin.Namespace() != chronicle.DefaultNamespace || other.Namespace() != "unrelated" {
		t.Fatal("removal changed handle selection")
	}
	if cached := fixture.store(t, "first", " tenant "); cached != target {
		t.Fatal("removal evicted the target handle")
	}
}

func TestNamespaceRemovalRejectsBlankInputWithoutDispatch(t *testing.T) {
	fixture := newNamespaceRemovalFixture(t, nil)
	store := fixture.store(t, "store", chronicle.DefaultNamespace)
	before := fixture.calls.Load()
	for _, target := range []chronicle.Namespace{"", " ", "\t\n", "\u2003"} {
		if err := store.Namespaces().Remove(testContext(t), target); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
			t.Fatalf("target %q: %v", target, err)
		}
	}
	if fixture.calls.Load() != before {
		t.Fatal("invalid input dispatched an RPC")
	}
}

func TestNamespaceRemovalAcceptsRepeatedAcknowledgementsAndWarnings(t *testing.T) {
	fixture := newNamespaceRemovalFixture(t, func(context.Context, *namespaces.RemoveNamespaceRequest) (*namespaces.CommandResult, error) {
		return &namespaces.CommandResult{IsAuthorized: true, ValidationResults: []*namespaces.ValidationResult{{Severity: namespaces.ValidationResultSeverity_Warning, Message: "warning"}}}, nil
	})
	store := fixture.store(t, "store", chronicle.DefaultNamespace)
	before := fixture.calls.Load()
	// The kernel owns missing-target idempotency; the SDK sends each explicit request.
	for range 2 {
		if err := store.Namespaces().Remove(testContext(t), "missing"); err != nil {
			t.Fatal(err)
		}
	}
	if fixture.calls.Load()-before != 2 {
		t.Fatal("explicit requests were suppressed, retried or caused lifecycle work")
	}
}

func TestNamespaceRemovalPreservesCommandEnvelopeFailures(t *testing.T) {
	for _, test := range []struct {
		name  string
		reply *namespaces.CommandResult
		want  *chronicle.EnvelopeError
	}{
		{"empty", &namespaces.CommandResult{}, &chronicle.EnvelopeError{}},
		{"authorization", &namespaces.CommandResult{AuthorizationFailureReason: "denied"}, &chronicle.EnvelopeError{AuthorizationFailureReason: "denied"}},
		{"validation", &namespaces.CommandResult{IsAuthorized: true, ValidationResults: []*namespaces.ValidationResult{{Severity: namespaces.ValidationResultSeverity_Error, Message: "invalid", Members: []string{"Namespace"}}}}, &chronicle.EnvelopeError{Authorized: true, ValidationResults: []chronicle.ValidationResult{{Severity: 3, Message: "invalid", Members: []string{"Namespace"}}}}},
		{"execution", &namespaces.CommandResult{IsAuthorized: true, ExceptionMessages: []string{"partial removal"}}, &chronicle.EnvelopeError{Authorized: true, ExceptionMessages: []string{"partial removal"}}},
		{"unknown severity", &namespaces.CommandResult{IsAuthorized: true, ValidationResults: []*namespaces.ValidationResult{{Severity: 999, Message: "future validation"}}}, &chronicle.EnvelopeError{Authorized: true, ValidationResults: []chronicle.ValidationResult{{Severity: 999, Message: "future validation"}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newNamespaceRemovalFixture(t, func(context.Context, *namespaces.RemoveNamespaceRequest) (*namespaces.CommandResult, error) {
				return test.reply, nil
			})
			store := fixture.store(t, "store", chronicle.DefaultNamespace)
			before := fixture.calls.Load()
			err := store.Namespaces().Remove(testContext(t), "tenant")
			var envelope *chronicle.EnvelopeError
			if !errors.As(err, &envelope) {
				t.Fatalf("error = %v; want EnvelopeError", err)
			}
			if !reflect.DeepEqual(envelope, test.want) {
				t.Fatalf("envelope = %+v; want %+v", envelope, test.want)
			}
			if fixture.calls.Load()-before != 1 {
				t.Fatal("envelope failure retried or triggered lifecycle work")
			}
		})
	}
}

func TestNamespaceRemovalLeavesProtectedTargetsServerAuthoritative(t *testing.T) {
	for _, target := range []chronicle.Namespace{chronicle.DefaultNamespace, "dEfAuLt", "tenant"} {
		t.Run(string(target), func(t *testing.T) {
			fixture := newNamespaceRemovalFixture(t, func(_ context.Context, request *namespaces.RemoveNamespaceRequest) (*namespaces.CommandResult, error) {
				equalOperation(t, request, &namespaces.RemoveNamespaceRequest{EventStore: "system", Namespace: string(target)})
				return &namespaces.CommandResult{IsAuthorized: true, ExceptionMessages: []string{"protected target"}}, nil
			})
			store := fixture.store(t, "system", chronicle.DefaultNamespace)
			before := fixture.calls.Load()
			var envelope *chronicle.EnvelopeError
			if err := store.Namespaces().Remove(testContext(t), target); !errors.As(err, &envelope) || fixture.calls.Load()-before != 1 {
				t.Fatalf("server rejection not preserved: %v", err)
			}
		})
	}
}

func TestNamespaceRemovalPreservesTransportErrorsWithoutRetry(t *testing.T) {
	for _, code := range []codes.Code{codes.Unimplemented, codes.Unavailable, codes.PermissionDenied, codes.Internal} {
		t.Run(code.String(), func(t *testing.T) {
			fixture := newNamespaceRemovalFixture(t, func(context.Context, *namespaces.RemoveNamespaceRequest) (*namespaces.CommandResult, error) {
				return nil, status.Error(code, "removal failed")
			})
			store := fixture.store(t, "store", chronicle.DefaultNamespace)
			before := fixture.calls.Load()
			if err := store.Namespaces().Remove(testContext(t), "tenant"); status.Code(err) != code {
				t.Fatalf("error = %v; want %v", err, code)
			}
			if fixture.calls.Load()-before != 1 {
				t.Fatal("transport failure retried")
			}
		})
	}
}

func TestNamespaceRemovalRejectsMalformedResponse(t *testing.T) {
	var corrupt atomic.Bool
	fixture := newNamespaceRemovalFixture(t, func(context.Context, *namespaces.RemoveNamespaceRequest) (*namespaces.CommandResult, error) {
		return &namespaces.CommandResult{IsAuthorized: true}, nil
	}, grpc.ForceServerCodec(malformedRemovalCodec{enabled: &corrupt}))
	store := fixture.store(t, "store", chronicle.DefaultNamespace)
	corrupt.Store(true)
	before := fixture.calls.Load()
	if err := store.Namespaces().Remove(testContext(t), "tenant"); status.Code(err) != codes.Internal {
		t.Fatalf("malformed response error = %v", err)
	}
	if fixture.calls.Load()-before != 1 {
		t.Fatal("malformed response retried")
	}
}

type malformedRemovalCodec struct{ enabled *atomic.Bool }

func (malformedRemovalCodec) Name() string { return "proto" }
func (c malformedRemovalCodec) Marshal(value any) ([]byte, error) {
	if _, ok := value.(*namespaces.CommandResult); ok && c.enabled.Load() {
		return []byte{0xff}, nil
	}
	return proto.Marshal(value.(proto.Message))
}
func (malformedRemovalCodec) Unmarshal(data []byte, value any) error {
	return proto.Unmarshal(data, value.(proto.Message))
}

func TestNamespaceRemovalRejectsNilEnvelope(t *testing.T) {
	// The generated client allocates its response before Invoke, so neither a
	// fake connection nor bufconn can return a nil response with a nil error.
	// Exercise the shared envelope guard directly for that impossible wire shape.
	var reply *namespaces.CommandResult
	if err := wire.CheckEnvelope(reply); !errors.Is(err, chronicle.ErrProtocol) {
		t.Fatalf("nil envelope error = %v", err)
	}
}

func TestNamespaceRemovalCancellationAfterDispatch(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancellation", true: "deadline"}[deadline], func(t *testing.T) {
			entered, stopped := make(chan struct{}), make(chan struct{})
			fixture := newNamespaceRemovalFixture(t, func(ctx context.Context, _ *namespaces.RemoveNamespaceRequest) (*namespaces.CommandResult, error) {
				defer close(stopped)
				if _, ok := ctx.Deadline(); !ok {
					t.Error("caller deadline not propagated")
				}
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			})
			store := fixture.store(t, "store", chronicle.DefaultNamespace)
			before := fixture.calls.Load()
			budget := 5 * time.Second
			if deadline {
				budget = time.Second
			}
			ctx, cancel := context.WithTimeout(testContext(t), budget)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- store.Namespaces().Remove(ctx, "tenant") }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("removal did not reach the server")
			}
			want := codes.DeadlineExceeded
			if !deadline {
				cancel()
				want = codes.Canceled
			}
			if err := <-result; status.Code(err) != want {
				t.Fatalf("error = %v; want %v", err, want)
			}
			select {
			case <-stopped:
			case <-testContext(t).Done():
				t.Fatal("server handler did not stop")
			}
			if fixture.calls.Load()-before != 1 {
				t.Fatal("canceled removal retried")
			}
		})
	}
}

func TestNamespaceRemovalClosedOrCanceledClientDoesNotDispatch(t *testing.T) {
	for _, closed := range []bool{false, true} {
		t.Run(map[bool]string{false: "pre-canceled", true: "closed"}[closed], func(t *testing.T) {
			fixture := newNamespaceRemovalFixture(t, nil)
			store := fixture.store(t, "store", chronicle.DefaultNamespace)
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			want := context.Canceled
			if closed {
				if err := fixture.client.Close(); err != nil {
					t.Fatal(err)
				}
				want = chronicle.ErrClosed
			} else {
				cancel()
			}
			before := fixture.calls.Load()
			if err := store.Namespaces().Remove(ctx, "tenant"); !errors.Is(err, want) {
				t.Fatalf("error = %v; want %v", err, want)
			}
			if fixture.calls.Load() != before {
				t.Fatal("closed/canceled removal dispatched")
			}
		})
	}
}
