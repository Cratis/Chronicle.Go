// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	"github.com/cratis/chronicle.go/internal/connection"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Interceptors keep lifecycle tests entirely in memory, so synctest can join
// waiters and advance backoff without network scheduling or sleeps as synchronization.
type lifecycleStream struct {
	ctx   context.Context
	id    string
	first bool
	end   chan error
}

func (s *lifecycleStream) Header() (metadata.MD, error) { return nil, nil }
func (s *lifecycleStream) Trailer() metadata.MD         { return nil }
func (s *lifecycleStream) CloseSend() error             { return nil }
func (s *lifecycleStream) Context() context.Context     { return s.ctx }
func (s *lifecycleStream) SendMsg(message any) error {
	if request, ok := message.(*clients.ConnectRequest); ok {
		s.id = request.ConnectionId
	}
	return nil
}
func (s *lifecycleStream) RecvMsg(message any) error {
	if !s.first {
		s.first = true
		if heartbeat, ok := message.(*clients.ConnectionKeepAlive); ok {
			heartbeat.ConnectionId = s.id
			return nil
		}
	}
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case err := <-s.end:
		return err
	}
}

func memoryLifecycleClient(t *testing.T, invoke func(context.Context, string) error, options ...ClientOption) (*Client, chan *lifecycleStream) {
	t.Helper()
	streams := make(chan *lifecycleStream, 20)
	conn, err := grpc.NewClient("passthrough:///memory", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, _, reply any, _ *grpc.ClientConn, _ grpc.UnaryInvoker, _ ...grpc.CallOption) error {
			if invoke != nil {
				if err := invoke(ctx, method); err != nil {
					return err
				}
			}
			switch response := reply.(type) {
			case *clients.CompatibilityResponse:
				response.IsCompatible = true
			case *eventstores.CommandResult:
				response.IsAuthorized = true
			case *namespaces.CommandResult:
				response.IsAuthorized = true
			}
			return nil
		}),
		grpc.WithStreamInterceptor(func(ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ grpc.Streamer, _ ...grpc.CallOption) (grpc.ClientStream, error) {
			stream := &lifecycleStream{ctx: ctx, end: make(chan error, 1)}
			streams <- stream
			return stream, nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	base := []ClientOption{WithGRPCConnection(conn), WithNoAuthentication(), WithKeepAliveTimeout(time.Minute)}
	client, err := NewClient(append(base, options...)...)
	if err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	return client, streams
}

func TestLiveConnectWaiterSurvivesCanceledStarter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		first := true
		client, _ := memoryLifecycleClient(t, func(ctx context.Context, method string) error {
			if method == clients.ConnectionService_CheckCompatibility_FullMethodName && first {
				first = false
				close(entered)
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		})
		starterCtx, cancel := context.WithCancel(t.Context())
		defer cancel()
		starter, waiter := make(chan error, 1), make(chan error, 1)
		go func() { starter <- client.Connect(starterCtx) }()
		<-entered
		go func() { waiter <- client.Connect(t.Context()) }()
		synctest.Wait()
		cancel()
		if err := <-starter; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		if err := <-waiter; err != nil {
			t.Fatalf("live Connect inherited starter cancellation: %v", err)
		}
	})
}

func TestAbandonedStreamCancellationReleasesAdmission(t *testing.T) {
	for _, closeClient := range []bool{false, true} {
		t.Run(map[bool]string{false: "caller cancellation", true: "client close"}[closeClient], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, _ := memoryLifecycleClient(t, nil)
				if err := client.Connect(t.Context()); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				stream, err := client.transport.NewStream(ctx, &grpc.StreamDesc{}, "/abandoned")
				if err != nil {
					t.Fatal(err)
				}
				if !closeClient {
					cancel()
				}
				closed := make(chan error, 1)
				go func() { closed <- client.Close() }()
				synctest.Wait()
				select {
				case err := <-closed:
					if err != nil {
						t.Fatal(err)
					}
				default:
					// Release the old implementation's lease before failing, so
					// the regression cannot deadlock test cleanup.
					stream.(*ownedStream).done()
					<-closed
					t.Fatal("abandoned stream blocked Close")
				}
				if err := stream.RecvMsg(nil); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
				stream.(*ownedStream).done() // Completion and cancellation release only once.
			})
		})
	}
}

// dropAfterHealthCheck returns the sampled health, then drops the generation.
// With no workers on this synthetic generation, the two readiness checks are
// deterministic and exercise loss specifically between connect and acquire.
type dropAfterHealthCheck struct {
	context.Context
	cancel context.CancelFunc
}

func (c *dropAfterHealthCheck) Err() error {
	err := c.Context.Err()
	c.cancel()
	return err
}

func TestReadinessRetriesGenerationLostBeforeAdmission(t *testing.T) {
	for _, waitForStore := range []bool{false, true} {
		t.Run(map[bool]string{false: "Ready", true: "WaitForRegistration"}[waitForStore], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, _ := memoryLifecycleClient(t, nil)
				ctx, cancel := context.WithCancel(client.life)
				defer cancel()
				client.current = &generation{ctx: &dropAfterHealthCheck{Context: ctx, cancel: cancel}}
				client.connectionError = connection.ErrStale
				var err error
				if waitForStore {
					store := &EventStore{client: client, name: "store", namespace: DefaultNamespace, catalog: client.catalog}
					_, err = store.WaitForRegistration(t.Context())
				} else {
					err = client.Ready(t.Context())
				}
				if err != nil {
					t.Fatalf("readiness returned admission race: %v", err)
				}
			})
		})
	}
}

func TestReadinessRetriesGenerationLostDuringRegistration(t *testing.T) {
	for _, waitForStore := range []bool{false, true} {
		t.Run(map[bool]string{false: "Ready", true: "WaitForRegistration"}[waitForStore], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var old *generation
				first := true
				client, _ := memoryLifecycleClient(t, func(_ context.Context, method string) error {
					if method == eventstores.EventStores_EnsureEventStore_FullMethodName && first {
						first = false
						old.cancel()
						return status.Error(codes.Unavailable, "generation lost")
					}
					return nil
				}, WithRegistrationRetry(RegistrationRetry{MaxAttempts: 1, InitialDelay: time.Second, MaximumDelay: time.Minute, AttemptTimeout: time.Minute}))
				if err := client.Connect(t.Context()); err != nil {
					t.Fatal(err)
				}
				client.mu.Lock()
				old = client.current
				store := &EventStore{client: client, name: "store", namespace: DefaultNamespace, catalog: client.catalog}
				client.stores[storeKey{"store", DefaultNamespace}] = store
				client.mu.Unlock()
				var err error
				if waitForStore {
					var outcome RegistrationOutcome
					outcome, err = store.WaitForRegistration(t.Context())
					if err == nil && (!outcome.IsSuccess() || outcome.Generation <= old.number) {
						t.Fatal(outcome)
					}
				} else {
					err = client.Ready(t.Context())
				}
				if err != nil {
					t.Fatalf("readiness returned transient generation loss: %v", err)
				}
			})
		})
	}
}

func TestShutdownStopsSupervisionBeforeGenerationConstruction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, streams := memoryLifecycleClient(t, nil)
		ctx, cancel := context.WithCancel(client.life)
		g := &generation{ctx: ctx, cancel: cancel}
		g.work.Add(1) // Keep graceful shutdown draining while supervision joins.
		client.current = g
		client.beginShutdown(true)
		s := &supervision{first: make(chan struct{}), done: make(chan struct{})}
		client.work.Add(1)
		go client.supervise(t.Context(), s)
		synctest.Wait()
		select {
		case <-s.done:
		default:
			t.Error("supervisor continued during graceful shutdown")
		}
		select {
		case <-streams:
			t.Error("generation established after shutdown began")
		default:
		}
		g.work.Done()
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		if !terminalConnectionError(ErrClosed) {
			t.Fatal("ErrClosed must terminate supervision")
		}
	})
}

func TestLiveRegistrationJoinerThroughSharedStageAndInvoke(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		first := true
		client, _ := memoryLifecycleClient(t, func(ctx context.Context, method string) error {
			if method == eventstores.EventStores_EnsureEventStore_FullMethodName && first {
				first = false
				close(entered)
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}, WithRegistrationRetry(RegistrationRetry{MaxAttempts: 1, InitialDelay: time.Second, MaximumDelay: time.Minute, AttemptTimeout: time.Minute}))
		if err := client.Connect(t.Context()); err != nil {
			t.Fatal(err)
		}
		starterCtx, cancel := context.WithCancel(t.Context())
		defer cancel()
		store := &EventStore{client: client, name: "store", namespace: "one", catalog: client.catalog}
		other := &EventStore{client: client, name: "store", namespace: "two", catalog: client.catalog}
		starter, sameNamespace, otherNamespace := make(chan error, 1), make(chan error, 1), make(chan error, 1)
		invoke := func(ctx context.Context, store *EventStore) error {
			return (&clientTransport{client: client, store: store}).Invoke(ctx, "/read", nil, nil)
		}
		go func() { starter <- invoke(starterCtx, store) }()
		<-entered
		go func() { sameNamespace <- invoke(t.Context(), store) }()
		go func() { otherNamespace <- invoke(t.Context(), other) }()
		synctest.Wait()
		cancel()
		if err := <-starter; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		for _, result := range []<-chan error{sameNamespace, otherNamespace} {
			if err := <-result; err != nil {
				t.Fatalf("live registration joiner inherited starter cancellation: %v", err)
			}
		}
	})
}

var _ grpc.ClientStream = (*lifecycleStream)(nil)
