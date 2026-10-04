// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type panicDispositionError struct{ value any }

func (*panicDispositionError) Error() string                { return "unclassified disposition" }
func (e *panicDispositionError) GRPCStatus() *status.Status { panic(e.value) }

func TestBorrowedUnaryInterceptorPanicReleasesAdmittedWork(t *testing.T) {
	for _, dispatch := range []string{"ordinary", "additive", "destructive", "destructive-disposition"} {
		t.Run(dispatch, func(t *testing.T) {
			destructive := dispatch == "destructive" || dispatch == "destructive-disposition"
			registry, model := projectionRegistry(t)
			client, ctx := supervisionClient(t, runtimeKernel(), WithRegistry(registry))
			underlying := client.config.borrowed
			entered, release := make(chan struct{}), make(chan struct{})
			releaseRaw := sync.OnceFunc(func() { close(release) })
			t.Cleanup(releaseRaw)
			panicValue := &struct{ label string }{"application interceptor"}
			// This is an actual borrowed grpc.ClientConn: its application unary
			// interceptor panics inside raw Invoke, rather than an SDK test fake.
			conn, err := grpc.NewClient("passthrough:///panic", grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithUnaryInterceptor(func(ctx context.Context, method string, args, reply any, _ *grpc.ClientConn, _ grpc.UnaryInvoker, options ...grpc.CallOption) error {
					if method != "/test/ordinary" && method != "/test/register" {
						return underlying.Invoke(ctx, method, args, reply, options...)
					}
					close(entered)
					<-release
					if dispatch == "destructive-disposition" {
						return &panicDispositionError{value: panicValue}
					}
					panic(panicValue)
				}), grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, _ *grpc.ClientConn, method string, _ grpc.Streamer, options ...grpc.CallOption) (grpc.ClientStream, error) {
					return underlying.NewStream(ctx, desc, method, options...)
				}))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			client.config.borrowed = conn // Before production generation establishment.
			store, err := client.EventStore(ctx, "store")
			if err != nil {
				t.Fatal(err)
			}
			root := store.definitionRoot()
			client.mu.Lock()
			g := client.current
			client.mu.Unlock()
			if g.raw != conn {
				t.Fatal("production generation did not retain borrowed channel")
			}
			recovered := make(chan any, 1)
			go func() {
				defer func() { recovered <- recover() }()
				if dispatch == "ordinary" {
					_ = g.transport.Invoke(ctx, "/test/ordinary", nil, &emptypb.Empty{})
				} else {
					_ = store.definitionTransport(g, root, destructive).Invoke(ctx, "/test/register", nil, &emptypb.Empty{})
				}
			}()
			awaitSignal(t, ctx, entered)
			var added chan error
			if destructive {
				waiter := &flightWaitContext{Context: ctx, waiting: make(chan struct{})}
				_, declaration := runtimeModel(t)
				added = make(chan error, 1)
				go func() {
					result, err := store.RegisterProjection(waiter, declaration)
					if result.Published {
						t.Error("panic flight waiter published")
					}
					added <- err
				}()
				awaitSignal(t, ctx, waiter.waiting)
				if store.definitionRoot() != root {
					t.Fatal("flight waiter changed root")
				}
			}
			releaseRaw()
			select {
			case value := <-recovered:
				if value != panicValue {
					t.Fatal("panic identity changed")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if added != nil {
				select {
				case err := <-added:
					if !errors.Is(err, ErrDestructiveRegistrationUnknown) {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("panic did not release flight", ctx.Err())
				}
			}
			client.mu.Lock()
			flight, unknown := store.definitions.flight, store.definitions.destructiveUnknown
			client.mu.Unlock()
			if flight || unknown != destructive {
				t.Fatalf("flight=%v unknown=%v", flight, unknown)
			}
			joined := make(chan struct{})
			go func() { g.work.Wait(); close(joined) }()
			awaitSignal(t, ctx, joined)
			// The uncertainty fence is mutation-only; the current acknowledged
			// root's ordinary readiness, read and append are not poisoned.
			if err := client.Ready(ctx); err != nil {
				t.Fatal(err)
			}
			if outcome, err := store.WaitForRegistration(ctx); err != nil || !outcome.IsSuccess() {
				t.Fatal(outcome, err)
			}
			if _, err := readmodels.For(store.ReadModels(), model).Get(ctx, "key"); err != nil {
				t.Fatal(err)
			}
			if _, err := store.EventLog().Append(ctx, "source", ProjectionOpened{Name: "name"}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})); err != nil {
				t.Fatal(err)
			}
			if err := client.CloseContext(ctx); err != nil {
				t.Fatal("client-owned admitted counts were not released", err)
			}
			if conn.GetState() == connectivity.Shutdown {
				t.Fatal("SDK closed the borrowed connection")
			}
		})
	}
}
