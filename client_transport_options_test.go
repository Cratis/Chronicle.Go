// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
	"github.com/cratis/chronicle.go/seeding"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestMessageSizeOptionsValidateFinalValues(t *testing.T) {
	for _, option := range []func(int) ClientOption{WithMaxSendMessageSize, WithMaxReceiveMessageSize} {
		invalidSizes := []int{0, -1}
		overflow := int64(math.MaxInt32) + 1
		if int64(int(overflow)) == overflow {
			invalidSizes = append(invalidSizes, int(overflow))
		}
		for _, size := range invalidSizes {
			client, err := NewClient(option(size))
			if !errors.Is(err, ErrInvalidConfiguration) || client != nil {
				t.Fatalf("size %d: client=%v error=%v", size, client, err)
			}
		}
		for _, size := range []int{1, math.MaxInt32} {
			client, err := NewClient(option(0), option(size))
			if err != nil {
				t.Fatal(err)
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	client, err := NewClient(WithMaxSendMessageSize(42), WithMaxSendMessageSize(64), WithMaxReceiveMessageSize(80))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	if client.config.maxSendMessageSize != 64 || client.config.maxReceiveMessageSize != 80 || !client.config.maxSendMessageSizeSet || !client.config.maxReceiveMessageSizeSet {
		t.Fatal("explicit last-wins values were not frozen")
	}
	defaults, err := NewClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := defaults.Close(); err != nil {
			t.Error(err)
		}
	}()
	if defaults.config.maxSendMessageSize != 104857600 || defaults.config.maxReceiveMessageSize != 104857600 || defaults.config.maxSendMessageSizeSet || defaults.config.maxReceiveMessageSizeSet || defaults.config.skipKeepAlive {
		t.Fatal("defaults changed")
	}
}

func TestSkipKeepAliveRejectsExplicitTimeoutInEitherOrder(t *testing.T) {
	for _, options := range [][]ClientOption{
		{WithSkipKeepAlive(), WithKeepAliveTimeout(time.Second)},
		{WithKeepAliveTimeout(time.Hour), WithSkipKeepAlive()},
		{WithSkipKeepAlive(), WithKeepAliveTimeout(0)},
	} {
		if client, err := NewClient(options...); !errors.Is(err, ErrInvalidConfiguration) || client != nil {
			t.Fatalf("%v %v", client, err)
		}
	}
	if client, err := NewClient(WithGRPCConnection(nil), WithNoAuthentication(), WithSkipKeepAlive()); !errors.Is(err, ErrInvalidConfiguration) || client != nil {
		t.Fatalf("%v %v", client, err)
	}
}

// This in-memory connection exercises generation ownership with fake time, not
// network timing. Every method still passes through the actual auth transport.
func skipMemoryClient(t *testing.T, invoke func(context.Context, string) error, options ...ClientOption) (*Client, *atomic.Int32) {
	t.Helper()
	streams := &atomic.Int32{}
	conn, err := grpc.NewClient("passthrough:///skip", grpc.WithTransportCredentials(insecure.NewCredentials()),
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
		}), grpc.WithStreamInterceptor(func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, grpc.Streamer, ...grpc.CallOption) (grpc.ClientStream, error) {
			streams.Add(1)
			return nil, status.Error(codes.Unimplemented, "no application stream expected")
		}))
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClient(append([]ClientOption{WithGRPCConnection(conn), WithNoAuthentication(), WithSkipKeepAlive()}, options...)...)
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

func TestDefaultKeepAliveOpensSessionAndAcknowledgesWithoutProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var compatibility, probes, acks int
		client, streams := memoryLifecycleClient(t, func(_ context.Context, method string) error {
			switch method {
			case clients.ConnectionService_CheckCompatibility_FullMethodName:
				compatibility++
			case clients.ConnectionService_GetConnectedClients_FullMethodName:
				probes++
			case clients.ConnectionService_ConnectionKeepAlive_FullMethodName:
				acks++
			}
			return nil
		})
		if err := client.Connect(t.Context()); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if compatibility != 1 || acks != 1 || probes != 0 || len(streams) != 1 {
			t.Fatalf("compat=%d ack=%d probe=%d streams=%d", compatibility, acks, probes, len(streams))
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSkipKeepAliveCloseCancelsAndJoinsRegistrationReplay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered := make(chan struct{})
		client, _ := skipMemoryClient(t, func(ctx context.Context, method string) error {
			if method == eventstores.EventStores_EnsureEventStore_FullMethodName {
				close(entered)
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		})
		client.stores[storeKey{"store", DefaultNamespace}] = testDefinitionStore(t, &EventStore{client: client, name: "store", namespace: DefaultNamespace, catalog: client.catalog})
		if err := client.Connect(t.Context()); err != nil {
			t.Fatal(err)
		}
		<-entered
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSkipKeepAliveProbesRegistersAndSurvivesSilence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var compatibility, probes, acks, ensures int
		client, streams := skipMemoryClient(t, func(ctx context.Context, method string) error {
			md, _ := metadata.FromOutgoingContext(ctx)
			authorization := md.Get("authorization")
			if len(authorization) != 1 || authorization[0] != "Bearer external" {
				t.Fatalf("expected one matching authorization value; received %d values (credentials redacted)", len(authorization))
			}
			switch method {
			case clients.ConnectionService_CheckCompatibility_FullMethodName:
				compatibility++
			case clients.ConnectionService_GetConnectedClients_FullMethodName:
				probes++
			case clients.ConnectionService_ConnectionKeepAlive_FullMethodName:
				acks++
			case eventstores.EventStores_EnsureEventStore_FullMethodName:
				ensures++
			case "/long/read":
				time.Sleep(20 * time.Second)
			}
			return nil
		}, WithTokenSource(&invalidatingSource{}), func(c *clientConfig) { c.noAuth = false })
		// Replay owns a cached store even without a stream or watchdog.
		store := testDefinitionStore(t, &EventStore{client: client, name: "store", namespace: DefaultNamespace, catalog: client.catalog})
		client.stores[storeKey{"store", DefaultNamespace}] = store
		ctx, cancel := context.WithCancel(t.Context())
		if err := client.Connect(ctx); err != nil {
			t.Fatal(err)
		}
		cancel() // Startup lifetime is not retained.
		if err := client.Ready(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := client.transport.Invoke(t.Context(), "/long/read", nil, nil); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if compatibility != 1 || probes != 1 || acks != 0 || streams.Load() != 0 || ensures != 1 {
			t.Fatalf("compat=%d probe=%d ack=%d streams=%d ensure=%d", compatibility, probes, acks, streams.Load(), ensures)
		}
		client.mu.Lock()
		g := client.current
		client.mu.Unlock()
		if g == nil || g.stream != nil || g.id != "" || g.number != 1 {
			t.Fatal("fabricated session or silence-driven replacement")
		}
		if err := client.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		// Borrowed channel remains usable after both shutdown forms.
		if _, err := clients.NewConnectionServiceClient(client.config.borrowed).GetConnectedClients(metadata.AppendToOutgoingContext(t.Context(), "authorization", "Bearer external"), nil); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSkipKeepAliveProbeFailsClosedEvenWithoutCompatibility(t *testing.T) {
	for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied, codes.Unimplemented} {
		t.Run(code.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var probes, compatibility int
				reject := true
				source := &invalidatingSource{}
				client, streams := skipMemoryClient(t, func(_ context.Context, method string) error {
					if method == clients.ConnectionService_CheckCompatibility_FullMethodName {
						compatibility++
					}
					if method == clients.ConnectionService_GetConnectedClients_FullMethodName {
						probes++
						if reject {
							return status.Error(code, "probe rejected")
						}
					}
					return nil
				}, WithSkipCompatibilityCheck(), WithTokenSource(source), func(c *clientConfig) { c.noAuth = false })
				if err := client.Connect(t.Context()); status.Code(err) != code {
					t.Fatal(err)
				}
				synctest.Wait()
				if err := client.Ready(t.Context()); status.Code(err) != code {
					t.Fatal(err)
				}
				if compatibility != 0 || probes != 1 || streams.Load() != 0 {
					t.Fatal("anonymous readiness or swallowed probe failure")
				}
				wantInvalidations := int32(0)
				if code == codes.Unauthenticated {
					wantInvalidations = 1
				}
				if source.invalidations.Load() != wantInvalidations {
					t.Fatal("token invalidation changed")
				}
				reject = false
				if err := client.Connect(t.Context()); err != nil {
					t.Fatal(err)
				}
				if probes != 2 {
					t.Fatal("explicit Connect did not retry")
				}
			})
		})
	}
}

func TestSkipKeepAliveCompatibilityStillTerminatesStartup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		probes := 0
		client, streams := skipMemoryClient(t, func(_ context.Context, method string) error {
			if method == clients.ConnectionService_GetConnectedClients_FullMethodName {
				probes++
			}
			return status.Error(codes.Unimplemented, "compatibility unavailable")
		})
		if err := client.Connect(t.Context()); status.Code(err) != codes.Unimplemented {
			t.Fatal(err)
		}
		synctest.Wait()
		if err := client.Ready(t.Context()); status.Code(err) != codes.Unimplemented {
			t.Fatal(err)
		}
		if probes != 0 || streams.Load() != 0 {
			t.Fatal("preflight bypassed")
		}
	})
}

func TestSkipKeepAliveProbeCancellationAndCloseJoin(t *testing.T) {
	for _, mode := range []string{"cancel", "timeout", "close"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered := make(chan struct{})
				var once sync.Once
				client, streams := skipMemoryClient(t, func(ctx context.Context, method string) error {
					if method == clients.ConnectionService_GetConnectedClients_FullMethodName {
						once.Do(func() { close(entered) })
						<-ctx.Done()
						return ctx.Err()
					}
					return nil
				})
				ctx, cancel := context.WithCancel(t.Context())
				if mode == "timeout" {
					cancel()
					ctx, cancel = context.WithTimeout(t.Context(), 4*time.Second)
				}
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- client.Connect(ctx) }()
				<-entered
				switch mode {
				case "cancel":
					cancel()
				case "timeout":
					<-ctx.Done()
				case "close":
					if err := client.Close(); err != nil {
						t.Fatal(err)
					}
				}
				err := <-result
				if err == nil || (!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrClosed)) {
					t.Fatal(err)
				}
				if err := client.Close(); err != nil {
					t.Fatal(err)
				}
				if streams.Load() != 0 {
					t.Fatal("opened application stream")
				}
			})
		})
	}
}

func TestSkipKeepAliveTokenFailureDoesNotDispatchProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		client, _ := skipMemoryClient(t, func(context.Context, string) error { calls++; return nil }, WithSkipCompatibilityCheck(), WithTokenSource(failingOptionTokens{}), func(c *clientConfig) { c.noAuth = false })
		var auth *AuthenticationError
		if err := client.Connect(t.Context()); !errors.As(err, &auth) {
			t.Fatal(err)
		}
		if calls != 0 {
			t.Fatal("token failure dispatched")
		}
	})
}

func TestSkipKeepAliveRejectsConfiguredObserverRuntimesBeforeIO(t *testing.T) {
	for _, kind := range []string{"reactor", "reducer", "passive reducer", "read-model reactor"} {
		for _, perStore := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/perStore=%t", kind, perStore), func(t *testing.T) {
				registry := NewRegistry()
				if _, err := RegisterEvent[FoldChanged](registry); err != nil {
					t.Fatal(err)
				}
				var modelOptions []readmodels.ModelOption
				if kind == "passive reducer" {
					modelOptions = append(modelOptions, readmodels.Passive())
				}
				if kind == "read-model reactor" {
					modelOptions = append(modelOptions, readmodels.WithObserver(readmodels.Projection, "options-projection"))
				}
				model, err := RegisterReadModel[FoldTotal](registry, modelOptions...)
				if err != nil {
					t.Fatal(err)
				}
				switch kind {
				case "reactor":
					err = RegisterReactorHandler(registry, "options-reactor", func(context.Context, FoldChanged) error { return nil })
				case "reducer", "passive reducer":
					err = RegisterReducer[*FrozenFold](registry, model, nil, reducers.WithID("options-reducer"))
				case "read-model reactor":
					err = RegisterReadModelReactorHandlers(registry, "options-model-reactor", model, []reactors.ReadModelHandler{reactors.ReadModelOn(readmodels.Added, func(*FoldTotal) {})})
				}
				if err != nil {
					t.Fatal(err)
				}
				defaultRegistry := registry
				options := []ClientOption{WithSkipKeepAlive(), WithRegistry(registry)}
				if perStore {
					defaultRegistry = NewRegistry()
					options = []ClientOption{WithSkipKeepAlive(), WithRegistry(defaultRegistry), WithRegistryForStore("special", registry)}
				}
				prepared := false
				if err := RegisterSeederFunc(defaultRegistry, func(*seeding.Builder) error { prepared = true; return nil }); err != nil {
					t.Fatal(err)
				}
				client, err := NewClient(options...)
				if prepared {
					t.Fatal("invalid skip configuration invoked caller preparation")
				}
				if client != nil || !errors.Is(err, ErrInvalidConfiguration) || !strings.Contains(err.Error(), "logical connection session") {
					t.Fatalf("client=%v error=%v", client, err)
				}
			})
		}
	}
}

type failingOptionTokens struct{}

func (failingOptionTokens) Token(context.Context) (Token, error) {
	return Token{AccessToken: "\n"}, nil
}
