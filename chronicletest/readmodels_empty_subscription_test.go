// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	projectioncontracts "github.com/cratis/chronicle.go/contracts/projections"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"google.golang.org/grpc"
)

func TestStrictProjectionWithEmptyKernelMembershipOpensAndRejectsEverySeed(t *testing.T) {
	t.Run("public constructor", func(t *testing.T) {
		registry := subscriptionRegistry(t, "join removal only")
		kernel := &substituteKernel{}
		rpc := grpc.NewServer()
		clients.RegisterConnectionServiceServer(rpc, kernel)
		eventstores.RegisterEventStoresServer(rpc, kernel)
		eventtypes.RegisterEventTypesServer(rpc, kernel)
		namespaces.RegisterNamespacesServer(rpc, kernel)
		projectioncontracts.RegisterProjectionsServer(rpc, &subscriptionRegistration{})
		modelcontracts.RegisterReadModelsServer(rpc, &subscriptionModels{})
		var requests atomic.Uint64
		// This server accepts registration only; it does not evaluate projections.
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			if strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
				rpc.ServeHTTP(w, r)
				return
			}
			if r.URL.Path != "/connect/token" {
				t.Errorf("unexpected HTTP route: %s", r.URL.Path)
				http.NotFound(w, r)
				return
			}
			if _, err := w.Write([]byte(`{"access_token":"token","expires_in":3600}`)); err != nil {
				t.Error(err)
			}
		}))
		server.EnableHTTP2 = true
		server.StartTLS()
		t.Cleanup(func() { rpc.Stop(); server.Close() })
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		scenario, err := OpenReadModelScenario[subscriptionModel](ctx, Config{
			Registry: registry, Engine: Kernel, Development: true,
			ConnectionString: "chronicle://" + strings.TrimPrefix(server.URL, "https://"),
		}, ReadModelOptions[subscriptionModel]{StrictEventSubscription: true})
		if err != nil {
			t.Fatalf("valid root-RemovedWithJoin-only projection did not open: %v", err)
		}
		t.Cleanup(func() {
			if err := scenario.Close(); err != nil {
				t.Error(err)
			}
		})
		before := requests.Load()
		assertEmptySubscriptionRejectsEverySeed(t, scenario)
		if requests.Load() != before {
			t.Fatal("rejected seed invoked RPC or OAuth")
		}
	})
	t.Run("instrumented seed effects", func(t *testing.T) {
		f := newSubscriptionFixture(t, true, "join removal only")
		beforeUnary, beforeStreams := f.connection.unaryCalls.Load(), f.connection.streamCalls.Load()
		assertEmptySubscriptionRejectsEverySeed(t, f.scenario)
		if f.connection.unaryCalls.Load() != beforeUnary || f.connection.streamCalls.Load() != beforeStreams || f.providers.Load() != 0 || f.appendServer.calls != 0 {
			t.Fatal("rejected seed invoked RPC, providers or append")
		}
	})
}

func assertEmptySubscriptionRejectsEverySeed(t *testing.T, scenario *ReadModelScenario[subscriptionModel]) {
	t.Helper()
	wire := scenario.projection.KernelDefinition()
	if scenario.subscription == nil || len(scenario.subscription.ids) != 0 || len(wire.RemovedWithJoin) != 1 || len(wire.From) != 0 || len(wire.RemovedWith) != 0 {
		t.Fatal("fixture is not an empty resolver set with only a root join removal")
	}
	descriptors := scenario.artifacts.Events.Descriptors()
	if len(descriptors) == 0 {
		t.Fatal("fixture has no registered seeds to reject")
	}
	rejectedCodecCalls.Store(0)
	rejectedErrorHooks.Store(0)
	for _, descriptor := range descriptors {
		value := reflect.New(descriptor.GoType()).Elem().Interface()
		if err := scenario.Given(t.Context(), "source", value); !errors.Is(err, ErrUnsubscribedEventSeeded) {
			t.Fatalf("seed %s = %v; want strict rejection", descriptor.Ref().ID, err)
		}
	}
	if rejectedCodecCalls.Load() != 0 || rejectedErrorHooks.Load() != 0 || len(scenario.history) != 0 {
		t.Fatal("rejected seed invoked codecs/error hooks or changed history")
	}
}
