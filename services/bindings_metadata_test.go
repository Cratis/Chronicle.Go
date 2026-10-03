// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services_test

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/services"
	"github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

func TestFacadeSourceExampleUsesRealStoreAPI(t *testing.T) {
	kernel := &facadeKernel{}
	conn := facadeConnection(t, kernel)
	ctx := context.WithValue(facadeContext(t), sourceStoreKey{}, SourceStoreMetadata{Store: "orders", Namespace: "north", Principal: "host-principal"})
	if err := useSourceFacades(ctx, chronicle.WithGRPCConnection(conn), chronicle.WithNoAuthentication()); err != nil {
		t.Fatal(err)
	}
	kernel.mu.Lock()
	defer kernel.mu.Unlock()
	if len(kernel.coordinates) != 1 || kernel.coordinates[0] != [2]string{"orders", "north"} {
		t.Fatal("example selected wrong coordinates", kernel.coordinates)
	}
}

func TestFacadeContextGuardBelongsToHostAndChecksPrincipalOnCachedResolution(t *testing.T) {
	ctx := facadeContext(t)
	trusted := SourceStoreMetadata{Store: "orders", Namespace: "north", Principal: "host-principal"}
	operation := context.WithValue(ctx, sourceStoreKey{}, trusted)
	p := captureFacadeClient(t, &facadeKernel{})
	var bindings container.Registry
	calls := 0
	bindFacades(t, &bindings, p, func(ctx context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
		calls++
		return selectSourceStore(ctx)
	})
	provider := facadeProvider(t, &bindings, p.Client(), container.WithContextGuard(sourceStoreGuard))
	if _, err := services.PrepareClient(operation, p, provider); err != nil {
		t.Fatal(err)
	}
	scope := facadeScope(t, operation, provider)
	selected := resolveFacade[*chronicle.EventStore](t, operation, scope)
	for _, changed := range []SourceStoreMetadata{
		{Store: "orders", Namespace: "south", Principal: trusted.Principal},
		{Store: "orders", Namespace: "north", Principal: "other-principal"},
		{Store: "other-store", Namespace: "north", Principal: trusted.Principal},
	} {
		if _, err := dependencyinjection.Resolve[*chronicle.EventStore](context.WithValue(ctx, sourceStoreKey{}, changed), scope); !errors.Is(err, dependencyinjection.ErrContextMismatch) {
			t.Fatal("cached store bypassed host guard", err)
		}
	}
	if _, err := dependencyinjection.Resolve[*chronicle.EventStore](ctx, scope); !errors.Is(err, dependencyinjection.ErrContextMismatch) {
		t.Fatal("presence mismatch bypassed guard", err)
	}
	if got := resolveFacade[*chronicle.EventStore](t, operation, scope); got != selected || calls != 1 {
		t.Fatal("guard caused reselection")
	}
}
