// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services_test

import (
	"context"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/services"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

func TestEvictionPreservesBorrowedScopeAndCapturedSelection(t *testing.T) {
	ctx := facadeContext(t)
	p := captureFacadeClient(t, &facadeKernel{})
	var bindings container.Registry
	selections := 0
	bindFacades(t, &bindings, p, func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
		selections++
		return "first", "north", nil
	})
	provider := facadeProvider(t, &bindings, p.Client())
	client, err := services.PrepareClient(ctx, p, provider)
	if err != nil {
		t.Fatal(err)
	}
	oldScope := facadeScope(t, ctx, provider)
	old := resolveFacade[*chronicle.EventStore](t, ctx, oldScope)
	evictor, ok := any(client).(interface{ EvictEventStores() error })
	if !ok {
		t.Fatal("Client.EvictEventStores is missing")
	}
	if err := evictor.EvictEventStores(); err != nil {
		t.Fatal(err)
	}
	if same := resolveFacade[*chronicle.EventStore](t, ctx, oldScope); same != old || selections != 1 {
		t.Fatal("eviction changed a resolved scope")
	}
	freshScope := facadeScope(t, ctx, provider)
	fresh := resolveFacade[*chronicle.EventStore](t, ctx, freshScope)
	if fresh == old || fresh.Name() != old.Name() || fresh.Namespace() != old.Namespace() || selections != 2 {
		t.Fatal("new scope did not acquire new cache facade")
	}
	if resolveFacade[*chronicle.Client](t, ctx, freshScope) != client {
		t.Fatal("client identity changed")
	}
	if err := oldScope.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := freshScope.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := provider.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := old.WaitForRegistration(ctx); err != nil {
		t.Fatal("provider disposed borrowed resources", err)
	}
}
