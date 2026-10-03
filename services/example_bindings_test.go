// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/services"
	"github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

// SourceStoreMetadata is application-owned metadata installed by a trusted host
// after authentication/authorization, not values read from a message payload.
type SourceStoreMetadata struct {
	Store     chronicle.StoreName
	Namespace chronicle.Namespace
	Principal string
}
type sourceStoreKey struct{}

func selectSourceStore(ctx context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
	metadata, ok := ctx.Value(sourceStoreKey{}).(SourceStoreMetadata)
	if !ok {
		return "", "", errors.New("source store metadata missing")
	}
	return metadata.Store, metadata.Namespace, nil
}

func sourceStoreGuard(ctx context.Context) (dependencyinjection.ContextCheck, error) {
	captured, present := ctx.Value(sourceStoreKey{}).(SourceStoreMetadata)
	return func(actual context.Context) error {
		metadata, ok := actual.Value(sourceStoreKey{}).(SourceStoreMetadata)
		if ok != present || metadata != captured {
			return dependencyinjection.ErrContextMismatch
		}
		return nil
	}, nil
}

// This compiled example needs a reachable development kernel to execute.
func ExampleBindEventStore() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, sourceStoreKey{}, SourceStoreMetadata{Store: "orders", Namespace: "north", Principal: "host-principal"})
	if err := useSourceFacades(ctx, chronicle.WithDevelopmentDefaults()); err != nil {
		panic(err)
	}
}

func useSourceFacades(ctx context.Context, options ...chronicle.ClientOption) (err error) {
	// Origin configuration must be captured before preparation. This illustrative
	// host forwards existing local attribution; an Arc host supplies its own
	// SDK ResolveAppendOrigin callback here instead, without a Chronicle dependency
	// on Arc. Neither callback authorizes namespace access.
	options = append(options, chronicle.WithAppendOriginResolver(func(ctx context.Context) (eventsequences.Origin, bool, error) {
		return eventsequences.OriginFrom(ctx), true, nil
	}))
	p, err := chronicle.CaptureClient(options...)
	if err != nil {
		return err
	}
	var provider dependencyinjection.Provider
	defer func() {
		err = errors.Join(err, p.Client().Close())
		if provider != nil {
			err = errors.Join(err, provider.Close(context.WithoutCancel(ctx)))
		}
	}()
	var bindings container.Registry
	if err = services.BindClient(&bindings, p.Client()); err != nil {
		return err
	}
	if err = services.BindEventStore(&bindings, selectSourceStore); err != nil {
		return err
	}
	if err = services.BindEventLog(&bindings); err != nil {
		return err
	}
	if err = services.BindEventTypes(&bindings); err != nil {
		return err
	}
	// Resolve the exact concrete key and forward the same borrowed instance.
	if err = dependencyinjection.BindBorrowed[catalogReader](&bindings, dependencyinjection.Scoped,
		func(ctx context.Context, r dependencyinjection.Resolver) (catalogReader, error) {
			return dependencyinjection.Resolve[*events.Catalog](ctx, r)
		}, dependencyinjection.KeyFor[*events.Catalog]()); err != nil {
		return err
	}
	provider, err = bindings.Build(container.WithContextGuard(sourceStoreGuard))
	if err != nil {
		return err
	}
	client, err := services.PrepareClient(ctx, p, provider)
	if err != nil {
		return err
	}
	// Catalog-reading integrations (including Arc sdk.New) belong here, after
	// preparation. This example does not import or assert adoption by Arc.
	scope, err := provider.NewScope(ctx)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, scope.Close(context.WithoutCancel(ctx))) }()
	selected, err := dependencyinjection.Resolve[*chronicle.EventStore](ctx, scope)
	if err != nil {
		return err
	}
	log, err := dependencyinjection.Resolve[*eventsequences.Sequence](ctx, scope)
	if err != nil {
		return err
	}
	// Plain construction stays first-class and shares the same SDK cache.
	plain, err := client.EventStore(ctx, selected.Name(), chronicle.WithNamespace(selected.Namespace()))
	if err != nil {
		return err
	}
	if plain != selected || log != selected.EventLog() {
		return errors.New("facade identity changed")
	}
	fmt.Println(true, true)
	return nil
}

// AuditLog is an application key, not an SDK named/keyed service mechanism.
type AuditLog struct{ Sequence *eventsequences.Sequence }

// This compiled illustration shows a second fixed target using existing generic
// BindBorrowed. Both targets borrow the same client; neither owns SDK resources.
func ExampleBindEventStore_multipleTargets() {
	ctx := context.Background()
	p, err := chronicle.CaptureClient()
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := p.Client().Close(); err != nil {
			panic(err)
		}
	}()
	var bindings container.Registry
	if err := services.BindClient(&bindings, p.Client()); err != nil {
		panic(err)
	}
	if err := services.BindEventStore(&bindings, selectSourceStore); err != nil {
		panic(err)
	}
	if err := dependencyinjection.BindBorrowed(&bindings, dependencyinjection.Scoped,
		func(ctx context.Context, r dependencyinjection.Resolver) (*AuditLog, error) {
			client, err := dependencyinjection.Resolve[*chronicle.Client](ctx, r)
			if err != nil {
				return nil, err
			}
			store, err := client.EventStore(ctx, "audit", chronicle.WithNamespace("host-audit"))
			if err != nil {
				return nil, err
			}
			return &AuditLog{Sequence: store.EventLog()}, nil
		}, dependencyinjection.KeyFor[*chronicle.Client]()); err != nil {
		panic(err)
	}
	provider, err := bindings.Build()
	if err != nil {
		panic(err)
	}
	if _, err := services.PrepareClient(ctx, p, provider); err != nil {
		panic(err)
	}
	// Stop/join application work before coordinated shutdown.
	if err := p.Client().Close(); err != nil {
		panic(err)
	}
	if err := provider.Close(ctx); err != nil {
		panic(err)
	}
	fmt.Println("distinct concrete keys; shared borrowed client")
	// Output: distinct concrete keys; shared borrowed client
}
