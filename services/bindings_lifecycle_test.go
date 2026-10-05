// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	chronicle "github.com/cratis/chronicle.go"
	di "github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

func TestFacadeAdmittedProviderWaiterCancellationWhileSelectionBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, err := chronicle.CaptureClient()
		if err != nil {
			t.Fatal(err)
		}
		failure := errors.New("frozen selector failure")
		started, release := make(chan struct{}), make(chan struct{})
		var calls atomic.Int32
		var bindings container.Registry
		if err := BindClient(&bindings, p.Client()); err != nil {
			t.Fatal(err)
		}
		if err := BindEventStore(&bindings, func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
			calls.Add(1)
			close(started)
			<-release
			return "", "", failure
		}); err != nil {
			t.Fatal(err)
		}
		provider, err := bindings.Build()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := p.Client().Close(); err != nil {
				t.Error(err)
			}
			if err := provider.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		scope, err := provider.NewScope(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		leader := make(chan error, 1)
		go func() { _, err := di.Resolve[*chronicle.EventStore](context.Background(), scope); leader <- err }()
		<-started
		waiterCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		waiter := make(chan error, 1)
		go func() { _, err := di.Resolve[*chronicle.EventStore](waiterCtx, scope); waiter <- err }()
		// Both resolutions are durably blocked: the live waiter reached the
		// provider's in-flight entry before cancellation, not pre-admission.
		synctest.Wait()
		select {
		case <-waiter:
			t.Fatal("waiter did not wait for leader")
		default:
		}
		cancel()
		if err := <-waiter; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		select {
		case <-leader:
			t.Fatal("waiter cancellation released leader")
		default:
		}
		close(release)
		if err := <-leader; !errors.Is(err, failure) {
			t.Fatal(err)
		}
		if _, err := di.Resolve[*chronicle.EventStore](context.Background(), scope); !errors.Is(err, failure) {
			t.Fatal("frozen outcome changed", err)
		}
		if calls.Load() != 1 {
			t.Fatal("waiter reselected")
		}
		if err := scope.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := p.Prepare(context.Background(), nil); err != nil {
			t.Fatal("scope disposed borrowed captured client", err)
		}
	})
}

func TestFacadeScopeCloseJoinsBlockedSelectionAfterCallbackClientClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, err := chronicle.CaptureClient()
		if err != nil {
			t.Fatal(err)
		}
		failure := errors.New("frozen selector failure")
		started, release := make(chan struct{}), make(chan struct{})
		var bindings container.Registry
		if err := BindClient(&bindings, p.Client()); err != nil {
			t.Fatal(err)
		}
		if err := BindEventStore(&bindings, func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
			// Client closure inside a selector cannot self-join provider work.
			if err := p.Client().Close(); err != nil {
				t.Error(err)
			}
			close(started)
			<-release
			return "", "", failure
		}); err != nil {
			t.Fatal(err)
		}
		provider, err := bindings.Build()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := provider.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
		scope, err := provider.NewScope(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		leader := make(chan error, 1)
		go func() { _, err := di.Resolve[*chronicle.EventStore](context.Background(), scope); leader <- err }()
		<-started
		// A canceled cleanup wait does not cancel the selector or release its
		// resources. A later live Close joins the caller-owned resolution.
		closeCtx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := scope.Close(closeCtx); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		closed := make(chan error, 1)
		go func() { closed <- scope.Close(context.Background()) }()
		synctest.Wait()
		select {
		case <-closed:
			t.Fatal("scope close failed to join selection")
		default:
		}
		if _, err := di.Resolve[*chronicle.EventStore](context.Background(), scope); !errors.Is(err, di.ErrClosed) {
			t.Fatal(err)
		}
		close(release)
		if err := <-leader; !errors.Is(err, failure) {
			t.Fatal("admitted selector failure lost", err)
		}
		if err := <-closed; err != nil {
			t.Fatal(err)
		}
		if err := scope.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}
