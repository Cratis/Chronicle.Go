// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/alerts"
	"github.com/cratis/chronicle.go/eventsources"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAlertsAndEventSourcesFacadesKeepRegistrationAndCloseBarrier(t *testing.T) {
	var passes atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	kernel := &supervisedKernel{register: func(ctx context.Context) error {
		if passes.Add(1) == 2 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}}
	client, ctx := supervisionClient(t, kernel)
	store, err := client.EventStore(ctx, "store", WithNamespace("tenant"))
	if err != nil {
		t.Fatal(err)
	}
	incidentQueries, sources := store.Alerts(), store.EventSources()
	assertSourceFacadeCoordinates(t, incidentQueries, sources)
	operations := map[string]func(context.Context) error{
		"alerts": func(ctx context.Context) error {
			_, err := incidentQueries.GetOpenIncidents(ctx, alerts.Query{})
			return err
		},
		"event sources": func(ctx context.Context) error {
			return sources.Register(ctx, eventsources.Definition{Name: "Billing"})
		},
	}
	kernel.endStream <- status.Error(codes.Unavailable, "reconnect")
	awaitSignal(t, ctx, entered)
	for name, operation := range operations {
		t.Run(name+" waits for registration", func(t *testing.T) {
			callCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
			defer cancel()
			assertSourceFacadeBarrier(t, operation(callCtx), context.DeadlineExceeded)
		})
	}
	close(release)
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	for name, operation := range operations {
		t.Run(name+" refuses closed client", func(t *testing.T) {
			assertSourceFacadeBarrier(t, operation(ctx), ErrClosed)
		})
	}
}

func assertSourceFacadeCoordinates(t *testing.T, incidents *alerts.Service, sources *eventsources.Service) {
	t.Helper()
	if incidents.Store() != "store" || incidents.Namespace() != "tenant" {
		t.Fatal("alert coordinates lost")
	}
	if sources.Store() != "store" {
		t.Fatal("source coordinates lost")
	}
}

func assertSourceFacadeBarrier(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	var unknown *eventsources.OutcomeUnknownError
	if errors.As(err, &unknown) {
		t.Fatalf("before-dispatch failure marked outcome-unknown: %v", err)
	}
}
