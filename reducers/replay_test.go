// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reducers_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/reducers"
)

func TestReplayNotificationPreservesAmbientMetadataAndCleanupCauses(t *testing.T) {
	catalog, model, models := catalog(t)
	var trace []string
	failure := errors.New("handler failed")
	cleanupFailure := errors.New("scope cleanup failed")
	identity := identities.Identity{Subject: "caller", Name: "notification caller"}
	ctx := metadata.WithIdentity(t.Context(), identity)
	correlation, err := metadata.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	ctx = metadata.WithCorrelation(ctx, correlation)
	ctx = metadata.WithCausation(ctx, metadata.Causation{Type: "original cause"})
	var notificationContext context.Context
	callbacks := reducers.ReplayCallbacks{BeginReplay: func(received context.Context) error {
		notificationContext = received
		trace = append(trace, "notification")
		if !reflect.DeepEqual(metadata.Identity(received), identity) || metadata.Correlation(received) != metadata.Correlation(ctx) || !reflect.DeepEqual(metadata.CausationChain(received), metadata.CausationChain(ctx)) {
			t.Error("notification installed fabricated fold metadata")
		}
		return failure
	}}
	d, err := reducers.DefineHandlers(model, "replay", []reducers.Handler{reducers.On(func(context.Context, Changed, *Total, events.Context) (*Total, error) {
		t.Error("notification folded")
		return nil, nil
	})}, reducers.WithReplayCallbacks(callbacks))
	if err != nil {
		t.Fatal(err)
	}
	plan, err := reducers.Compile(d, catalog, models, &scopes{trace: &trace, fail: cleanupFailure})
	if err != nil {
		t.Fatal(err)
	}
	err = plan.NotifyReplay(ctx, reducers.BeginReplay, "")
	var activation *reducers.ActivationError
	if !errors.Is(err, failure) || !errors.Is(err, cleanupFailure) || !errors.As(err, &activation) || !slices.Equal(trace, []string{"open", "notification", "scope"}) {
		t.Fatal(err, trace)
	}
	if !errors.Is(notificationContext.Err(), context.Canceled) {
		t.Error("notification operation context outlived callback cleanup")
	}
	trace = nil
	if err := plan.NotifyReplay(ctx, reducers.ReplayState(99), ""); err == nil || len(trace) != 0 {
		t.Fatal(err, trace)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := plan.NotifyReplay(canceled, reducers.EndReplay, ""); !errors.Is(err, context.Canceled) || len(trace) != 0 {
		t.Fatal(err, trace)
	}
}

func TestReplayNotificationActivationFailureIsClosedBeforeReturn(t *testing.T) {
	catalog, model, models := catalog(t)
	var trace []string
	failure := errors.New("activation failed")
	d, err := reducers.Define[*ownedFold](model, func() (*ownedFold, error) { return &ownedFold{&trace}, failure })
	if err != nil {
		t.Fatal(err)
	}
	plan, err := reducers.Compile(d, catalog, models, &scopes{trace: &trace})
	if err != nil {
		t.Fatal(err)
	}
	err = plan.NotifyReplay(t.Context(), reducers.BeginReplay, "")
	var activation *reducers.ActivationError
	if !errors.Is(err, failure) || !errors.As(err, &activation) || !slices.Equal(trace, []string{"open", "artifact", "scope"}) {
		t.Fatal(err, trace)
	}
}
