// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"fmt"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/eventsequences"
)

func TestClientAppendOriginResolverFrozenAndSharedAcrossStoreCoordinates(t *testing.T) {
	origin := eventsequences.NewOrigin()
	var resolutions int
	var client *chronicle.Client
	resolver := eventsequences.AppendOriginResolver(func(ctx context.Context) (eventsequences.Origin, bool, error) {
		resolutions++
		// Reenter client/store caches: resolver invocation must hold neither lock.
		store, err := client.EventStore(ctx, "customers")
		if err != nil {
			return eventsequences.Origin{}, false, err
		}
		other, err := store.EventSequence("from-callback")
		if err != nil {
			return eventsequences.Origin{}, false, err
		}
		other.OnAppend(func(eventsequences.AppendNotification) {})()
		return origin, true, nil
	})
	option := chronicle.WithAppendOriginResolver(resolver)
	resolver = func(context.Context) (eventsequences.Origin, bool, error) {
		panic("earlier option must be replaced; option must retain its original callback")
	}
	client, _ = testClient(t, &fakeKernel{}, chronicle.WithAppendOriginResolver(resolver), option)
	ctx := eventsequences.WithOrigin(testContext(t), eventsequences.NewOrigin())
	for _, coordinates := range []struct {
		store     chronicle.StoreName
		namespace chronicle.Namespace
	}{{"customers", "default"}, {"customers", "other"}, {"other", "default"}} {
		store, err := client.EventStore(ctx, coordinates.store, chronicle.WithNamespace(coordinates.namespace))
		if err != nil {
			t.Fatal(err)
		}
		sequence, err := store.EventSequence("custom")
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range []*eventsequences.Sequence{store.EventLog(), sequence} {
			var received []eventsequences.AppendNotification
			unsubscribe := s.OnAppend(func(n eventsequences.AppendNotification) { received = append(received, n) })
			result, err := s.Append(ctx, "A", CustomerRegistered{Name: "Ada"}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
			unsubscribe()
			if err != nil || result.Disposition != eventsequences.Committed || len(received) != 1 || received[0].Origin != origin {
				t.Fatalf("result=%+v err=%v notifications=%+v", result, err, received)
			}
		}
	}
	if resolutions != 6 {
		t.Fatalf("resolutions=%d, want 6", resolutions)
	}
}

func TestClientNilAppendOriginResolverDisablesEarlierOption(t *testing.T) {
	client, _ := testClient(t, &fakeKernel{}, chronicle.WithAppendOriginResolver(func(context.Context) (eventsequences.Origin, bool, error) {
		panic("nil must disable resolution")
	}), chronicle.WithAppendOriginResolver(nil))
	origin := eventsequences.NewOrigin()
	ctx := eventsequences.WithOrigin(testContext(t), origin)
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	var received []eventsequences.AppendNotification
	defer store.EventLog().OnAppend(func(n eventsequences.AppendNotification) { received = append(received, n) })()
	if _, err := store.EventLog().Append(ctx, "A", CustomerRegistered{Name: "Ada"}); err != nil {
		t.Fatal(err)
	}
	if len(received) != 1 || received[0].Origin != origin {
		t.Fatal("nil resolver changed existing OriginFrom behavior")
	}
}

type invocationOriginKey struct{}

func ExampleWithAppendOriginResolver() {
	// An adapter reads the current invocation's snapshot, never a captured
	// request context. Chronicle needs no dependency on the command framework.
	resolver := eventsequences.AppendOriginResolver(func(ctx context.Context) (eventsequences.Origin, bool, error) {
		origin, handled := ctx.Value(invocationOriginKey{}).(eventsequences.Origin)
		return origin, handled, nil
	})
	client, err := chronicle.NewClient(chronicle.WithNoAuthentication(), chronicle.WithAppendOriginResolver(resolver))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = client.Close() }()
	origin := eventsequences.NewOrigin()
	ctx := context.WithValue(context.Background(), invocationOriginKey{}, origin)
	selected, handled, err := resolver(ctx)
	fmt.Println("current invocation:", selected == origin && handled && err == nil)
	// Output: current invocation: true
}
