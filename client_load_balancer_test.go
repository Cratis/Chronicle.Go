// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type testLoadBalancer struct {
	next   func(context.Context, []ServerAddress) (ServerAddress, error)
	closed atomic.Bool
}

func (b *testLoadBalancer) Next(ctx context.Context, candidates []ServerAddress) (ServerAddress, error) {
	return b.next(ctx, candidates)
}
func (b *testLoadBalancer) Close() error { b.closed.Store(true); return nil }

func TestLoadBalancerValidationAndLastWins(t *testing.T) {
	valid := &testLoadBalancer{}
	borrowed, err := grpc.NewClient("passthrough:///validation", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := borrowed.Close(); err != nil {
			t.Error(err)
		}
	})
	var typedNil *testLoadBalancer
	for name, options := range map[string][]ClientOption{
		"nil":               {WithLoadBalancer(nil)},
		"typed nil":         {WithLoadBalancer(typedNil)},
		"URI conflict":      {WithLoadBalancer(valid), WithConnectionString("chronicle://test?loadBalancer=random")},
		"borrowed conflict": {WithLoadBalancer(valid), WithGRPCConnection(borrowed), WithNoAuthentication()},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewClient(options...); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatal(err)
			}
		})
	}
	client, err := NewClient(WithLoadBalancer(nil), WithLoadBalancer(valid))
	if err != nil {
		t.Fatal(err)
	}
	if client.config.loadBalancer != valid {
		t.Fatal("last option did not win")
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	if valid.closed.Load() {
		t.Fatal("borrowed balancer closed")
	}
}

func balancerRegistry(t *testing.T) *Registry {
	t.Helper()
	registry := NewRegistry()
	if _, err := RegisterEvent[lifecycleEvent](registry); err != nil {
		t.Fatal(err)
	}
	return registry
}

func discoveryAddress(record *net.SRV) ServerAddress {
	return ServerAddress{Host: record.Target, Port: record.Port}
}

func TestLoadBalancerSelectsEveryGenerationAndCopiesURICandidates(t *testing.T) {
	first, record1, tokens1 := discoveryServer(t)
	second, record2, tokens2 := discoveryServer(t)
	want := []ServerAddress{discoveryAddress(record1), discoveryAddress(record2)}
	calls := make(chan []ServerAddress, 4)
	var count int
	balancer := &testLoadBalancer{next: func(_ context.Context, candidates []ServerAddress) (ServerAddress, error) {
		calls <- append([]ServerAddress(nil), candidates...)
		selected := candidates[count%2]
		count++
		candidates[0] = ServerAddress{Host: "mutated", Port: 1}
		return selected, nil
	}}
	client, err := NewClient(WithConnectionString("chronicle://"+want[0].String()+","+want[1].String()), WithLoadBalancer(balancer), WithDevelopmentDefaults(), WithRegistry(balancerRegistry(t)), WithKeepAliveTimeout(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.WaitForRegistration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, ctx, first.registered)
	first.endStream <- status.Error(codes.Unavailable, "move")
	awaitSignal(t, ctx, second.registered)
	after, err := store.WaitForRegistration(ctx)
	if err != nil || after.Generation <= before.Generation {
		t.Fatalf("generation: %+v %v", after, err)
	}
	for range 2 {
		select {
		case got := <-calls:
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("candidates=%v want=%v", got, want)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if tokens1.Load() != 1 || tokens2.Load() != 1 {
		t.Fatalf("OAuth authority=%d/%d", tokens1.Load(), tokens2.Load())
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	if balancer.closed.Load() {
		t.Fatal("borrowed balancer closed")
	}
}

func TestLoadBalancerErrorsAreTransientAndRedacted(t *testing.T) {
	cause := status.Error(codes.Unauthenticated, "secret")
	failure := &LoadBalancerError{Err: cause}
	if terminalConnectionError(failure) || !errors.Is(failure, cause) {
		t.Fatal("lost transient/cause contract")
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
		if got := fmt.Sprintf(format, failure); got != failure.Error() {
			t.Fatalf("unsafe format: %s", got)
		}
	}
}
