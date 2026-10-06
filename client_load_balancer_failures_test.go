// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestLoadBalancerFailuresRetryBeforeOAuthOrRPC(t *testing.T) {
	for _, mode := range []string{"error", "non-candidate", "panic", "unauthenticated"} {
		t.Run(mode, func(t *testing.T) {
			_, record, tokens, requests := discoveryServerWithRPCCount(t)
			address := discoveryAddress(record)
			allow := make(chan struct{})
			var calls atomic.Int32
			balancer := &testLoadBalancer{next: func(ctx context.Context, candidates []ServerAddress) (ServerAddress, error) {
				if calls.Add(1) > 1 {
					select {
					case <-allow:
						return candidates[0], nil
					case <-ctx.Done():
						return ServerAddress{}, ctx.Err()
					}
				}
				switch mode {
				case "error":
					return ServerAddress{}, errors.New("private failure")
				case "unauthenticated":
					return ServerAddress{}, status.Error(codes.Unauthenticated, "private")
				case "panic":
					panic("private panic")
				default:
					return ServerAddress{Host: "not-a-candidate", Port: 1}, nil
				}
			}}
			client, err := NewClient(WithConnectionString("chronicle://"+address.String()), WithLoadBalancer(balancer), WithDevelopmentDefaults(), WithKeepAliveTimeout(time.Minute))
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
			err = client.Connect(ctx)
			var failed *LoadBalancerError
			if !errors.As(err, &failed) {
				t.Fatalf("Connect=%v", err)
			}
			if tokens.Load() != 0 || requests.Load() != 0 {
				t.Fatal("failed selection performed I/O")
			}
			close(allow)
			if err = client.Ready(ctx); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 2 || tokens.Load() != 1 || requests.Load() == 0 {
				t.Fatalf("calls=%d tokens=%d requests=%d", calls.Load(), tokens.Load(), requests.Load())
			}
		})
	}
}

func TestLoadBalancerAttemptCanceledByClose(t *testing.T) {
	entered, exited := make(chan struct{}), make(chan struct{})
	balancer := &testLoadBalancer{next: func(ctx context.Context, _ []ServerAddress) (ServerAddress, error) {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("attempt has no deadline")
		}
		close(entered)
		<-ctx.Done()
		close(exited)
		return ServerAddress{}, ctx.Err()
	}}
	client, err := NewClient(WithLoadBalancer(balancer), WithConnectTimeout(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- client.Connect(t.Context()) }()
	awaitSignal(t, t.Context(), entered)
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, t.Context(), exited)
	if err = <-result; !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestLoadBalancerSRVRefreshesCandidates(t *testing.T) {
	first, record1, _ := discoveryServer(t)
	second, record2, _ := discoveryServer(t)
	resolver := &rotatingResolver{first: record1, second: record2}
	seen := make(chan []ServerAddress, 2)
	balancer := &testLoadBalancer{next: func(_ context.Context, candidates []ServerAddress) (ServerAddress, error) {
		seen <- candidates
		return candidates[0], nil
	}}
	client, err := NewClient(WithConnectionString("chronicle+srv://cluster"), WithSRVResolver(resolver), WithLoadBalancer(balancer), WithDevelopmentDefaults(), WithRegistry(balancerRegistry(t)), WithKeepAliveTimeout(time.Minute))
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
	if _, err = client.EventStore(ctx, "store"); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, ctx, first.registered)
	first.endStream <- status.Error(codes.Unavailable, "rediscover")
	awaitSignal(t, ctx, second.registered)
	for _, want := range []ServerAddress{discoveryAddress(record1), discoveryAddress(record2)} {
		select {
		case got := <-seen:
			if len(got) != 1 || got[0] != want {
				t.Fatalf("SRV candidates=%v want=%v", got, want)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
