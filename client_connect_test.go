// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type startupTokens struct {
	once             sync.Once
	entered, release chan struct{}
	callback         func()
}

func (s *startupTokens) Token(ctx context.Context) (Token, error) {
	if s.callback != nil {
		s.callback()
	}
	s.once.Do(func() {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
		}
	})
	if err := ctx.Err(); err != nil {
		return Token{}, err
	}
	return Token{AccessToken: "valid"}, nil
}

func TestConnectSharesStartupWithoutRetainingCallerLifetime(t *testing.T) {
	source := &startupTokens{entered: make(chan struct{}), release: make(chan struct{})}
	client, ctx := supervisionClient(t, &supervisedKernel{}, WithTokenSource(source))
	// A user callback may inspect client state without being called under its lock.
	source.callback = func() {
		client.mu.Lock()
		initialized := client.life != nil
		client.mu.Unlock()
		if !initialized {
			t.Error("callback ran before client initialization")
		}
	}
	startup, cancelStartup := context.WithCancel(ctx)
	connected := make(chan error, 1)
	go func() { connected <- client.Connect(startup) }()
	awaitSignal(t, ctx, source.entered)
	waiter, cancelWaiter := context.WithCancel(ctx)
	cancelWaiter()
	if err := client.Connect(waiter); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	client.mu.Lock()
	current := client.current
	client.mu.Unlock()
	if current != nil {
		t.Fatal("construction claimed readiness before authentication")
	}
	close(source.release)
	if err := <-connected; err != nil {
		t.Fatal(err)
	}
	cancelStartup()
	if err := client.Ready(ctx); err != nil {
		t.Fatal("startup context retained", err)
	}
}

func TestCanceledStartupJoinsAndAllowsRetry(t *testing.T) {
	source := &startupTokens{entered: make(chan struct{}), release: make(chan struct{})}
	client, ctx := supervisionClient(t, &supervisedKernel{}, WithTokenSource(source))
	startup, cancel := context.WithCancel(ctx)
	connected := make(chan error, 1)
	go func() { connected <- client.Connect(startup) }()
	awaitSignal(t, ctx, source.entered)
	cancel()
	if err := <-connected; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(source.release)
	client.mu.Lock()
	supervisor := client.supervisor
	client.mu.Unlock()
	awaitSignal(t, ctx, supervisor.first)
	if err := client.Ready(ctx); err != nil {
		t.Fatal("startup cancellation poisoned supervisor", err)
	}
}
