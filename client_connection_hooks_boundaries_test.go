// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"testing/synctest"
)

func TestConnectionHooksPanicsContainedAndLogged(t *testing.T) {
	logger := &recordingHandler{}
	entered := make(chan struct{})
	client, ctx := supervisionClient(t, &supervisedKernel{}, WithLogger(slog.New(logger)),
		WithOnConnected(func(context.Context, ConnectionEvent) { close(entered); panic("secret connected") }),
		WithOnDisconnected(func(context.Context, ConnectionEvent) { panic("secret disconnected") }))
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, ctx, entered)
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	counts := make(map[string]int)
	for _, record := range logger.snapshot() {
		if strings.Contains(record.Message, "secret") {
			t.Fatal("panic text logged")
		}
		attrs := make(map[string]string)
		record.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value.String()
			if strings.Contains(a.Value.String(), "secret") {
				t.Error("panic value logged")
			}
			return true
		})
		if attrs["category"] == "panic" && attrs["operation"] == "client" {
			counts[attrs["stage"]]++
		}
	}
	if counts["connected"] != 1 || counts["disconnected"] != 1 {
		t.Fatal(counts)
	}
}

func TestConnectionHooksSkipKeepAliveOnlyPairsAtShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		events := make(chan hookNotification, 4)
		hook := func(ctx context.Context, e ConnectionEvent) { events <- hookNotification{e, ctx} }
		client, streams := skipMemoryClient(t, nil, WithOnConnected(hook), WithOnDisconnected(hook))
		if err := client.Connect(t.Context()); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		first := awaitHook(t, t.Context(), events)
		if first.event.ConnectionID != "" || first.event.Address != "" || first.event.Err != nil {
			t.Fatalf("skip connected=%+v", first.event)
		}
		if err := client.Ready(t.Context()); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if streams.Load() != 0 {
			t.Fatal("skip opened a session")
		}
		select {
		case extra := <-events:
			t.Fatalf("skip reconnected: %+v", extra.event)
		default:
		}
		if err := client.Shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
		last := awaitHook(t, t.Context(), events)
		if !errors.Is(last.event.Err, ErrClosed) || last.event.Generation != first.event.Generation || last.event.ConnectionID != "" {
			t.Fatalf("skip disconnected=%+v", last.event)
		}
	})
}

func TestConnectionHooksFailedStartupFiresNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		events := make(chan hookNotification, 2)
		hook := func(ctx context.Context, e ConnectionEvent) { events <- hookNotification{e, ctx} }
		client, _ := skipMemoryClient(t, func(context.Context, string) error { return &CompatibilityError{} }, WithOnConnected(hook), WithOnDisconnected(hook))
		var incompatible *CompatibilityError
		if err := client.Connect(t.Context()); !errors.As(err, &incompatible) {
			t.Fatal(err)
		}
		if err := client.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case e := <-events:
			t.Fatalf("startup hook=%+v", e.event)
		default:
		}
	})
}
