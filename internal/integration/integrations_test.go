//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	webhookcontracts "github.com/cratis/chronicle.go/contracts/observation/webhooks"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/webhooks"
)

type IntegrationPublished struct{ Value string }

func TestKernelExternalSubscriptionForwardsOutboxToObserverInbox(t *testing.T) {
	f := newKernelFixture(t)
	sourceName := chronicle.StoreName("source-" + strings.TrimPrefix(string(f.storeName), "go-regression-"))
	sourceRegistry := integrationRegistry[IntegrationPublished](t, events.WithID("IntegrationPublished"))
	targetRegistry := integrationRegistry[IntegrationPublished](t, events.WithID("IntegrationPublished"), events.WithSourceStore(string(sourceName)))
	delivered := make(chan IntegrationPublished, 8)
	if err := chronicle.RegisterReactorHandler(targetRegistry, "external-inbox", func(ctx context.Context, event IntegrationPublished) error {
		select {
		case delivered <- event:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}); err != nil {
		t.Fatal(err)
	}
	client := f.client(targetRegistry, chronicle.WithRegistryForStore(sourceName, sourceRegistry))
	source, err := client.EventStore(f.ctx, sourceName)
	if err != nil {
		t.Fatal(err)
	}
	target, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	definitions, err := target.Subscriptions().GetAll(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(definitions) != 1 || definitions[0].SourceStore() != sourceName || len(definitions[0].EventTypes()) != 1 || definitions[0].EventTypes()[0] != "IntegrationPublished" {
		t.Fatalf("subscription readback: %v", definitions)
	}
	outbox, err := source.EventSequence(events.Outbox)
	if err != nil {
		t.Fatal(err)
	}
	result, err := outbox.Append(f.ctx, "item", IntegrationPublished{Value: "forwarded"})
	if err != nil || result.Err() != nil {
		t.Fatalf("outbox append: %v %v", result.Err(), err)
	}
	select {
	case got := <-delivered:
		if got.Value != "forwarded" {
			t.Fatal("incorrect forwarded content")
		}
	case <-time.After(20 * time.Second):
		inbox, openErr := target.EventSequence(events.SequenceID(events.InboxPrefix + string(sourceName)))
		if openErr != nil {
			t.Fatal(openErr)
		}
		persisted, readErr := inbox.ReadSource(f.ctx, "item", eventsequences.SourceFilter{})
		t.Fatalf("no observer delivery; inbox events=%d read=%v", len(persisted), readErr)
	case <-f.ctx.Done():
		t.Fatal(f.ctx.Err())
	}
	if err = target.Subscriptions().Unsubscribe(f.ctx, definitions[0].Identifier()); err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(f.ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		remaining, err := target.Subscriptions().GetAll(deadline)
		if err != nil {
			t.Fatal(err)
		}
		if len(remaining) == 0 {
			break
		}
		select {
		case <-deadline.Done():
			t.Fatal("subscription removal did not materialize")
		case <-ticker.C:
		}
	}
}

// TestKernelWebhookInactiveCSharpWireReadback compares the SDK with the exact
// explicit-zero field occurrences protobuf-net writes for DefaultValue(true).
func TestKernelWebhookInactiveCSharpWireReadback(t *testing.T) {
	f := newKernelFixture(t)
	store, err := f.client(integrationRegistry[IntegrationPublished](t, events.WithID("IntegrationPublished"))).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Webhooks().Register(f.ctx, "go", "https://example.invalid/events", webhooks.WithActive(false), webhooks.WithReplayable(false)); err != nil {
		t.Fatal(err)
	}
	control := &webhookcontracts.WebhookDefinition{Identifier: "csharp", EventSequenceId: "event-log", EventTypes: []*webhookcontracts.EventType{{Id: "IntegrationPublished", Generation: 1}}, Target: &webhookcontracts.WebhookTarget{Url: "https://example.invalid/events"}}
	control.ProtoReflect().SetUnknown([]byte{0x28, 0x00, 0x30, 0x00}) // Fields 5 and 6 explicitly false, like C#.
	response, err := webhookcontracts.NewWebhooksClient(f.conn).AddWebhooks(f.ctx, &webhookcontracts.AddWebhooksRequest{EventStore: string(f.storeName), Webhooks: []*webhookcontracts.WebhookDefinition{control}})
	if err != nil {
		t.Fatal(err)
	}
	if err = wire.CheckEnvelope(response); err != nil {
		t.Fatal(err)
	}
	definitions := awaitWebhookCount(t, f.ctx, store.Webhooks(), 2)
	for _, definition := range definitions {
		if definition.IsReplayable() {
			t.Fatal("explicit false replay flag was lost")
		}
	}
	for _, id := range []webhooks.ID{"go", "csharp"} {
		if err = store.Webhooks().Remove(f.ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	awaitWebhookCount(t, f.ctx, store.Webhooks(), 0)
	if definitions[0].IsActive() && definitions[1].IsActive() {
		t.Skip("https://github.com/Cratis/Chronicle/issues/4394: inactive webhook readback is true for Go and C#-equivalent wire; MongoDB WebhookDefinitionConverters.ToKernel omits IsActive")
	}
	for _, definition := range definitions {
		if definition.IsActive() {
			t.Fatal("Go and C#-equivalent wire disagree on activity")
		}
	}
}

func awaitWebhookCount(t *testing.T, ctx context.Context, service *webhooks.Service, count int) []webhooks.Definition {
	t.Helper()
	deadline, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		definitions, err := service.GetAll(deadline)
		if err != nil {
			t.Fatal(err)
		}
		if len(definitions) == count {
			return definitions
		}
		select {
		case <-deadline.Done():
			t.Fatalf("webhook materialization count=%d want=%d", len(definitions), count)
		case <-ticker.C:
		}
	}
}

func TestKernelWebhookDefinitionRegisterListRemove(t *testing.T) {
	f := newKernelFixture(t)
	client := f.client(integrationRegistry[IntegrationPublished](t, events.WithID("IntegrationPublished")))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Webhooks().Register(f.ctx, "notify", "https://example.invalid/events", webhooks.WithHeader("x-integration", "go")); err != nil {
		t.Fatal(err)
	}
	definitions := awaitWebhookCount(t, f.ctx, store.Webhooks(), 1)
	if err != nil || len(definitions) != 1 {
		t.Fatalf("webhook list count=%d err=%v", len(definitions), err)
	}
	definition := definitions[0].KernelDefinition()
	if definition.Identifier != "notify" || definition.EventSequenceId != "event-log" || !definition.IsActive || !definition.IsReplayable || definition.Target.Url != "https://example.invalid/events" || definition.Target.Headers["x-integration"] != "go" || definition.Target.Authorization != nil || len(definition.EventTypes) != 1 || definition.EventTypes[0].Id != "IntegrationPublished" {
		t.Fatalf("webhook readback differs: id=%s sequence=%s active=%v replayable=%v authPresent=%v events=%d headerPresent=%v", definition.Identifier, definition.EventSequenceId, definition.IsActive, definition.IsReplayable, definition.Target.Authorization != nil, len(definition.EventTypes), definition.Target.Headers["x-integration"] == "go")
	}
	if err = store.Webhooks().Remove(f.ctx, "notify"); err != nil {
		t.Fatal(err)
	}
	definitions = awaitWebhookCount(t, f.ctx, store.Webhooks(), 0)
	if err != nil || len(definitions) != 0 {
		t.Fatalf("webhook removal count=%d err=%v", len(definitions), err)
	}
}
