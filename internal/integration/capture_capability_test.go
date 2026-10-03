//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"errors"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/captures"
	contracts "github.com/cratis/chronicle.go/contracts/captures"
	servicecontracts "github.com/cratis/chronicle.go/contracts/externalservices"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/externalservices"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
)

func TestKernelCaptureCapability(t *testing.T) {
	f := newKernelFixture(t)
	registry := chronicle.NewRegistry()
	event, err := chronicle.RegisterEvent[IntegrationPublished](registry, events.WithID("IntegrationPublished"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := f.client(registry).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.ExternalServices().Register(f.ctx, "Inventory", externalservices.HTTP("https://example.invalid/items")); err != nil {
		t.Fatal(err)
	}
	services, err := servicecontracts.NewExternalServicesClient(f.conn).GetExternalServices(f.ctx, &servicecontracts.GetExternalServicesRequest{EventStore: string(f.storeName)})
	if err != nil {
		t.Fatal(err)
	}
	if err = wire.CheckEnvelope(services); err != nil {
		t.Fatal(err)
	}
	if len(services.Data) != 1 || services.Data[0].Name != "Inventory" {
		t.Fatal("external service was not persisted")
	}
	builder := new(captures.Builder).From(captures.API("Inventory", "/items", "1m")).Key("id").Append(captures.Append(event, captures.Added(), map[string]string{"Value": "$.value"}))
	definition, err := builder.Build("InventoryCapture")
	if err != nil {
		t.Fatal(err)
	}
	if err = store.Captures().Validate(f.ctx, definition); err != nil {
		var failure *captures.ValidationError
		if errors.As(err, &failure) {
			t.Fatalf("API capability diagnostics: %v", failure.Messages)
		}
		t.Fatal(err)
	}
	if err = store.Captures().Save(f.ctx, definition); err != nil {
		t.Fatal(err)
	}
	result, err := contracts.NewCapturesClient(f.conn).GetCaptures(f.ctx, &contracts.GetCapturesRequest{EventStore: string(f.storeName)})
	if err != nil {
		t.Fatal(err)
	}
	if err = wire.CheckEnvelope(result); err != nil {
		t.Fatal(err)
	}
	if len(result.Data) != 1 || result.Data[0].Id != definition.ID().String() || result.Data[0].Declaration != definition.Declaration() || result.Data[0].Status != contracts.CaptureStatus_Stopped {
		t.Fatal("saved capture differs or was activated")
	}
	for _, source := range []captures.Source{captures.Webhook("/items"), captures.MessageTopic("items")} {
		unsupported, err := builder.From(source).Build("UnsupportedCapture")
		if err != nil {
			t.Fatal(err)
		}
		err = store.Captures().Validate(f.ctx, unsupported)
		var failure *captures.ValidationError
		if !errors.As(err, &failure) || !strings.Contains(strings.Join(failure.Messages, " "), "not supported") {
			t.Fatalf("unsupported source did not fail explicitly: %v", err)
		}
	}
	removed, err := contracts.NewCapturesClient(f.conn).DeleteCapture(f.ctx, &contracts.DeleteCaptureRequest{EventStore: string(f.storeName), CaptureId: wire.Guid(metadata.CorrelationID(definition.ID()))})
	if err != nil {
		t.Fatal(err)
	}
	if err = wire.CheckEnvelope(removed); err != nil {
		t.Fatal(err)
	}
}
