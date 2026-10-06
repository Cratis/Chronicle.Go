//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/captures"
	contracts "github.com/cratis/chronicle.go/contracts/captures"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
)

func TestKernelCaptureAuthorizationSubmissionBoundary(t *testing.T) {
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
	builder := new(captures.Builder).From(captures.Webhook("/synthetic-capture")).Key("id").
		Append(captures.Append(event, captures.Added(), map[string]string{"Value": "$.value"}))
	unprotected, err := builder.Build("SyntheticUnprotectedCapture")
	if err != nil {
		t.Fatal(err)
	}
	client := contracts.NewCapturesClient(f.conn)
	var capability *captures.ValidationError
	if err := store.Captures().Save(f.ctx, unprotected); !errors.As(err, &capability) {
		t.Fatal("unprotected webhook did not return kernel capability diagnostics")
	}
	result, err := client.GetCaptures(f.ctx, &contracts.GetCapturesRequest{EventStore: string(f.storeName)})
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.CheckEnvelope(result); err != nil {
		t.Fatal(err)
	}
	if len(result.Data) != 1 || result.Data[0].Id != unprotected.ID().String() ||
		result.Data[0].Status != contracts.CaptureStatus_Stopped || result.Data[0].Declaration != unprotected.Declaration() {
		t.Fatal("unprotected webhook was not persisted as stopped")
	}
	protected, err := builder.From(captures.Webhook("/synthetic-capture", captures.WithBearerToken("synthetic-token"))).
		Build("SyntheticProtectedCapture")
	if err != nil {
		t.Fatal(err)
	}
	for _, submit := range []func() error{
		func() error { return store.Captures().Validate(f.ctx, protected) },
		func() error { return store.Captures().Save(f.ctx, protected) },
	} {
		if err := submit(); !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatal("protected webhook submission was not refused locally")
		}
	}
	result, err = client.GetCaptures(f.ctx, &contracts.GetCapturesRequest{EventStore: string(f.storeName)})
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.CheckEnvelope(result); err != nil {
		t.Fatal(err)
	}
	if len(result.Data) != 1 || result.Data[0].Id != unprotected.ID().String() {
		t.Fatal("refused authorized definition mutated remote captures")
	}
	for _, saved := range result.Data {
		if saved.Id == protected.ID().String() {
			t.Fatal("refused authorized definition was persisted")
		}
	}
	removed, err := client.DeleteCapture(f.ctx, &contracts.DeleteCaptureRequest{
		EventStore: string(f.storeName), CaptureId: wire.Guid(metadata.CorrelationID(unprotected.ID())),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := wire.CheckEnvelope(removed); err != nil {
		t.Fatal(err)
	}
}
