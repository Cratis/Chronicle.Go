// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/serialization"
)

type classifiedUnusedVariant struct {
	Secret string `chronicle:"pii"`
}
type derivedAdmissionEvent struct{ Value any }

func TestDerivedAdmissionAndAppendRejectionDispatchNoRPC(t *testing.T) {
	registry := NewRegistry()
	bad, err := serialization.NewCodecs(serialization.Derived[any, classifiedUnusedVariant]("secret"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterEvent[derivedAdmissionEvent](registry, events.WithCodecs(bad)); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("protected admission: %v", err)
	}
	// A failed declaration leaves no artifact to register later.
	codecs, err := derivedfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterEvent[derivedfixtures.MembersChanged](registry, events.WithCodecs(codecs)); err != nil {
		t.Fatal(err)
	}
	var appends atomic.Int32
	kernel := &supervisedKernel{appendCall: func(context.Context) error { appends.Add(1); return nil }}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
	artifacts, err := client.Artifacts("store")
	if err != nil {
		t.Fatal(err)
	}
	if len(artifacts.Events.Descriptors()) != 1 {
		t.Fatal("failed protected declaration was partially admitted")
	}
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []derivedfixtures.MembersChanged{
		{Primary: (*derivedfixtures.HumanValue)(nil)},
		{Primary: derivedfixtures.RobotValue{Count: 1<<53 + 1}},
	} {
		if _, err := store.EventLog().Append(ctx, "source", value); !errors.Is(err, ErrUnsupported) {
			t.Fatalf("append admitted invalid family value: %v", err)
		}
	}
	if appends.Load() != 0 {
		t.Fatal("invalid family value reached append RPC")
	}
}
