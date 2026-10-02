//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"os"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/google/uuid"
)

type BatchOpened struct {
	Value string `json:"value"`
}
type BatchChanged struct {
	Value string `json:"value"`
}

func batchFixture(t *testing.T) (context.Context, *eventsequences.Sequence) {
	t.Helper()
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		t.Fatal("set CHRONICLE_INTEGRATION_CONNECTION_STRING; integration never silently skips")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	t.Cleanup(cancel)
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[BatchOpened](registry, events.WithID("batch-opened"), events.WithTags("opened", "shared")); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[BatchChanged](registry, events.WithID("batch-changed"), events.WithTags("changed", "shared")); err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.Dial(ctx, chronicle.WithConnectionString(endpoint), chronicle.WithDevelopmentDefaults(), chronicle.WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := client.EventStore(ctx, chronicle.StoreName("go-batches-"+uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}
	return ctx, store.EventLog()
}

func sourceScope(source events.SourceID, expectation eventsequences.Expectation) eventsequences.LabeledScope {
	filter := eventsequences.ScopeFilter{}
	if expectation != eventsequences.NoCheck() {
		filter.SourceID = &source
	}
	return eventsequences.LabeledScope{Label: string(source), Scope: eventsequences.Scope{Expectation: expectation, Filter: filter}}
}
