//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"crypto/rand"
	"os"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

type ModelBoundEmailClaimed struct {
	Email string  `json:"email" chronicle:"unique(name=\"model-bound-email\",message=\"Address {PropertyValue} is already claimed\",sequences=[\"event-log\"])"`
	Owner *string `json:"owner" chronicle:"subject"`
}
type ModelBoundEmailReleased struct{}

func TestKernelModelBoundUniqueRemovalAndSubject(t *testing.T) {
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		t.Fatal("set CHRONICLE_INTEGRATION_CONNECTION_STRING; integration never silently skips")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[ModelBoundEmailClaimed](registry); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[ModelBoundEmailReleased](registry, events.WithRemoveConstraints("model-bound-email")); err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.Dial(ctx, chronicle.WithConnectionString(endpoint), chronicle.WithDevelopmentDefaults(), chronicle.WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	store, err := client.EventStore(ctx, chronicle.StoreName("go-eventdecl-"+rand.Text()))
	if err != nil {
		t.Fatal(err)
	}
	log := store.EventLog()
	owner := "tagged-owner"
	event := ModelBoundEmailClaimed{Email: "shared@example.test", Owner: &owner}
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, log, "first", event))
	rejected := appendConstraintEvent(t, ctx, log, "second", event)
	requireConstraintRejection(t, rejected, "model-bound-email")
	if rejected.ConstraintViolations[0].Message != "Address shared@example.test is already claimed" {
		t.Fatalf("declaration message lost: %+v", rejected)
	}
	requireSourceCount(t, ctx, log, "second", 0)
	history, err := log.ReadSource(ctx, "first", eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].Context.Subject != "tagged-owner" {
		t.Fatalf("tag subject not persisted: %+v", history)
	}
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, log, "first", ModelBoundEmailReleased{}))
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, log, "second", event, eventsequences.WithSubject("override")))
	history, err = log.ReadSource(ctx, "second", eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].Context.Subject != "override" {
		t.Fatalf("append override lost: %+v", history)
	}
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, log, "third", ModelBoundEmailClaimed{Email: "other@example.test"}))
	history, err = log.ReadSource(ctx, "third", eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].Context.Subject != "third" {
		t.Fatalf("nil subject fallback lost: %+v", history)
	}
}
