//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"os"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpcmetadata "google.golang.org/grpc/metadata"
)

type CustomerRegistered struct {
	Name    string `json:"name"`
	Active  bool   `json:"active"`
	Balance uint64 `json:"balance"`
}

func TestKernelRegisterAppendRead(t *testing.T) {
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		t.Fatal("set CHRONICLE_INTEGRATION_CONNECTION_STRING to a development kernel; integration tests never silently skip")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[CustomerRegistered](registry, events.WithID("go-customer-registered")); err != nil {
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
	storeName := chronicle.StoreName("go-smoke-" + uuid.NewString())
	store, err := client.EventStore(ctx, storeName, chronicle.WithNamespace("smoke"))
	if err != nil {
		t.Fatal(err)
	}
	id, err := metadata.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	ctx = metadata.WithCorrelation(ctx, id)
	source := events.SourceID(uuid.NewString())
	scope := eventsequences.Scope{Expectation: eventsequences.NoMatchingEvent(), Filter: eventsequences.ScopeFilter{SourceID: &source}}
	result, err := store.EventLog().Append(ctx, source, CustomerRegistered{Name: "Ada", Active: false, Balance: 0}, eventsequences.WithScope(scope))
	if err != nil || result.Err() != nil {
		t.Fatalf("append: %+v %v", result, err)
	}
	if result.Position == nil || *result.Position != 0 || result.CorrelationID != id || !result.ConcurrencyCheckPerformed {
		t.Fatalf("wrong append result: %+v", result)
	}
	conflict, err := store.EventLog().Append(ctx, source, CustomerRegistered{Name: "Second"}, eventsequences.WithScope(scope))
	if err != nil || conflict.Disposition != eventsequences.Rejected || len(conflict.ConcurrencyViolations) != 1 {
		t.Fatalf("protected empty scope did not reject: %+v %v", conflict, err)
	}
	// Reads are slice 2. Use the public contracts to verify actual persisted content now.
	uri, err := chronicle.ParseConnectionString(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	policy := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} // Test-owned development kernel only.
	tokens := connection.NewOAuth(uri.Addresses()[0].String(), "chronicle-dev-client", "chronicle-dev-secret", policy)
	defer tokens.Close()
	token, err := tokens.Token(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(uri.Addresses()[0].String(), grpc.WithTransportCredentials(credentials.NewTLS(policy)), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}()
	readCtx := grpcmetadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token.AccessToken)
	response, err := sequences.NewEventSequencesClient(conn).ForEventSourceIdAndEventTypes(readCtx, &sequences.ForEventSourceIdAndEventTypesRequest{EventStore: string(storeName), Namespace: "smoke", EventSequenceId: "event-log", EventSourceId: string(source)})
	if err != nil {
		t.Fatal(err)
	}
	if err = wire.CheckEnvelope(response); err != nil {
		t.Fatal(err)
	}
	if len(response.Data) != 1 {
		t.Fatalf("persisted events = %d; rejected append must not commit", len(response.Data))
	}
	event := response.Data[0]
	var content CustomerRegistered
	if err = json.Unmarshal([]byte(event.Content), &content); err != nil {
		t.Fatal(err)
	}
	if content.Name != "Ada" || content.Active || content.Balance != 0 || event.Context.EventType.Id != "go-customer-registered" || event.Context.EventType.Generation != 1 || wire.Correlation(event.Context.CorrelationId) != id || event.Context.Subject != string(source) {
		t.Fatalf("persisted event = %v", event)
	}
	t.Logf("registered, appended position %d, rejected competing empty scope and read back from %s", *result.Position, storeName)
}
