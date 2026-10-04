//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"crypto/tls"
	"os"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/identities"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	grpcmetadata "google.golang.org/grpc/metadata"
)

type identityRenameRecorded struct{ Value string }

func TestKernelIdentityRenameObservedName(t *testing.T) {
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		t.Fatal("set CHRONICLE_INTEGRATION_CONNECTION_STRING; integration never silently skips")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	uri, err := chronicle.ParseConnectionString(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} // Own pinned development kernel only.
	tokens := connection.NewOAuth(uri.Addresses()[0].String(), "chronicle-dev-client", "chronicle-dev-secret", tlsConfig)
	defer tokens.Close()
	var active atomic.Bool
	var reads, commands atomic.Int32
	var commandAcknowledged atomic.Bool
	var correlationHeader atomic.Bool
	selected, err := metadata.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(uri.Addresses()[0].String(), grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), grpc.WithDisableRetry(), grpc.WithUnaryInterceptor(func(call context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, options ...grpc.CallOption) error {
		if active.Load() {
			switch method {
			case contracts.Identities_GetIdentities_FullMethodName:
				reads.Add(1)
			case contracts.Identities_RenameIdentity_FullMethodName:
				commands.Add(1)
			}
			md, _ := grpcmetadata.FromOutgoingContext(call)
			if md.Get("x-correlation-id")[0] != selected.String() {
				correlationHeader.Store(false)
			}
		}
		err := invoker(call, method, req, reply, cc, options...)
		if active.Load() && method == contracts.Identities_RenameIdentity_FullMethodName && err == nil {
			response := reply.(*contracts.CommandResult)
			commandAcknowledged.Store(response.IsAuthorized && len(response.ValidationResults) == 0 && len(response.ExceptionMessages) == 0 && response.ExceptionStackTrace == "")
		}
		return err
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	}()
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[identityRenameRecorded](registry, events.WithID("identity-rename-recorded")); err != nil {
		t.Fatal(err)
	}
	actor := identities.Identity{Subject: "rename-subject", Name: "old-name", UserName: "unchanged-username"}
	client, err := chronicle.Dial(ctx, chronicle.WithGRPCConnection(conn), chronicle.WithTokenSource(tokens), chronicle.WithRegistry(registry), chronicle.WithIdentityProvider(func(context.Context) (identities.Identity, bool, error) { return actor, true, nil }), chronicle.WithCorrelationProvider(func(context.Context) (metadata.CorrelationID, bool, error) { return selected, true, nil }))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	store, err := client.EventStore(ctx, chronicle.StoreName("go-id-"+uuid.NewString()[:8]), chronicle.WithNamespace(chronicle.Namespace("id-"+uuid.NewString()[:8])))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []identities.Identity{actor, {Subject: "unrelated-subject", Name: "unrelated-name", UserName: "unrelated-username"}} {
		actor = value
		result, err := store.EventLog().Append(ctx, events.SourceID(value.Subject), identityRenameRecorded{Value: "ordinary-registration"})
		if err != nil {
			t.Fatal(err)
		}
		if err := result.Err(); err != nil {
			t.Fatal(err)
		}
	}
	actor = identities.Identity{Subject: "rename-subject", Name: "old-name", UserName: "unchanged-username"}
	independent := func() map[string]*contracts.IdentityDetailsResponse {
		t.Helper()
		token, err := tokens.Token(ctx)
		if err != nil {
			t.Fatal(err)
		}
		call := grpcmetadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token.AccessToken)
		result, err := contracts.NewIdentitiesClient(conn).GetIdentities(call, &contracts.GetIdentitiesRequest{EventStore: string(store.Name()), Namespace: string(store.Namespace())})
		if err != nil {
			t.Fatal(err)
		}
		if !result.IsAuthorized || len(result.ValidationResults) != 0 || len(result.ExceptionMessages) != 0 || result.ExceptionStackTrace != "" {
			t.Fatal("independent listing failed")
		}
		rows := make(map[string]*contracts.IdentityDetailsResponse)
		for _, row := range result.Data {
			if row == nil || row.Subject != row.Id || rows[row.Subject] != nil {
				t.Fatal("invalid independent listing")
			}
			rows[row.Subject] = row
		}
		return rows
	}
	before := independent()
	if before[actor.Subject] == nil || before[actor.Subject].Name != "old-name" || before["unrelated-subject"] == nil {
		t.Fatal("ordinary append did not establish identities")
	}
	correlationHeader.Store(true)
	active.Store(true)
	result, err := store.Identities().Rename(ctx, actor.Subject, identities.Name("new-name"))
	active.Store(false)
	if err != nil {
		t.Fatalf("public rename failed: result=%+v error=%v reads=%d commands=%d actualAck=%t", result, err, reads.Load(), commands.Load(), commandAcknowledged.Load())
	}
	if result.Disposition != chronicle.IdentityRenameObserved || !result.Acknowledged || !commandAcknowledged.Load() || reads.Load() != 2 || commands.Load() != 1 || !correlationHeader.Load() {
		t.Fatalf("result=%+v reads=%d commands=%d actualAck=%t", result, reads.Load(), commands.Load(), commandAcknowledged.Load())
	}
	after := independent()
	if len(after) != len(before) {
		t.Fatal("identity rows changed")
	}
	for subject, old := range before {
		row := after[subject]
		if row == nil || row.Subject != old.Subject || row.Id != old.Id || row.UserName != old.UserName {
			t.Fatal("identity key/username changed")
		}
		if subject == actor.Subject {
			if row.Name != "new-name" {
				t.Fatal("Mongo-backed listing did not observe renamed name")
			}
		} else if row.Name != old.Name {
			t.Fatal("unrelated identity changed")
		}
	}
	t.Logf("pinned ordinary identity-name observation: reads=%d command=%d actualAck=%t; independent storage listing preserved subjects/usernames/unrelated rows", reads.Load(), commands.Load(), commandAcknowledged.Load())
}
