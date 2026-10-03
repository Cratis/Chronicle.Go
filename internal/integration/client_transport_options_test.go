//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"crypto/tls"
	"os"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts"
	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestKernelClientTransportOptions(t *testing.T) {
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		t.Fatal("set CHRONICLE_INTEGRATION_CONNECTION_STRING; never silently skip")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	uri, err := chronicle.ParseConnectionString(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	policy := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} // Owned development kernel only.
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
	service := clients.NewConnectionServiceClient(conn)
	authCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token.AccessToken)
	before, err := service.GetConnectedClients(authCtx, &emptypb.Empty{})
	if err != nil {
		t.Fatalf("protected readiness probe unsupported/rejected: decision needed: %v", err)
	}
	// Anonymous compatibility is not evidence of protected readiness. This call
	// without credentials must be rejected by this disposable kernel.
	if _, err := service.GetConnectedClients(ctx, &emptypb.Empty{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("probe not authenticated: %v", err)
	}
	request := &clients.CompatibilityRequest{ClientType: "Go", ClientVersion: "0.1.0-dev", ProtocolVersion: contracts.ProtocolVersion, DescriptorSet: contracts.DescriptorSet()}
	response, err := service.CheckCompatibility(authCtx, request, grpc.MaxCallRecvMsgSize(100*1024*1024))
	if err != nil || !response.GetIsCompatible() {
		t.Fatalf("compatibility: %v %v", response, err)
	}
	sendSize, receiveSize := proto.Size(request), proto.Size(response)
	if sendSize < 2 || receiveSize < 2 {
		t.Fatal("vacuous boundary witness")
	}
	base := []chronicle.ClientOption{chronicle.WithConnectionString(endpoint), chronicle.WithDevelopmentDefaults(), chronicle.WithTokenSource(tokens), chronicle.WithSkipKeepAlive()}
	for _, tc := range []struct {
		name   string
		option chronicle.ClientOption
		want   codes.Code
	}{
		{"send exact", chronicle.WithMaxSendMessageSize(sendSize), codes.OK},
		{"send one short", chronicle.WithMaxSendMessageSize(sendSize - 1), codes.ResourceExhausted},
		{"receive exact", chronicle.WithMaxReceiveMessageSize(receiveSize), codes.OK},
		{"receive one short", chronicle.WithMaxReceiveMessageSize(receiveSize - 1), codes.ResourceExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := chronicle.Dial(ctx, append(append([]chronicle.ClientOption(nil), base...), tc.option)...)
			if status.Code(err) != tc.want {
				t.Fatalf("code=%s want=%s error=%v", status.Code(err), tc.want, err)
			}
			if client != nil {
				if err := client.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[CustomerRegistered](registry, events.WithID("go-options-customer-registered")); err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.Dial(ctx, append(base, chronicle.WithRegistry(registry))...)
	if err != nil {
		t.Fatalf("skipped startup probe: decision needed: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	store, err := client.EventStore(ctx, chronicle.StoreName("go-options-"+uuid.NewString()))
	if err != nil {
		t.Fatal(err)
	}
	source := events.SourceID(uuid.NewString())
	result, err := store.EventLog().Append(ctx, source, CustomerRegistered{Name: "no logical session"}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	if err != nil || result.Err() != nil {
		t.Fatalf("append: %v %v", err, result.Err())
	}
	history, err := store.EventLog().ReadSource(ctx, source, eventsequences.SourceFilter{})
	if err != nil || len(history) != 1 {
		t.Fatalf("read: count=%d error=%v", len(history), err)
	}
	after, err := service.GetConnectedClients(authCtx, &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(before, after) {
		t.Fatalf("skipped clients registered a logical Connect session: before=%v after=%v", before, after)
	}
	t.Logf("19.29.4 protected probe rejects anonymous access; skip authenticated registration/append/read succeeds without Connect registration; exact protobuf send=%d receive=%d limits enforce equality/one-byte-short", sendSize, receiveSize)
}
