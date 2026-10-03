//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts"
	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// kernelReadPayloadSize observes the actual uncompressed protobuf bytes. The
// protobuf-net server emits some explicit defaults that proto.Size of a decoded
// Go message omits, so reserialization is not an exact receive-boundary witness.
type kernelReadPayloadSize struct{ size atomic.Int64 }
type kernelReadPayloadKey struct{}

func (*kernelReadPayloadSize) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, kernelReadPayloadKey{}, info.FullMethodName == sequences.EventSequences_ForEventSourceIdAndEventTypes_FullMethodName)
}
func (s *kernelReadPayloadSize) HandleRPC(ctx context.Context, event stats.RPCStats) {
	if ctx.Value(kernelReadPayloadKey{}) != true {
		return
	}
	if payload, ok := event.(*stats.InPayload); ok {
		s.size.Store(int64(payload.Length))
	}
}
func (*kernelReadPayloadSize) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (*kernelReadPayloadSize) HandleConn(context.Context, stats.ConnStats) {}

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
	readPayloadSize := &kernelReadPayloadSize{}
	conn, err := grpc.NewClient(uri.Addresses()[0].String(), grpc.WithTransportCredentials(credentials.NewTLS(policy)), grpc.WithDisableRetry(), grpc.WithStatsHandler(readPayloadSize))
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
	// The probe includes other fixtures' sessions. Bound its normal envelope,
	// separately from the much larger, immutable event-read boundary below.
	const startupReceiveBound = 64 * 1024
	const fixtureMessageBound = 512 * 1024
	before, err := service.GetConnectedClients(authCtx, &emptypb.Empty{}, grpc.MaxCallRecvMsgSize(startupReceiveBound))
	if err != nil {
		t.Fatalf("protected readiness probe unsupported/rejected: decision needed: %v", err)
	}
	// Anonymous compatibility is not evidence of protected readiness. This call
	// without credentials must be rejected by this disposable kernel.
	if _, err := service.GetConnectedClients(ctx, &emptypb.Empty{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("probe not authenticated: %v", err)
	}
	request := &clients.CompatibilityRequest{ClientType: "Go", ClientVersion: "0.1.0-dev", ProtocolVersion: contracts.ProtocolVersion, DescriptorSet: contracts.DescriptorSet()}
	response, err := service.CheckCompatibility(authCtx, request, grpc.MaxCallRecvMsgSize(startupReceiveBound))
	if err != nil || !response.GetIsCompatible() {
		t.Fatalf("compatibility: %v %v", response, err)
	}
	sendSize := proto.Size(request)
	if sendSize < 2 || proto.Size(response) < 2 {
		t.Fatal("vacuous boundary witness")
	}
	base := []chronicle.ClientOption{chronicle.WithConnectionString(endpoint), chronicle.WithDevelopmentDefaults(), chronicle.WithTokenSource(tokens), chronicle.WithSkipKeepAlive(), chronicle.WithMaxReceiveMessageSize(startupReceiveBound)}
	for _, tc := range []struct {
		name   string
		option chronicle.ClientOption
		want   codes.Code
	}{
		{"compatibility send exact", chronicle.WithMaxSendMessageSize(sendSize), codes.OK},
		{"compatibility send one short", chronicle.WithMaxSendMessageSize(sendSize - 1), codes.ResourceExhausted},
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
	registered := append(append([]chronicle.ClientOption(nil), base...), chronicle.WithRegistry(registry))
	client, err := chronicle.Dial(ctx, append(append([]chronicle.ClientOption(nil), registered...), chronicle.WithMaxSendMessageSize(fixtureMessageBound), chronicle.WithMaxReceiveMessageSize(fixtureMessageBound))...)
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
	// Large enough to dominate compatibility, registration and the bounded
	// readiness probe, without using an unlimited/default-size transport.
	payload := CustomerRegistered{Name: strings.Repeat("x", 128*1024)}
	result, err := store.EventLog().Append(ctx, source, payload, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	if err != nil || result.Err() != nil {
		t.Fatalf("append: %v %v", err, result.Err())
	}
	assertPayload := func(t *testing.T, history []events.Appended) {
		t.Helper()
		if len(history) != 1 {
			t.Fatalf("read: count=%d want=1", len(history))
		}
		var got CustomerRegistered
		if err := json.Unmarshal(history[0].Content, &got); err != nil || got != payload {
			t.Fatalf("read payload mismatch: error=%v name length=%d", err, len(got.Name))
		}
	}
	history, err := store.EventLog().ReadSource(ctx, source, eventsequences.SourceFilter{})
	if err != nil {
		t.Fatal(err)
	}
	assertPayload(t, history)
	// Measure the complete protobuf response, not just the JSON content. This
	// source is immutable; no fixture writes into this unique store/source.
	readResponse, err := sequences.NewEventSequencesClient(conn).ForEventSourceIdAndEventTypes(authCtx, &sequences.ForEventSourceIdAndEventTypesRequest{EventStore: string(store.Name()), Namespace: string(store.Namespace()), EventSequenceId: string(events.EventLog), EventSourceId: string(source)}, grpc.MaxCallRecvMsgSize(fixtureMessageBound))
	if err != nil || len(readResponse.GetData()) != 1 {
		t.Fatalf("target read witness: %v", err)
	}
	receiveSize := int(readPayloadSize.size.Load())
	if receiveSize < proto.Size(readResponse) || receiveSize <= startupReceiveBound || receiveSize >= fixtureMessageBound {
		t.Fatalf("target read size=%d must exceed startup bound=%d and fit fixture bound=%d", receiveSize, startupReceiveBound, fixtureMessageBound)
	}
	for _, tc := range []struct {
		name  string
		limit int
		want  codes.Code
	}{
		{"event read receive exact", receiveSize, codes.OK},
		{"event read receive one short", receiveSize - 1, codes.ResourceExhausted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bounded, err := chronicle.Dial(ctx, append(append([]chronicle.ClientOption(nil), registered...), chronicle.WithMaxReceiveMessageSize(tc.limit))...)
			if err != nil {
				t.Fatalf("startup must fit target limit=%d: %v", tc.limit, err)
			}
			defer func() {
				if err := bounded.Close(); err != nil {
					t.Error(err)
				}
			}()
			boundedStore, err := bounded.EventStore(ctx, store.Name())
			if err != nil {
				t.Fatalf("registration must fit target limit=%d: %v", tc.limit, err)
			}
			got, err := boundedStore.EventLog().ReadSource(ctx, source, eventsequences.SourceFilter{})
			if status.Code(err) != tc.want {
				t.Fatalf("target read code=%s want=%s limit=%d error=%v", status.Code(err), tc.want, tc.limit, err)
			}
			if err == nil {
				assertPayload(t, got)
			} else if got != nil {
				t.Fatal("oversize read returned partial history")
			}
		})
	}
	after, err := service.GetConnectedClients(authCtx, &emptypb.Empty{}, grpc.MaxCallRecvMsgSize(startupReceiveBound))
	if err != nil {
		t.Fatal(err)
	}
	// No public connection ID exists for a skipped session. These tests are
	// serial within this process; compare only newly visible own-process IDs,
	// not global counts, LastSeen timestamps or sessions from other binaries.
	ownBefore := make(map[string]bool)
	for _, connected := range before.GetItems() {
		if connected.GetProcessId() == int32(os.Getpid()) && connected.GetClientType() == "Go" {
			ownBefore[connected.GetConnectionId()] = true
		}
	}
	for _, connected := range after.GetItems() {
		if connected.GetProcessId() == int32(os.Getpid()) && connected.GetClientType() == "Go" && !ownBefore[connected.GetConnectionId()] {
			t.Fatalf("skipped client registered an own-process Connect session: %s", connected.GetConnectionId())
		}
	}
	t.Logf("19.29.4 rejects anonymous probe; authenticated skip registration/append/read adds no own-process session; startup probe decoded protobuf=%d bounded at %d; compatibility send=%d and immutable event read wire protobuf receive=%d (decoded=%d) enforce equality/one-byte-short", proto.Size(before), startupReceiveBound, sendSize, receiveSize, proto.Size(readResponse))
}
