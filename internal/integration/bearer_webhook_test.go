//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"strings"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	contracts "github.com/cratis/chronicle.go/contracts/observation/webhooks"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/webhooks"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Standalone fake values, never transport credentials. Even these are redacted
// from diagnostics; no request, authorization payload or metadata is printed.
const (
	bearerWebhookToken  = "synthetic-bearer-registration-fixture"
	bearerWebhookPass   = "synthetic-basic-registration-fixture"
	bearerWebhookSecret = "synthetic-oauth-registration-fixture"
)

// TestKernelWebhookBearerRegistrationCSharpSourceEquivalent compares the public
// SDK with independently assembled generated contracts on the same public RPC.
// Source authority: Chronicle 2e31b0d, WebhookDefinitionBuilder.WithBearerToken,
// WebhookDefinitionConverter.ToContract and Webhooks.Register. This is NOT an
// executed .NET serializer capture. No delivery or credential-use claim is made.
func TestKernelWebhookBearerRegistrationCSharpSourceEquivalent(t *testing.T) {
	f := newKernelFixture(t)
	uri, err := chronicle.ParseConnectionString(f.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	codec := &bearerRegistrationCodec{requests: make(chan []byte, 1)}
	responses := make(chan *contracts.CommandResult, 1)
	conn, err := grpc.NewClient(uri.Addresses()[0].String(),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true})), // Owned development kernel only.
		grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(grpc.ForceCodec(codec)),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, request, response any, cc *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
			err := invoke(ctx, method, request, response, cc, options...)
			if method == contracts.Webhooks_AddWebhooks_FullMethodName && err == nil {
				// Capture the raw command envelope before RegisterDefinition checks it.
				responses <- proto.Clone(response.(*contracts.CommandResult)).(*contracts.CommandResult)
			}
			return err
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := f.client(integrationRegistry[IntegrationPublished](t, events.WithID("IntegrationPublished")), chronicle.WithGRPCConnection(conn)).EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	client := contracts.NewWebhooksClient(conn)
	// The bounded hypothesis is order-dependent internal schema registration:
	// bearer first in a fresh store, then new bearer IDs after Basic/OAuth. Each
	// request has a unique ID, so no case can accidentally take an update/no-op path.
	profiles := []struct {
		name string
		kind contracts.AuthorizationType
	}{
		{"fresh-bearer", contracts.AuthorizationType_Bearer},
		{"basic-control", contracts.AuthorizationType_Basic},
		{"oauth-control", contracts.AuthorizationType_OAuth},
		{"after-controls-bearer", contracts.AuthorizationType_Bearer},
	}
	for _, profile := range profiles {
		t.Run(profile.name, func(t *testing.T) {
			option, authorization := bearerRegistrationAuthorization(profile.kind)
			sdkID, rawID := profile.name+"-sdk", profile.name+"-raw"
			definition, err := webhooks.Define(catalog, webhooks.ID(sdkID), "https://target.invalid/events", option,
				webhooks.WithEventTypes(events.TypeRef{ID: "IntegrationPublished", Generation: 1}), webhooks.WithActive(false), webhooks.WithReplayable(false))
			if err != nil {
				t.Fatal(err)
			}
			control := &contracts.AddWebhooksRequest{EventStore: string(f.storeName), Webhooks: []*contracts.WebhookDefinition{{
				Identifier: rawID, EventSequenceId: "event-log",
				EventTypes: []*contracts.EventType{{Id: "IntegrationPublished", Generation: 1}},
				Target:     &contracts.WebhookTarget{Url: "https://target.invalid/events", Authorization: authorization},
			}}}
			// C# NotActive/NotReplayable serialize explicit zeros because the
			// protobuf-net contract declares DefaultValue(true). Not #4394's waiver.
			control.Webhooks[0].ProtoReflect().SetUnknown([]byte{0x28, 0x00, 0x30, 0x00})
			sdkErr := store.Webhooks().RegisterDefinition(f.ctx, definition)
			sdkResponse := bearerRegistrationResponse(t, responses, sdkErr)
			sdkBytes := bearerRegistrationBytes(t, codec.requests)
			response, rawErr := client.AddWebhooks(f.ctx, control)
			rawResponse := bearerRegistrationResponse(t, responses, rawErr)
			rawBytes := bearerRegistrationBytes(t, codec.requests)
			if !proto.Equal(response, rawResponse) {
				t.Fatal("raw envelope changed after interception")
			}
			expectedSDK := proto.Clone(control).(*contracts.AddWebhooksRequest)
			expectedSDK.Webhooks[0].Identifier = sdkID
			assertBearerRegistrationBody(t, sdkBytes, expectedSDK)
			assertBearerRegistrationBody(t, rawBytes, control)
			t.Log("request bodies match C# source-equivalent contracts including authorization arm and explicit false fields; only webhook IDs differ")
			logBearerRegistrationResult(t, "sdk", sdkResponse, sdkErr)
			logBearerRegistrationResult(t, "raw-source-equivalent", rawResponse, rawErr)
			if sdkErr != nil || rawErr != nil || wire.CheckEnvelope(rawResponse) != nil {
				t.Fatal("bearer/control registration was not accepted; inspect sanitized envelopes (no unrelated inactive-webhook skip)")
			}
			eventID := map[contracts.AuthorizationType]string{
				contracts.AuthorizationType_Bearer: "BearerTokenAuthorizationSetForWebhook",
				contracts.AuthorizationType_Basic:  "BasicAuthorizationSetForWebhook",
				contracts.AuthorizationType_OAuth:  "OAuthAuthorizationSetForWebhook",
			}[profile.kind]
			missingSchema := !bearerRegistrationSchemaExists(t, f.ctx, conn, string(f.storeName), eventID)
			if missingSchema && !bearerRegistrationSchemaExists(t, f.ctx, conn, "System", eventID) {
				t.Fatal("authorization schema absent from System too; not the known non-System registration defect")
			}
			awaitBearerRegistrationNames(t, f.ctx, client, string(f.storeName), profile.kind, missingSchema, sdkID, rawID)
			for _, id := range []string{sdkID, rawID} {
				assertBearerRegistrationHistory(t, f.ctx, conn, string(f.storeName), id, eventID, missingSchema)
			}
			if missingSchema {
				t.Skipf("https://github.com/Cratis/Chronicle/issues/4567: %s schema exists only in System; both accepted requests persisted only WebhookAdded and read back AuthorizationType=None", eventID)
			}
		})
	}
}

func bearerRegistrationAuthorization(kind contracts.AuthorizationType) (webhooks.Option, *contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization) {
	switch kind {
	case contracts.AuthorizationType_Basic:
		return webhooks.WithBasicAuth("fixture-user", bearerWebhookPass), &contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value0: &contracts.BasicAuthorization{Username: "fixture-user", Password: bearerWebhookPass}}
	case contracts.AuthorizationType_OAuth:
		return webhooks.WithOAuth("https://issuer.invalid", "fixture-client", bearerWebhookSecret), &contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value2: &contracts.OAuthAuthorization{Authority: "https://issuer.invalid", ClientId: "fixture-client", ClientSecret: bearerWebhookSecret}}
	default:
		return webhooks.WithBearerToken(bearerWebhookToken), &contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value1: &contracts.BearerTokenAuthorization{Token: bearerWebhookToken}}
	}
}

func bearerRegistrationResponse(t *testing.T, responses <-chan *contracts.CommandResult, err error) *contracts.CommandResult {
	t.Helper()
	select {
	case response := <-responses:
		return response
	default:
		if err == nil {
			t.Fatal("missing command envelope")
		}
		return nil
	}
}

func bearerRegistrationBytes(t *testing.T, requests <-chan []byte) []byte {
	t.Helper()
	select {
	case body := <-requests:
		return body
	default:
		t.Fatal("missing transmitted AddWebhooks body")
		return nil
	}
}

func assertBearerRegistrationBody(t *testing.T, body []byte, expected *contracts.AddWebhooksRequest) {
	t.Helper()
	encoded, err := proto.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body, encoded) {
		t.Fatal("actual registration bytes differ from source-equivalent request (authorization redacted)")
	}
	actual, control := &contracts.AddWebhooksRequest{}, &contracts.AddWebhooksRequest{}
	if err := proto.Unmarshal(body, actual); err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(encoded, control); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(actual, control) {
		t.Fatal("decoded registration request differs (authorization redacted)")
	}
}

func logBearerRegistrationResult(t *testing.T, route string, response *contracts.CommandResult, err error) {
	t.Helper()
	redact := strings.NewReplacer(bearerWebhookToken, "[REDACTED]", bearerWebhookPass, "[REDACTED]", bearerWebhookSecret, "[REDACTED]")
	if response == nil {
		t.Logf("%s: no envelope; grpc=%s", route, status.Code(err))
		return
	}
	t.Logf("%s: grpc=OK authorized=%v validation=%d exceptions=%d sdkError=%v", route, response.IsAuthorized, len(response.ValidationResults), len(response.ExceptionMessages), err != nil)
	for _, message := range response.ExceptionMessages {
		t.Logf("%s exception: %s", route, redact.Replace(message))
	}
	if response.ExceptionStackTrace != "" {
		t.Logf("%s stack: %s", route, redact.Replace(response.ExceptionStackTrace))
	}
}

func awaitBearerRegistrationNames(t *testing.T, ctx context.Context, client contracts.WebhooksClient, store string, kind contracts.AuthorizationType, missingSchema bool, names ...string) {
	t.Helper()
	deadline, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		response, err := client.GetWebhooks(deadline, &contracts.GetWebhooksRequest{EventStore: store})
		if err != nil || wire.CheckEnvelope(response) != nil {
			t.Fatal("named webhook readback failed")
		}
		found := 0
		for _, name := range names {
			for _, definition := range response.Data {
				if definition.Identifier != name {
					continue
				}
				// WebhookAdded and authorization materialize separately. With a
				// schema present, None is an intermediate state, never a pass.
				if !missingSchema && definition.AuthorizationType == contracts.AuthorizationType_None {
					continue
				}
				wantKind := kind
				if missingSchema {
					wantKind = contracts.AuthorizationType_None
				}
				if definition.AuthorizationType != wantKind || definition.EventSequenceId != "event-log" || len(definition.EventTypes) != 1 || definition.EventTypes[0].Id != "IntegrationPublished" || definition.EventTypes[0].Generation != 1 || definition.IsReplayable {
					t.Fatalf("named webhook definition differs: name=%s kind=%s sequence=%s events=%d replayable=%v (authorization payload not exposed)", name, definition.AuthorizationType, definition.EventSequenceId, len(definition.EventTypes), definition.IsReplayable)
				}
				found++
			}
		}
		if found == len(names) {
			return
		}
		select {
		case <-deadline.Done():
			t.Fatal("expected named webhook definitions did not materialize")
		case <-ticker.C:
		}
	}
}

// An empty successful schema query is the only recognized missing-schema state.
// Transport/envelope failures never authorize a skip. The System positive control
// distinguishes the proven registration-scope defect from a missing kernel type.
func bearerRegistrationSchemaExists(t *testing.T, ctx context.Context, conn *grpc.ClientConn, store, eventID string) bool {
	t.Helper()
	response, err := eventtypes.NewEventTypesClient(conn).AllEventTypeGenerations(ctx, &eventtypes.AllEventTypeGenerationsRequest{EventStore: store, EventTypeId: eventID})
	if err != nil || wire.CheckEnvelope(response) != nil {
		t.Fatal("authorization schema query failed; not the known absence signature")
	}
	if len(response.Data) == 0 {
		return false
	}
	for _, generation := range response.Data {
		if generation.GetType().GetId() == eventID && generation.GetType().GetGeneration() == 1 && generation.Schema != "" {
			return true
		}
	}
	t.Fatal("authorization event schema has an unexpected generation or empty body")
	return false
}

func assertBearerRegistrationHistory(t *testing.T, ctx context.Context, conn *grpc.ClientConn, store, id, eventID string, missingSchema bool) {
	t.Helper()
	response, err := sequences.NewEventSequencesClient(conn).ForEventSourceIdAndEventTypes(ctx, &sequences.ForEventSourceIdAndEventTypesRequest{
		EventStore: store, Namespace: string(chronicle.DefaultNamespace), EventSequenceId: string(events.SystemSequence), EventSourceId: id,
	})
	if err != nil || wire.CheckEnvelope(response) != nil {
		t.Fatal("webhook system history query failed")
	}
	want := []string{"WebhookAdded", eventID}
	if missingSchema {
		want = want[:1]
	}
	if len(response.Data) != len(want) {
		t.Fatal("unexpected webhook system history length; not the documented missing-authorization signature")
	}
	for i, event := range response.Data {
		if event.GetContext().GetEventType().GetId() != want[i] || event.GetContext().GetEventType().GetGeneration() != 1 || event.GetContext().GetEventSourceId() != id {
			t.Fatal("unexpected webhook system event identity or generation")
		}
	}
	t.Logf("named history verified: events=%d authorizationEventPresent=%v", len(want), !missingSchema)
}

// Marshal is the actual client codec boundary, not a second serialization of a
// public snapshot. Each body stays in memory and is consumed after its RPC.
type bearerRegistrationCodec struct{ requests chan []byte }

func (*bearerRegistrationCodec) Name() string { return "proto" }
func (c *bearerRegistrationCodec) Marshal(value any) ([]byte, error) {
	body, err := proto.Marshal(value.(proto.Message))
	if _, ok := value.(*contracts.AddWebhooksRequest); ok && err == nil {
		c.requests <- bytes.Clone(body)
	}
	return body, err
}
func (*bearerRegistrationCodec) Unmarshal(body []byte, value any) error {
	return proto.Unmarshal(body, value.(proto.Message))
}
