// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package webhooks_test

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/observation/webhooks"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/webhooks"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

// These are standalone synthetic fixtures, never credentials for a real service.
const wireBearerFixture = "synthetic-bearer-wire-fixture"

type bearerWireEvent struct{ Value string }

// TestWebhookBearerRegisterDefinitionWire exercises a single bearer-only submit,
// not a Basic -> Bearer -> OAuth option chain whose final request is OAuth.
func TestWebhookBearerRegisterDefinitionWire(t *testing.T) {
	assertAuthorizationRegistrationWire(t, webhooks.WithBearerToken(wireBearerFixture),
		&contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{
			Value1: &contracts.BearerTokenAuthorization{Token: wireBearerFixture},
		})
}

func TestWebhookRegistrationWirePositiveAuthorizationControls(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		assertAuthorizationRegistrationWire(t, webhooks.WithBasicAuth("fixture-user", "fixture-password"),
			&contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{
				Value0: &contracts.BasicAuthorization{Username: "fixture-user", Password: "fixture-password"},
			})
	})
	t.Run("oauth", func(t *testing.T) {
		assertAuthorizationRegistrationWire(t, webhooks.WithOAuth("https://issuer.invalid", "fixture-client", "fixture-secret"),
			&contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{
				Value2: &contracts.OAuthAuthorization{Authority: "https://issuer.invalid", ClientId: "fixture-client", ClientSecret: "fixture-secret"},
			})
	})
}

func assertAuthorizationRegistrationWire(t *testing.T, option webhooks.Option, authorization *contracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization) {
	t.Helper()
	event, err := events.Define[bearerWireEvent](events.WithID("BearerWireEvent"), events.WithGeneration(3))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	kernel := &bearerWireKernel{received: make(chan *contracts.AddWebhooksRequest, 1)}
	codec := &bearerWireCodec{received: make(chan []byte, 1)}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.ForceServerCodec(codec))
	contracts.RegisterWebhooksServer(server, kernel)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := server.Serve(listener); err != nil {
			t.Error(err)
		}
	}()
	t.Cleanup(func() { server.Stop(); <-done })
	conn, err := grpc.NewClient("passthrough:///bearer-wire", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return listener.DialContext(ctx)
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	service, err := webhooks.New("bearer-wire-store", catalog, conn)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := webhooks.Define(catalog, "notify", "https://target.invalid/events", option, webhooks.WithActive(false), webhooks.WithReplayable(false))
	if err != nil {
		t.Fatal(err)
	}
	if err := service.RegisterDefinition(t.Context(), definition); err != nil {
		t.Fatal(err)
	}
	if kernel.calls.Load() != 1 {
		t.Fatal("RegisterDefinition must send exactly one AddWebhooks request")
	}

	// Independently assembled from Chronicle 2e31b0d WebhookDefinitionBuilder,
	// WebhookDefinitionConverter and Webhooks.Register; not an executed .NET capture.
	want := &contracts.AddWebhooksRequest{EventStore: "bearer-wire-store", Webhooks: []*contracts.WebhookDefinition{{
		Identifier: "notify", EventSequenceId: "event-log",
		EventTypes: []*contracts.EventType{{Id: "BearerWireEvent", Generation: 3}},
		Target:     &contracts.WebhookTarget{Url: "https://target.invalid/events", Authorization: authorization},
	}}}
	// protobuf-net DefaultValue(true) requires explicit false fields 5 and 6.
	want.Webhooks[0].ProtoReflect().SetUnknown([]byte{0x28, 0x00, 0x30, 0x00})
	encoded, err := proto.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(<-codec.received, encoded) {
		t.Fatal("transmitted AddWebhooks body differs from source-equivalent control (authorization redacted)")
	}
	decoded := &contracts.AddWebhooksRequest{}
	if err := proto.Unmarshal(encoded, decoded); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(<-kernel.received, decoded) {
		t.Fatal("decoded AddWebhooks request differs (authorization redacted)")
	}
}

type bearerWireKernel struct {
	contracts.UnimplementedWebhooksServer
	calls    atomic.Int32
	received chan *contracts.AddWebhooksRequest
}

func (k *bearerWireKernel) AddWebhooks(_ context.Context, request *contracts.AddWebhooksRequest) (*contracts.CommandResult, error) {
	k.calls.Add(1)
	select {
	case k.received <- request:
	default:
	}
	return &contracts.CommandResult{IsAuthorized: true}, nil
}

// Capture the actual incoming protobuf body before decoding drops explicit zeros.
// Bytes are compared in memory only; diagnostic output never includes auth data.
type bearerWireCodec struct{ received chan []byte }

func (*bearerWireCodec) Name() string { return "proto" }
func (*bearerWireCodec) Marshal(value any) ([]byte, error) {
	return proto.Marshal(value.(proto.Message))
}
func (c *bearerWireCodec) Unmarshal(data []byte, value any) error {
	if _, ok := value.(*contracts.AddWebhooksRequest); ok {
		select {
		case c.received <- bytes.Clone(data):
		default:
		}
	}
	return proto.Unmarshal(data, value.(proto.Message))
}
