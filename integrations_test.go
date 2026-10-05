// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/captures"
	capturecontracts "github.com/cratis/chronicle.go/contracts/captures"
	servicecontracts "github.com/cratis/chronicle.go/contracts/externalservices"
	subscriptioncontracts "github.com/cratis/chronicle.go/contracts/observation/eventstoresubscriptions"
	webhookcontracts "github.com/cratis/chronicle.go/contracts/observation/webhooks"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventstoresubscriptions"
	"github.com/cratis/chronicle.go/externalservices"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/webhooks"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

type integrationDefinitionsKernel struct {
	subscriptioncontracts.UnimplementedEventStoreSubscriptionsServer
	webhookcontracts.UnimplementedWebhooksServer
	servicecontracts.UnimplementedExternalServicesServer
	capturecontracts.UnimplementedCapturesServer
	subscribe     func(context.Context, *subscriptioncontracts.AddEventStoreSubscriptions) error
	subscriptions func(*subscriptioncontracts.GetEventStoreSubscriptionsRequest) *subscriptioncontracts.IEnumerable_EventStoreSubscriptionDefinition
	unsubscribe   func(*subscriptioncontracts.RemoveEventStoreSubscriptions)
	webhook       func(*webhookcontracts.AddWebhooksRequest) *webhookcontracts.CommandResult
	webhooks      func(*webhookcontracts.GetWebhooksRequest) *webhookcontracts.QueryResult_IEnumerable_WebhookDetailsResponse
	removeWebhook func(*webhookcontracts.RemoveWebhooksRequest)
	service       func(context.Context, *servicecontracts.AddExternalServicesRequest) (*servicecontracts.CommandResult, error)
	capture       func(*capturecontracts.SaveCaptureRequest) *capturecontracts.CommandResult_SaveCaptureResponse
	validate      func(*capturecontracts.ValidateCaptureDeclarationRequest) *capturecontracts.CommandResult_ValidateCaptureDeclarationResponse
}

func (k *integrationDefinitionsKernel) Add(ctx context.Context, r *subscriptioncontracts.AddEventStoreSubscriptions) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, k.subscribe(ctx, r)
}
func (k *integrationDefinitionsKernel) GetSubscriptions(_ context.Context, r *subscriptioncontracts.GetEventStoreSubscriptionsRequest) (*subscriptioncontracts.IEnumerable_EventStoreSubscriptionDefinition, error) {
	return k.subscriptions(r), nil
}
func (k *integrationDefinitionsKernel) Remove(_ context.Context, r *subscriptioncontracts.RemoveEventStoreSubscriptions) (*emptypb.Empty, error) {
	k.unsubscribe(r)
	return &emptypb.Empty{}, nil
}
func (k *integrationDefinitionsKernel) AddWebhooks(_ context.Context, r *webhookcontracts.AddWebhooksRequest) (*webhookcontracts.CommandResult, error) {
	return k.webhook(r), nil
}
func (k *integrationDefinitionsKernel) GetWebhooks(_ context.Context, r *webhookcontracts.GetWebhooksRequest) (*webhookcontracts.QueryResult_IEnumerable_WebhookDetailsResponse, error) {
	return k.webhooks(r), nil
}
func (k *integrationDefinitionsKernel) RemoveWebhooks(_ context.Context, r *webhookcontracts.RemoveWebhooksRequest) (*webhookcontracts.CommandResult, error) {
	k.removeWebhook(r)
	return &webhookcontracts.CommandResult{IsAuthorized: true}, nil
}
func (k *integrationDefinitionsKernel) AddExternalServices(ctx context.Context, r *servicecontracts.AddExternalServicesRequest) (*servicecontracts.CommandResult, error) {
	return k.service(ctx, r)
}
func (k *integrationDefinitionsKernel) SaveCapture(_ context.Context, r *capturecontracts.SaveCaptureRequest) (*capturecontracts.CommandResult_SaveCaptureResponse, error) {
	return k.capture(r), nil
}
func (k *integrationDefinitionsKernel) ValidateCaptureDeclaration(_ context.Context, r *capturecontracts.ValidateCaptureDeclarationRequest) (*capturecontracts.CommandResult_ValidateCaptureDeclarationResponse, error) {
	return k.validate(r), nil
}
func integrationDefinitionsConnection(t *testing.T, k *integrationDefinitionsKernel) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	subscriptioncontracts.RegisterEventStoreSubscriptionsServer(server, k)
	webhookcontracts.RegisterWebhooksServer(server, k)
	servicecontracts.RegisterExternalServicesServer(server, k)
	capturecontracts.RegisterCapturesServer(server, k)
	done := make(chan struct{})
	t.Cleanup(func() {
		server.Stop()
		if err := listener.Close(); err != nil {
			t.Error(err)
		}
		<-done
	})
	go serveKernelFixture(server, listener, done, t.Error)
	conn, err := grpc.NewClient("passthrough:///integrations", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	return conn
}

type IntegrationChanged struct{ Value string }

func integrationCatalog(t *testing.T) *events.Catalog {
	t.Helper()
	event, err := events.Define[IntegrationChanged](events.WithID("IntegrationChanged"), events.WithGeneration(3))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}
func TestSubscriptionWireDefaultsOwnershipAndIsolation(t *testing.T) {
	catalog := integrationCatalog(t)
	var calls atomic.Int32
	kernel := &integrationDefinitionsKernel{
		subscribe: func(_ context.Context, r *subscriptioncontracts.AddEventStoreSubscriptions) error {
			calls.Add(1)
			want := &subscriptioncontracts.AddEventStoreSubscriptions{TargetEventStore: "target", Subscriptions: []*subscriptioncontracts.EventStoreSubscriptionDefinition{{Identifier: "source", SourceEventStore: "source", EventTypes: []*subscriptioncontracts.EventType{{Id: "IntegrationChanged", Generation: 1}}}}}
			if !proto.Equal(r, want) {
				t.Errorf("request = %v", r)
			}
			return nil
		},
		subscriptions: func(r *subscriptioncontracts.GetEventStoreSubscriptionsRequest) *subscriptioncontracts.IEnumerable_EventStoreSubscriptionDefinition {
			if r.TargetEventStore != "other" {
				t.Error("lost target scope")
			}
			return &subscriptioncontracts.IEnumerable_EventStoreSubscriptionDefinition{Items: []*subscriptioncontracts.EventStoreSubscriptionDefinition{{Identifier: "source", SourceEventStore: "source", EventTypes: []*subscriptioncontracts.EventType{{Id: "foreign", Generation: 9}}}}}
		},
		unsubscribe: func(r *subscriptioncontracts.RemoveEventStoreSubscriptions) {
			if r.TargetEventStore != "other" || !reflect.DeepEqual(r.SubscriptionIds, []string{"source"}) {
				t.Error("lost removal scope")
			}
		},
	}
	conn := integrationDefinitionsConnection(t, kernel)
	service, err := eventstoresubscriptions.New("target", catalog, conn)
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Subscribe(t.Context(), "source", "source"); err != nil {
		t.Fatal(err)
	}
	ids := []events.TypeID{"IntegrationChanged", "IntegrationChanged"}
	d, err := eventstoresubscriptions.Define(catalog, "source", "source", ids...)
	if err != nil {
		t.Fatal(err)
	}
	ids[0] = "changed"
	d.EventTypes()[0] = "changed"
	d.KernelDefinition().EventTypes[0].Id = "changed"
	if err = service.Register(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	other, err := eventstoresubscriptions.New("other", catalog, conn)
	if err != nil {
		t.Fatal(err)
	}
	got, err := other.GetAll(t.Context())
	if err != nil || len(got) != 1 || got[0].EventTypes()[0] != "foreign" || got[0].KernelDefinition().EventTypes[0].Generation != 1 {
		t.Fatalf("readback: %v %v", got, err)
	}
	if err = other.Unsubscribe(t.Context(), "source"); err != nil {
		t.Fatal(err)
	}
	if err = service.Subscribe(t.Context(), "bad", "target"); !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("invalid subscription dispatched")
	}
}
func TestWebhookWireAuthorizationReadbackAndRemoval(t *testing.T) {
	catalog := integrationCatalog(t)
	kernel := &integrationDefinitionsKernel{
		webhook: func(r *webhookcontracts.AddWebhooksRequest) *webhookcontracts.CommandResult {
			want := &webhookcontracts.AddWebhooksRequest{EventStore: "target", Webhooks: []*webhookcontracts.WebhookDefinition{{Identifier: "notify", EventSequenceId: "outbox", IsReplayable: false, IsActive: false, EventTypes: []*webhookcontracts.EventType{{Id: "IntegrationChanged", Generation: 3}}, Target: &webhookcontracts.WebhookTarget{Url: "https://example.test/events", Headers: map[string]string{"secret": "header"}, Authorization: &webhookcontracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value2: &webhookcontracts.OAuthAuthorization{Authority: "https://issuer.test", ClientId: "client", ClientSecret: "secret"}}}}}}
			if !proto.Equal(r, want) {
				t.Error("webhook did not match C# wire definition")
			}
			return &webhookcontracts.CommandResult{IsAuthorized: true}
		},
		webhooks: func(r *webhookcontracts.GetWebhooksRequest) *webhookcontracts.QueryResult_IEnumerable_WebhookDetailsResponse {
			if r.EventStore != "target" {
				t.Error("lost store")
			}
			return &webhookcontracts.QueryResult_IEnumerable_WebhookDetailsResponse{IsAuthorized: true, Data: []*webhookcontracts.WebhookDetailsResponse{{Identifier: "notify", Url: "https://example.test/events", EventSequenceId: "outbox", AuthorizationType: webhookcontracts.AuthorizationType_OAuth, Headers: map[string]string{"secret": "header"}, EventTypes: []*webhookcontracts.EventType{{Id: "IntegrationChanged", Generation: 3}}}}}
		},
		removeWebhook: func(r *webhookcontracts.RemoveWebhooksRequest) {
			if r.EventStore != "target" || !reflect.DeepEqual(r.Webhooks, []string{"notify"}) {
				t.Error("lost removal scope")
			}
		},
	}
	service, err := webhooks.New("target", catalog, integrationDefinitionsConnection(t, kernel))
	if err != nil {
		t.Fatal(err)
	}
	defaults, err := webhooks.Define(catalog, "default", "https://example.test")
	if err != nil {
		t.Fatal(err)
	}
	wireDefault := defaults.KernelDefinition()
	if !wireDefault.IsActive || !wireDefault.IsReplayable || wireDefault.EventSequenceId != "event-log" || wireDefault.Target.Authorization != nil {
		t.Fatal("defaults differ")
	}
	d, err := webhooks.Define(catalog, "notify", "https://example.test/events", webhooks.WithEventSequence(events.Outbox), webhooks.WithReplayable(false), webhooks.WithActive(false), webhooks.WithHeader("secret", "header"), webhooks.WithBasicAuth("old", "old"), webhooks.WithBearerToken("old"), webhooks.WithOAuth("https://issuer.test", "client", "secret"))
	if err != nil {
		t.Fatal(err)
	}
	copy := d.KernelDefinition()
	copy.Target.Headers["secret"] = "mutated"
	copy.Target.Authorization.Value2.ClientSecret = "mutated"
	if err = service.RegisterDefinition(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	if output := fmt.Sprintf("%v %+v %#v", d, d, d); strings.Contains(output, "header") || strings.Contains(output, "https") || strings.Contains(output, "secret") {
		t.Fatal("formatting exposed credentials")
	}
	got, err := service.GetAll(t.Context())
	if err != nil || len(got) != 1 || got[0].KernelDefinition().Target.Authorization != nil {
		t.Fatal("readback fabricated authorization", err)
	}
	if err = service.Remove(t.Context(), "notify"); err != nil {
		t.Fatal(err)
	}
}
func TestExternalServiceWireAndSecretOwnership(t *testing.T) {
	kernel := &integrationDefinitionsKernel{service: func(_ context.Context, r *servicecontracts.AddExternalServicesRequest) (*servicecontracts.CommandResult, error) {
		if r.EventStore != "target" || len(r.ExternalServices) != 1 {
			t.Fatal("invalid service scope")
		}
		d := r.ExternalServices[0]
		if d.Id != d.Name {
			t.Error("name must also be ID")
		}
		switch d.Name {
		case "api":
			want := &servicecontracts.ExternalServiceEndpoint{Type: servicecontracts.ExternalServiceEndpointType_Http, Http: &servicecontracts.HttpEndpointConfiguration{Url: "https://example.test", Headers: map[string]string{"x-secret": "header"}, Authorization: &servicecontracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value0: &servicecontracts.BasicAuthorization{Username: "user", Password: "secret"}}}}
			if !proto.Equal(d.Endpoint, want) {
				t.Error("HTTP definition differs")
			}
		case "database":
			want := &servicecontracts.ExternalServiceEndpoint{Type: servicecontracts.ExternalServiceEndpointType_PostgreSql, Database: &servicecontracts.DatabaseEndpointConfiguration{Host: "db", Database: "data", Username: "user", Password: "secret", Options: map[string]string{"ssl": "require", "extra": "value"}}}
			if !proto.Equal(d.Endpoint, want) {
				t.Error("database definition differs")
			}
		default:
			t.Error("unknown definition")
		}
		return &servicecontracts.CommandResult{IsAuthorized: true}, nil
	}}
	service, err := externalservices.New("target", integrationDefinitionsConnection(t, kernel))
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Register(t.Context(), "api", externalservices.HTTP("https://example.test"), externalservices.WithBasicAuth("user", "secret"), externalservices.WithHeader("x-secret", "header")); err != nil {
		t.Fatal(err)
	}
	options := map[string]string{"ssl": "require"}
	database := externalservices.Database{Host: "db", Name: "data", Username: "user", Password: "secret", Options: options}
	option := externalservices.PostgreSQL(database)
	options["ssl"] = "mutated"
	d, err := externalservices.Define("database", option, externalservices.WithOption("extra", "value"))
	if err != nil {
		t.Fatal(err)
	}
	d.KernelDefinition().Endpoint.Database.Options["ssl"] = "mutated"
	if err = service.RegisterDefinition(t.Context(), d); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(fmt.Sprintf("%#v %+v", d, database), "secret") {
		t.Fatal("credentials exposed by formatting")
	}
	for _, bad := range []externalservices.Option{externalservices.MSSQL(externalservices.Database{Host: "db", Name: "name", Port: 65536}), externalservices.HTTP("https://user:secret@example.test")} {
		if _, err = externalservices.Define("bad", bad); !errors.Is(err, faults.ErrInvalidConfiguration) || strings.Contains(err.Error(), "secret") {
			t.Fatal("unsafe validation", err)
		}
	}
}
func TestDefinitionAuthorizationVariantsAndEndpointSwitching(t *testing.T) {
	catalog := integrationCatalog(t)
	cases := []struct {
		name        string
		hook        webhooks.Option
		service     externalservices.Option
		wantHook    *webhookcontracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization
		wantService *servicecontracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization
	}{
		{"basic", webhooks.WithBasicAuth("user", "secret"), externalservices.WithBasicAuth("user", "secret"), &webhookcontracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value0: &webhookcontracts.BasicAuthorization{Username: "user", Password: "secret"}}, &servicecontracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value0: &servicecontracts.BasicAuthorization{Username: "user", Password: "secret"}}},
		{"bearer", webhooks.WithBearerToken("token"), externalservices.WithBearerToken("token"), &webhookcontracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value1: &webhookcontracts.BearerTokenAuthorization{Token: "token"}}, &servicecontracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value1: &servicecontracts.BearerTokenAuthorization{Token: "token"}}},
		{"oauth", webhooks.WithOAuth("https://issuer.test", "client", "secret"), externalservices.WithOAuth("https://issuer.test", "client", "secret"), &webhookcontracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value2: &webhookcontracts.OAuthAuthorization{Authority: "https://issuer.test", ClientId: "client", ClientSecret: "secret"}}, &servicecontracts.OneOf_BasicAuthorization_BearerTokenAuthorization_OAuthAuthorization{Value2: &servicecontracts.OAuthAuthorization{Authority: "https://issuer.test", ClientId: "client", ClientSecret: "secret"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hook, err := webhooks.Define(catalog, "notify", "https://example.test", tc.hook)
			if err != nil {
				t.Fatal(err)
			}
			service, err := externalservices.Define("api", externalservices.HTTP("https://example.test"), tc.service)
			if err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(hook.KernelDefinition().Target.Authorization, tc.wantHook) || !proto.Equal(service.KernelDefinition().Endpoint.Http.Authorization, tc.wantService) {
				t.Fatal("authorization fields differ")
			}
		})
	}
	definition, err := externalservices.Define("database", externalservices.WithOption("before", "retained"), externalservices.MSSQL(externalservices.Database{Host: "db", Name: "data", Port: 1433}), externalservices.WithBearerToken("unused"))
	if err != nil {
		t.Fatal(err)
	}
	endpoint := definition.KernelDefinition().Endpoint
	if endpoint.Type != servicecontracts.ExternalServiceEndpointType_MsSql || endpoint.Http != nil || endpoint.Database.Port != 1433 || endpoint.Database.Options["before"] != "retained" {
		t.Fatal("endpoint switching lost database settings or leaked HTTP authorization")
	}
}

func TestIntegrationServicesPreserveFailuresAndCancellationWithoutRetry(t *testing.T) {
	var calls atomic.Int32
	entered := make(chan struct{})
	kernel := &integrationDefinitionsKernel{
		subscribe: func(ctx context.Context, _ *subscriptioncontracts.AddEventStoreSubscriptions) error {
			calls.Add(1)
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		},
		webhook: func(*webhookcontracts.AddWebhooksRequest) *webhookcontracts.CommandResult {
			return &webhookcontracts.CommandResult{IsAuthorized: false}
		},
		service: func(context.Context, *servicecontracts.AddExternalServicesRequest) (*servicecontracts.CommandResult, error) {
			calls.Add(1)
			return nil, status.Error(codes.Unavailable, "unavailable")
		},
	}
	conn := integrationDefinitionsConnection(t, kernel)
	catalog := integrationCatalog(t)
	subscriptions, err := eventstoresubscriptions.New("target", catalog, conn)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- subscriptions.Subscribe(ctx, "source", "source") }()
	<-entered
	cancel()
	if err := <-done; status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
	hooks, err := webhooks.New("target", catalog, conn)
	if err != nil {
		t.Fatal(err)
	}
	var envelope *wire.EnvelopeError
	if err = hooks.Register(t.Context(), "hook", "https://example.test"); !errors.As(err, &envelope) {
		t.Fatal("envelope hidden", err)
	}
	services, err := externalservices.New("target", conn)
	if err != nil {
		t.Fatal(err)
	}
	if err = services.Register(t.Context(), "api", externalservices.HTTP("https://example.test")); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("write retried")
	}
}
func TestCaptureSubmissionReportsCapabilityAndPartialSave(t *testing.T) {
	event, err := events.Define[IntegrationChanged](events.WithID("IntegrationChanged"))
	if err != nil {
		t.Fatal(err)
	}
	d, err := new(captures.Builder).From(captures.API("api", "/items", "1m")).Key("id").Append(captures.Append(event, captures.Added(), map[string]string{"Value": "$.value"})).Build("ImportItems")
	if err != nil {
		t.Fatal(err)
	}
	kernel := &integrationDefinitionsKernel{
		validate: func(r *capturecontracts.ValidateCaptureDeclarationRequest) *capturecontracts.CommandResult_ValidateCaptureDeclarationResponse {
			if r.EventStore != "target" || r.Declaration != d.Declaration() {
				t.Error("capture validation wire")
			}
			return &capturecontracts.CommandResult_ValidateCaptureDeclarationResponse{IsAuthorized: true, Response: &capturecontracts.ValidateCaptureDeclarationResponse{Messages: []*capturecontracts.CaptureValidationMessage{{Message: "unsupported"}}}}
		},
		capture: func(r *capturecontracts.SaveCaptureRequest) *capturecontracts.CommandResult_SaveCaptureResponse {
			if r.EventStore != "target" || r.Declaration != d.Declaration() || wire.Correlation(r.Id).String() != d.ID().String() {
				t.Error("capture save wire")
			}
			return &capturecontracts.CommandResult_SaveCaptureResponse{IsAuthorized: true, Response: &capturecontracts.SaveCaptureResponse{Capture: &capturecontracts.CaptureDetailsResponse{Id: wire.Correlation(r.Id).String()}, Messages: []*capturecontracts.CaptureValidationMessage{{Message: "unsupported"}}}}
		},
	}
	service, err := captures.New("target", integrationDefinitionsConnection(t, kernel))
	if err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{service.Validate(t.Context(), d), service.Save(t.Context(), d)} {
		var failure *captures.ValidationError
		if !errors.As(err, &failure) || len(failure.Messages) != 1 {
			t.Fatal("capability silently succeeded", err)
		}
	}
}
