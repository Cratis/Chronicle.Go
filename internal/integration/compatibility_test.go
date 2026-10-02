//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/connection"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

type kernelFixture struct {
	t         *testing.T
	ctx       context.Context
	endpoint  string
	conn      *grpc.ClientConn
	storeName chronicle.StoreName
}

func newKernelFixture(t *testing.T) *kernelFixture {
	t.Helper()
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		t.Fatal("set CHRONICLE_INTEGRATION_CONNECTION_STRING; integration tests never silently skip")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	t.Cleanup(cancel)
	uri, err := chronicle.ParseConnectionString(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	policy := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} // Test-owned development kernel only.
	tokens := connection.NewOAuth(uri.Addresses()[0].String(), "chronicle-dev-client", "chronicle-dev-secret", policy)
	t.Cleanup(tokens.Close)
	token, err := tokens.Token(ctx)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(uri.Addresses()[0].String(), grpc.WithTransportCredentials(credentials.NewTLS(policy)), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	return &kernelFixture{t: t, ctx: metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token.AccessToken), endpoint: endpoint, conn: conn, storeName: chronicle.StoreName("go-regression-" + uuid.NewString())}
}

func (f *kernelFixture) client(registry *chronicle.Registry, options ...chronicle.ClientOption) *chronicle.Client {
	f.t.Helper()
	base := []chronicle.ClientOption{chronicle.WithConnectionString(f.endpoint), chronicle.WithDevelopmentDefaults(), chronicle.WithRegistry(registry)}
	client, err := chronicle.Dial(f.ctx, append(base, options...)...)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(func() {
		if err := client.Close(); err != nil {
			f.t.Error(err)
		}
	})
	return client
}

func (f *kernelFixture) read(source events.SourceID) []*sequences.AppendedEventResponse {
	f.t.Helper()
	response, err := sequences.NewEventSequencesClient(f.conn).ForEventSourceIdAndEventTypes(f.ctx, &sequences.ForEventSourceIdAndEventTypesRequest{EventStore: string(f.storeName), Namespace: string(chronicle.DefaultNamespace), EventSequenceId: "event-log", EventSourceId: string(source)})
	if err != nil {
		f.t.Fatal(err)
	}
	if err = wire.CheckEnvelope(response); err != nil {
		f.t.Fatal(err)
	}
	return response.Data
}

func integrationRegistry[T any](t *testing.T, options ...events.TypeOption) *chronicle.Registry {
	t.Helper()
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[T](registry, options...); err != nil {
		t.Fatal(err)
	}
	return registry
}

func appendSuccessfully(t *testing.T, ctx context.Context, store *chronicle.EventStore, source events.SourceID, value any, options ...eventsequences.AppendOption) eventsequences.AppendResult {
	t.Helper()
	result, err := store.EventLog().Append(ctx, source, value, options...)
	if err != nil || result.Err() != nil || result.Disposition != eventsequences.Committed {
		var envelope *chronicle.EnvelopeError
		if errors.As(err, &envelope) {
			t.Logf("kernel exceptions: %v; validation: %v", envelope.ExceptionMessages, envelope.ValidationResults)
		}
		t.Fatalf("append: %+v, %v", result, err)
	}
	return result
}

type ChangedCustomer struct{ Renamed string }

func TestKernelSchemaValidationPreservesExistingGeneration(t *testing.T) {
	f := newKernelFixture(t)
	original := f.client(integrationRegistry[CustomerRegistered](t, events.WithID("stable-event")), chronicle.WithEventTypeGenerationValidation(true))
	store, err := original.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	appendSuccessfully(t, f.ctx, store, "customer", CustomerRegistered{Name: "Ada", Balance: 1<<53 + 1})
	storedSchema := func() string {
		response, err := eventtypes.NewEventTypesClient(f.conn).AllEventTypeGenerations(f.ctx, &eventtypes.AllEventTypeGenerationsRequest{EventStore: string(f.storeName), EventTypeId: "stable-event"})
		if err != nil {
			t.Fatal(err)
		}
		if err = wire.CheckEnvelope(response); err != nil {
			t.Fatal(err)
		}
		if len(response.Data) != 1 {
			t.Fatalf("event types = %d", len(response.Data))
		}
		return response.Data[0].Schema
	}
	before := storedSchema()
	changed := f.client(integrationRegistry[ChangedCustomer](t, events.WithID("stable-event")), chronicle.WithEventTypeGenerationValidation(true))
	_, err = changed.EventStore(f.ctx, f.storeName)
	var envelope *chronicle.EnvelopeError
	if !errors.As(err, &envelope) || len(envelope.ExceptionMessages) == 0 {
		t.Fatalf("changed generation accepted: %v", err)
	}
	if after := storedSchema(); before != after {
		t.Fatal("rejected registration overwrote the stored schema")
	}
	persisted := f.read("customer")
	if len(persisted) != 1 {
		t.Fatalf("events = %d", len(persisted))
	}
	var got CustomerRegistered
	if err = json.Unmarshal([]byte(persisted[0].Content), &got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "Ada" || got.Balance != 1<<53+1 {
		t.Fatalf("history changed: %+v", got)
	}
}

func TestKernelGenerationWithoutMigrationsMatchesCSharp(t *testing.T) {
	f := newKernelFixture(t)
	registry := integrationRegistry[CustomerRegistered](t, events.WithGeneration(2))
	client := f.client(registry)
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	appendSuccessfully(t, f.ctx, store, "generation-two", CustomerRegistered{Name: "Ada"})
	persisted := f.read("generation-two")
	if len(persisted) != 1 || persisted[0].Context.EventType.Generation != 2 {
		t.Fatalf("generation two not persisted: %v", persisted)
	}
	strict := f.client(registry, chronicle.WithEventTypeGenerationValidation(true))
	_, err = strict.EventStore(f.ctx, f.storeName)
	var envelope *chronicle.EnvelopeError
	if !errors.As(err, &envelope) {
		t.Fatalf("missing migration chain accepted with validation enabled: %v", err)
	}
}

type NamedUnsigned uint64
type OmittedNumber int64

func (v OmittedNumber) IsZero() bool { return v == -1 }

type ScalarValues struct {
	Signed        int64
	Unsigned      uint64
	Named         NamedUnsigned
	SmallSigned   int8
	ShortSigned   int16
	Int32         int32
	SmallUnsigned uint8
	ShortUnsigned uint16
	Uint32        uint32
	NativeInt     int
	NativeUint    uint
	Values        []uint64
	Flag          *bool
	Count         *int64
	When          *time.Time
	ID            *uuid.UUID
	Omitted       *OmittedNumber `json:"omitted,omitzero"`
}

func TestKernelScalarAndNullableRoundTrips(t *testing.T) {
	f := newKernelFixture(t)
	client := f.client(integrationRegistry[ScalarValues](t), chronicle.WithEventTypeGenerationValidation(true))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	populated := ScalarValues{Signed: math.MinInt64, Unsigned: math.MaxInt64, Named: 1<<53 + 1, SmallSigned: math.MinInt8, ShortSigned: math.MinInt16, Int32: math.MinInt32, SmallUnsigned: math.MaxUint8, ShortUnsigned: math.MaxUint16, Uint32: math.MaxUint32, NativeInt: 1<<31 + 1, NativeUint: 1<<32 + 1, Values: []uint64{1<<53 + 1, math.MaxInt64}, Flag: new(false), Count: new(int64(0)), When: new(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)), ID: new(uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")), Omitted: new(OmittedNumber(-1))}
	appendSuccessfully(t, f.ctx, store, "values", ScalarValues{})
	appendSuccessfully(t, f.ctx, store, "values", populated)
	if _, err = store.EventLog().Append(f.ctx, "values", ScalarValues{Unsigned: math.MaxUint64}); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("unsigned BSON overflow was not rejected locally: %v", err)
	}
	populated.Omitted = nil // IsZero deliberately omits this nonzero pointer value.
	persisted := f.read("values")
	if len(persisted) != 2 {
		t.Fatalf("events = %d", len(persisted))
	}
	for i, want := range []ScalarValues{{}, populated} {
		var got ScalarValues
		if err = json.Unmarshal([]byte(persisted[i].Content), &got); err != nil {
			t.Fatalf("decode %s: %v", persisted[i].Content, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round trip %d: got %+v, want %+v; JSON %s", i, got, want, persisted[i].Content)
		}
	}
}

type DictionaryValues struct {
	Unsigned map[string]uint64
	Signed   map[string]int64
	Nested   map[string][]NamedUnsigned
	Labels   map[string]string
}

func TestKernelDictionaryPrecisionGuard(t *testing.T) {
	f := newKernelFixture(t)
	client := f.client(integrationRegistry[DictionaryValues](t), chronicle.WithEventTypeGenerationValidation(true))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	want := DictionaryValues{Unsigned: map[string]uint64{"boundary": 1 << 53}, Signed: map[string]int64{"boundary": -(1 << 53)}, Nested: map[string][]NamedUnsigned{"values": {1 << 53}}, Labels: map[string]string{"CaseSensitive": "label"}}
	appendSuccessfully(t, f.ctx, store, "dictionary", want)
	for _, value := range []DictionaryValues{
		{Unsigned: map[string]uint64{"n": 1<<53 + 1}}, {Signed: map[string]int64{"n": -(1 << 53) - 1}}, {Nested: map[string][]NamedUnsigned{"values": {1<<53 + 1}}},
	} {
		if _, err = store.EventLog().Append(f.ctx, "dictionary", value); !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatalf("unsafe dictionary accepted: %v", err)
		}
	}
	persisted := f.read("dictionary")
	if len(persisted) != 1 {
		t.Fatalf("unsafe dictionary reached storage: events = %d", len(persisted))
	}
	var got DictionaryValues
	if err = json.Unmarshal([]byte(persisted[0].Content), &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dictionary rounded: got %+v, want %+v", got, want)
	}
}

func TestKernelExactIsAnUpperBound(t *testing.T) {
	f := newKernelFixture(t)
	client := f.client(integrationRegistry[CustomerRegistered](t))
	store, err := client.EventStore(f.ctx, f.storeName)
	if err != nil {
		t.Fatal(err)
	}
	source := events.SourceID("upper-bound")
	scope := eventsequences.Scope{Expectation: eventsequences.Exact(10), Filter: eventsequences.ScopeFilter{SourceID: &source}}
	for range 2 {
		result := appendSuccessfully(t, f.ctx, store, source, CustomerRegistered{Name: "Ada"}, eventsequences.WithScope(scope))
		if !result.ConcurrencyCheckPerformed {
			t.Fatal("explicit upper bound was not checked")
		}
	}
	scope.Expectation = eventsequences.Exact(0)
	result, err := store.EventLog().Append(f.ctx, source, CustomerRegistered{}, eventsequences.WithScope(scope))
	if err != nil || result.Disposition != eventsequences.Rejected || len(result.ConcurrencyViolations) != 1 {
		t.Fatalf("tail above bound not rejected: %+v %v", result, err)
	}
	if persisted := f.read(source); len(persisted) != 2 {
		t.Fatalf("events = %d", len(persisted))
	}
}
