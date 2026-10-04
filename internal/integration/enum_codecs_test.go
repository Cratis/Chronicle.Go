//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/enumfixtures"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/google/uuid"
)

type enumScalarModel[T ~int32] struct {
	ID    string `json:"id"`
	Value T
}
type enumArrayModel struct {
	ID     string `json:"id"`
	Values []enumfixtures.Bits
}
type enumOptionalModel struct {
	ID    string `json:"id"`
	Value *enumfixtures.Int32Sample
}

type enumKernelCapture struct {
	Enums []struct {
		Name    string
		Members []struct{ Name, Numeric string }
	}
	Profiles []struct {
		NamingPolicy string
		Cases        []struct {
			DeclaredType, ID, Operation string
			Input                       json.RawMessage
			Result                      struct {
				Status      string
				Output      json.RawMessage
				Reserialize *struct {
					Status string
					Output json.RawMessage
				}
			}
		}
	}
}
type enumKernelRoundtrip struct {
	NamingPolicy string          `json:"namingPolicy"`
	DeclaredType string          `json:"declaredType"`
	ID           string          `json:"id"`
	Payload      string          `json:"payload"`
	Expected     json.RawMessage `json:"expected"`
}

func TestKernelDeclaredInt32EnumProfile(t *testing.T) {
	data, err := os.ReadFile("../../serialization/testdata/enum/profile.json")
	if err != nil {
		t.Fatal(err)
	}
	var capture enumKernelCapture
	if err := json.Unmarshal(data, &capture); err != nil {
		t.Fatal(err)
	}
	var outputs []enumKernelRoundtrip
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase} {
		naming := "Cratis.Serialization.DefaultNamingPolicy"
		if policy == serialization.CamelCase {
			naming = "Cratis.Serialization.CamelCaseNamingPolicy"
		}
		t.Run(naming, func(t *testing.T) {
			outputs = append(outputs, enumKernelScenario[enumfixtures.Scalar[enumfixtures.Int32Sample], enumScalarModel[enumfixtures.Int32Sample]](t, capture, policy, naming, "Scalar<Int32Sample>", "es", "Value", []string{"numeric:-2147483648", "numeric:2147483647", "numeric:-1", "numeric:1", "numeric:0"})...)
			outputs = append(outputs, enumKernelScenario[enumfixtures.Scalar[enumfixtures.AllBits], enumScalarModel[enumfixtures.AllBits]](t, capture, policy, naming, "Scalar<AllBits>", "ef", "Value", []string{"numeric:-1", "numeric:3", "numeric:0"})...)
			outputs = append(outputs, enumKernelScenario[enumfixtures.ArrayValue[enumfixtures.Bits], enumArrayModel](t, capture, policy, naming, "ArrayValue<Bits>", "ea", "Values", []string{"declared-values", "empty"})...)
			outputs = append(outputs, enumKernelScenario[enumfixtures.NullableScalar[enumfixtures.Int32Sample], enumOptionalModel](t, capture, policy, naming, "NullableScalar<Int32Sample>", "en", "Value", []string{"null", "one"})...)
		})
	}
	if len(outputs) != 96 {
		t.Fatalf("kernel typed roundtrips=%d, want 96", len(outputs))
	}
	if path := os.Getenv("CHRONICLE_ENUM_KERNEL_OUTPUT"); path != "" {
		data, err := json.MarshalIndent(outputs, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := file.Write(data)
		closeErr := file.Close()
		if writeErr != nil {
			t.Fatal(writeErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
	}
	t.Logf("verified %d raw event/model readbacks from captured C# and Go enum writes", len(outputs))
}

func enumKernelScenario[E, M any](t *testing.T, capture enumKernelCapture, policy serialization.NamingPolicy, naming, declared, id, field string, cases []string) []enumKernelRoundtrip {
	t.Helper()
	fixture := newKernelFixture(t)
	fixture.storeName = chronicle.StoreName("enum-" + uuid.NewString()[:8])
	codecs, err := enumfixtures.Codecs()
	if err != nil {
		t.Fatal(err)
	}
	registry := chronicle.NewRegistry()
	event, err := chronicle.RegisterEvent[E](registry, events.WithID(events.TypeID(id)), events.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[M](registry, readmodels.WithIdentifier(readmodels.Identifier(id)), readmodels.WithContainerName(id), readmodels.WithCodecs(codecs))
	if err != nil {
		t.Fatal(err)
	}
	builder := projections.NewBuilder(id, model)
	projections.From(builder, event, nil)
	projection, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AddProjection(projection); err != nil {
		t.Fatal(err)
	}
	client := fixture.client(registry, chronicle.WithNamingPolicy(policy))
	store, err := client.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := client.Artifacts(fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := artifacts.Events.LookupRef(event.Ref())
	if !ok {
		t.Fatal("enum event not registered")
	}
	var outputs []enumKernelRoundtrip
	for i, caseID := range cases {
		var payload string
		var expected json.RawMessage
		matches := 0
		for _, profile := range capture.Profiles {
			if profile.NamingPolicy != naming {
				continue
			}
			for _, c := range profile.Cases {
				if c.DeclaredType != declared || c.ID != caseID {
					continue
				}
				if c.Operation == "EventSerializer.Serialize" {
					matches++
					if c.Result.Status != "accepted" || json.Unmarshal(c.Result.Output, &payload) != nil {
						t.Fatal("captured write failed")
					}
					expected = c.Input
				} else if c.Operation == "EventSerializer.Deserialize" && caseID == "one" {
					matches++
					if c.Result.Status != "accepted" || c.Result.Reserialize == nil || c.Result.Reserialize.Status != "accepted" || json.Unmarshal(c.Result.Reserialize.Output, &payload) != nil {
						t.Fatal("captured nullable reserialization failed")
					}
					expected = c.Result.Output
				}
			}
		}
		if matches != 1 {
			t.Fatalf("expected one capture for %s/%s, got %d", declared, caseID, matches)
		}
		var original E
		if err := descriptor.Unmarshal([]byte(payload), &original); err != nil {
			t.Fatal(err)
		}
		appendSuccessfully(t, fixture.ctx, store, "go", original)
		response, err := sequences.NewEventSequencesClient(fixture.conn).Append(fixture.ctx, &sequences.AppendRequest{
			EventStore: string(fixture.storeName), Namespace: string(chronicle.DefaultNamespace), EventSequenceId: "event-log", EventSourceId: "csharp",
			EventType: &sequences.EventType{Id: id, Generation: 1}, Content: payload, CorrelationId: wire.Guid(metadata.CorrelationID(uuid.New())),
			Occurred: &sequences.SerializableDateTimeOffset{Value: wire.DateTimeOffset(time.Now().UTC())}, CausedBy: &sequences.Identity{}, ConcurrencyScope: &sequences.ConcurrencyScope{SequenceNumber: uint64(events.Unavailable)},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := wire.CheckEnvelope(response); err != nil {
			t.Fatal(err)
		}
		if response.Response == nil || !response.Response.IsSuccess {
			t.Fatalf("captured enum append failed for %s/%s: %v", declared, caseID, response.Response)
		}
		for _, source := range []events.SourceID{"go", "csharp"} {
			stored := fixture.read(source)
			if len(stored) != i+1 {
				t.Fatalf("history=%d, want %d", len(stored), i+1)
			}
			content := stored[i].Content
			var decoded E
			if err := descriptor.Unmarshal([]byte(content), &decoded); err != nil || !reflect.DeepEqual(decoded, original) {
				t.Fatalf("enum event readback %s/%s: %v", declared, caseID, err)
			}
			want := reflect.ValueOf(original).FieldByName(field).Interface()
			awaitProjection(t, fixture.ctx, readmodels.For(store.ReadModels(), model), readmodels.Key(source), func(value M) bool {
				return reflect.DeepEqual(reflect.ValueOf(value).FieldByName(field).Interface(), want)
			})
			raw, err := store.ReadModels().Get(fixture.ctx, model.Identifier(), readmodels.Key(source))
			if err != nil || !raw.Exists {
				t.Fatalf("enum model read: %v", err)
			}
			assertEnumKernelNames(t, capture, policy, field, string(raw.Value), expected)
			outputs = append(outputs, enumKernelRoundtrip{naming, declared, "event/" + string(source) + "/" + caseID, content, expected}, enumKernelRoundtrip{naming, declared, "model/" + string(source) + "/" + caseID, string(raw.Value), expected})
		}
	}
	// Every profile rejects unknown payloads before append. The persisted source
	// must remain empty; a separate transport unit test counts zero Append RPCs.
	invalid := reflect.New(reflect.TypeFor[E]()).Elem()
	v := invalid.FieldByName(field)
	switch v.Kind() {
	case reflect.Int32:
		v.SetInt(9)
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		v.Elem().SetInt(9)
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 1, 1))
		v.Index(0).SetInt(9)
	}
	if _, err := store.EventLog().Append(fixture.ctx, "invalid", invalid.Interface()); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("unknown enum append: %v", err)
	}
	if len(fixture.read("invalid")) != 0 {
		t.Fatal("unknown enum persisted")
	}
	return outputs
}
