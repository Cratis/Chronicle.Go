// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/constraints"
	constraintcontracts "github.com/cratis/chronicle.go/contracts/events/constraints"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	projectioncontracts "github.com/cratis/chronicle.go/contracts/projections"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/derivedfixtures"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type factoryCodecCurrent struct {
	DisplayName string
	Member      derivedfixtures.Member
}
type factoryCodecPrevious struct {
	Name   string
	Member derivedfixtures.Member
}
type factoryCodecModel struct {
	ID          string `chronicle:"key"`
	DisplayName string
	Member      derivedfixtures.Member
}

// The family remains unprotected and is an ordinary scalar wire property. The
// migration witness only compiles scalar name renaming: it does not claim kernel
// migration fidelity for the open-object family payload.
func TestFactoryCodecsSurviveNamingInitialStateStoreBindingAndReconnect(t *testing.T) {
	for _, policy := range []serialization.NamingPolicy{serialization.PreservePropertyNames, serialization.CamelCase} {
		t.Run(map[serialization.NamingPolicy]string{serialization.PreservePropertyNames: "preserved", serialization.CamelCase: "camel"}[policy], func(t *testing.T) {
			codecs, err := derivedfixtures.Codecs()
			if err != nil {
				t.Fatal(err)
			}
			var callbacks, classifiers atomic.Int32
			classifier := compliance.Using(func(compliance.Target) (compliance.Classification, error) {
				classifiers.Add(1)
				return compliance.Classification{}, nil
			})
			registry := NewRegistry()
			current, err := RegisterEvent[factoryCodecCurrent](registry, events.WithCodecs(codecs), events.WithGeneration(2), events.WithSourceStore("origin"), events.WithProtection(classifier))
			if err != nil {
				t.Fatal(err)
			}
			previous, err := RegisterEventGeneration[factoryCodecPrevious](registry, current, 1, events.WithCodecs(codecs), events.WithProtection(classifier))
			if err != nil {
				t.Fatal(err)
			}
			model, err := RegisterReadModel[factoryCodecModel](registry, readmodels.WithCodecs(codecs), readmodels.WithProtection(classifier))
			if err != nil {
				t.Fatal(err)
			}
			member := &derivedfixtures.HumanValue{Name: "frozen", Nested: derivedfixtures.Detail{DisplayName: "nested"}, Children: []derivedfixtures.Member{derivedfixtures.RobotValue{Count: 7}}}
			labels := []string{"factory-codecs"}
			factory := func() preparationDependency { callbacks.Add(1); return preparationDependency{} }
			if err := RegisterProjectionFactory(registry, "codec-projection", model.Descriptor(), factory, func(context.Context, preparationDependency) (projections.Declaration, error) {
				callbacks.Add(1)
				// Mutating caller configuration and this registry cannot change any
				// selected capture or the codecs inside already compiled plans.
				*codecs = serialization.Codecs{}
				if _, err := RegisterEvent[catalogReplacement](registry); err != nil {
					return projections.Declaration{}, err
				}
				return projections.ModelBound(model, projections.WithIdentifier("codec-projection"), projections.FromEvent(current), projections.WithLabels(labels...), projections.WithInitialValues(factoryCodecModel{DisplayName: "initial", Member: member})), nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := RegisterConstraintFactory(registry, []string{"codec-name"}, factory, func(context.Context, preparationDependency) ([]constraints.Definition, error) {
				callbacks.Add(1)
				definition, err := constraints.UniqueValues("codec-name").On(current.Descriptor(), "DisplayName").WithMessage("already used").Build()
				return []constraints.Definition{definition}, err
			}); err != nil {
				t.Fatal(err)
			}
			if err := RegisterEventMigrationFactory(registry, current.Descriptor(), previous.Descriptor(), factory, func(context.Context, preparationDependency) (events.MigrationDeclaration, error) {
				callbacks.Add(1)
				return events.DefineMigration(current, previous, events.Migration[factoryCodecCurrent, factoryCodecPrevious]{
					Upcast: func(b *events.MigrationBuilder[factoryCodecCurrent, factoryCodecPrevious]) {
						b.RenamedFrom("DisplayName", "Name")
					},
					Downcast: func(b *events.MigrationBuilder[factoryCodecPrevious, factoryCodecCurrent]) {
						b.RenamedFrom("Name", "DisplayName")
					},
				})
			}); err != nil {
				t.Fatal(err)
			}
			eventsSent := make(chan *eventtypes.RegisterEventTypesRequest, 2)
			constraintsSent := make(chan *constraintcontracts.RegisterConstraintsRequest, 2)
			modelsSent := make(chan *modelcontracts.RegisterManyRequest, 2)
			projectionsSent := make(chan *projectioncontracts.RegisterRequest, 2)
			kernel := &supervisedKernel{
				registerEventTypes: func(_ context.Context, r *eventtypes.RegisterEventTypesRequest) error { eventsSent <- r; return nil },
				registerConstraints: func(_ context.Context, r *constraintcontracts.RegisterConstraintsRequest) error {
					constraintsSent <- r
					return nil
				},
				readModels: &readModelKernel{register: func(_ context.Context, r *modelcontracts.RegisterManyRequest) error { modelsSent <- r; return nil }},
				projections: &projectionKernel{register: func(_ context.Context, r *projectioncontracts.RegisterRequest) error {
					projectionsSent <- r
					return nil
				}},
			}
			client, ctx := supervisionClient(t, kernel, WithRegistry(registry), WithRegistryForStore("mirror", registry), WithNamingPolicy(policy), WithEventTypeGenerationValidation(true))
			member.Name, member.Nested.DisplayName, labels[0] = "mutated", "mutated", "mutated"
			preparedCalls, classified := callbacks.Load(), classifiers.Load()
			if preparedCalls != 6 || classified == 0 {
				t.Fatal("shared registry did not prepare exactly once", preparedCalls, classified)
			}
			property := "DisplayName"
			if policy == serialization.CamelCase {
				property = "displayName"
			}
			for _, storeName := range []StoreName{"origin", "mirror"} {
				artifacts, err := client.Artifacts(storeName)
				if err != nil {
					t.Fatal(err)
				}
				if len(artifacts.Events.Descriptors()) != 2 || len(artifacts.Projections) != 1 || len(artifacts.Constraints) != 1 {
					t.Fatal("late declaration admitted or frozen definition lost")
				}
				migrations := artifacts.Events.Migrations()
				if len(migrations) != 1 || !strings.Contains(migrations[0].UpcastJSON, `"`+property+`"`) {
					t.Fatal("factory migration lost naming or historical endpoint", migrations)
				}
				projection := artifacts.Projections[0]
				wantSequence := events.EventLog
				if storeName == "mirror" {
					wantSequence = "inbox-origin"
				}
				if projection.EventSequence() != wantSequence {
					t.Fatal("store binding lost source metadata", projection.EventSequence())
				}
				definition := projection.KernelDefinition()
				var initial map[string]json.RawMessage
				if err := json.Unmarshal([]byte(definition.InitialModelState), &initial); err != nil || initial[property] == nil {
					t.Fatal("initial state was not rebound to current naming", err)
				}
				decoded, err := projection.Model().Unmarshal([]byte(definition.InitialModelState))
				if err != nil {
					t.Fatal(err)
				}
				want := &derivedfixtures.HumanValue{Name: "frozen", Nested: derivedfixtures.Detail{DisplayName: "nested"}, Children: []derivedfixtures.Member{derivedfixtures.RobotValue{Count: 7}}}
				if !reflect.DeepEqual(decoded.(*factoryCodecModel).Member, want) || len(definition.Tags) != 1 || definition.Tags[0] != "factory-codecs" {
					t.Fatalf("initial codec value or labels were not snapshotted: member=%#v want=%#v labels=%v state=%s", decoded.(*factoryCodecModel).Member, want, definition.Tags, definition.InitialModelState)
				}
				for _, descriptor := range artifacts.Events.Descriptors() {
					var value any = factoryCodecCurrent{DisplayName: "event", Member: derivedfixtures.RobotValue{Count: 42}}
					var target any = &factoryCodecCurrent{}
					if descriptor.IsHistorical() {
						value, target = factoryCodecPrevious{Name: "event", Member: derivedfixtures.RobotValue{Count: 42}}, &factoryCodecPrevious{}
					}
					encoded, err := descriptor.Marshal(value)
					if err != nil || descriptor.Unmarshal(encoded, target) != nil || !reflect.DeepEqual(value, reflect.ValueOf(target).Elem().Interface()) {
						t.Fatal("current/historical factory codec plan lost", err)
					}
					if _, err := descriptor.WithNamingPolicy(serialization.CamelCase); err != nil {
						t.Fatal("naming recompile discarded codecs", err)
					}
				}
				if _, err := projection.Model().WithNamingPolicy(serialization.CamelCase); err != nil {
					t.Fatal("model naming recompile discarded codecs", err)
				}
			}
			store, err := client.EventStore(ctx, "origin", WithNamespace("one"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.EventStore(ctx, "origin", WithNamespace("two")); err != nil {
				t.Fatal(err)
			}
			before, err := store.WaitForRegistration(ctx)
			if err != nil {
				t.Fatal(err)
			}
			receive := func() []proto.Message {
				return []proto.Message{receiveDefinition(t, ctx, eventsSent), receiveDefinition(t, ctx, constraintsSent), receiveDefinition(t, ctx, modelsSent), receiveDefinition(t, ctx, projectionsSent)}
			}
			first := receive()
			kernel.endStream <- status.Error(codes.Unavailable, "replace codec generation")
			second := receive()
			for i := range first {
				if !proto.Equal(first[i], second[i]) {
					t.Fatalf("reconnect changed factory codec request at stage %d", i)
				}
			}
			after, err := store.WaitForRegistration(ctx)
			if err != nil || after.Generation <= before.Generation || callbacks.Load() != preparedCalls || classifiers.Load() != classified {
				t.Fatal("reconnect reran preparation/classification or lost generation", err)
			}
		})
	}
}
