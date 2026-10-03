// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/constraints"
	constraintcontracts "github.com/cratis/chronicle.go/contracts/events/constraints"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	projectioncontracts "github.com/cratis/chronicle.go/contracts/projections"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type definitionCurrent struct{ Title string }
type definitionMiddle struct{ Title string }
type definitionPrevious struct{ Title string }
type definitionDirectModel struct{ ID, Title string }

func mixedFactoryRegistry(t *testing.T, factories bool, callbacks, classifiers *atomic.Int32) *Registry {
	t.Helper()
	registry := NewRegistry()
	current, err := RegisterEvent[definitionCurrent](registry, events.WithGeneration(3), events.WithProtection(compliance.Using(func(compliance.Target) (compliance.Classification, error) {
		classifiers.Add(1)
		return compliance.Classification{}, nil
	})))
	if err != nil {
		t.Fatal(err)
	}
	middle, err := RegisterEventGeneration[definitionMiddle](registry, current, 2)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := RegisterEventGeneration[definitionPrevious](registry, current, 1)
	if err != nil {
		t.Fatal(err)
	}
	model, err := RegisterReadModel[catalogModel](registry)
	if err != nil {
		t.Fatal(err)
	}
	directModel, err := RegisterReadModel[definitionDirectModel](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AddProjection(projections.ModelBound(directModel, projections.WithIdentifier("direct"), projections.FromEvent(current))); err != nil {
		t.Fatal(err)
	}
	directConstraint, err := constraints.UniqueEventTypes(current.Descriptor()).WithName("direct").Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.AddConstraint(directConstraint); err != nil {
		t.Fatal(err)
	}
	if err := RegisterEventMigration(registry, middle, previous, events.Migration[definitionMiddle, definitionPrevious]{Upcast: func(*events.MigrationBuilder[definitionMiddle, definitionPrevious]) {}, Downcast: func(*events.MigrationBuilder[definitionPrevious, definitionMiddle]) {}}); err != nil {
		t.Fatal(err)
	}
	projection := func(context.Context, preparationDependency) (projections.Declaration, error) {
		callbacks.Add(1)
		return projections.ModelBound(model, projections.WithIdentifier("prepared"), projections.FromEvent(current), projections.WithLabels("factory"), projections.WithInitialValues(catalogModel{Title: "frozen"})), nil
	}
	constraint := func(context.Context, preparationDependency) ([]constraints.Definition, error) {
		callbacks.Add(1)
		definition, err := constraints.UniqueValues("prepared").On(current.Descriptor(), "Title").WithMessage("static").Build()
		return []constraints.Definition{definition}, err
	}
	migration := func(context.Context, preparationDependency) (events.MigrationDeclaration, error) {
		callbacks.Add(1)
		return events.DefineMigration(current, middle, events.Migration[definitionCurrent, definitionMiddle]{Upcast: func(b *events.MigrationBuilder[definitionCurrent, definitionMiddle]) {
			b.DefaultValue("Title", "frozen")
		}, Downcast: func(*events.MigrationBuilder[definitionMiddle, definitionCurrent]) {}})
	}
	if factories {
		factory := func() preparationDependency { callbacks.Add(1); return preparationDependency{} }
		for _, err := range []error{RegisterProjectionFactory(registry, "prepared", model.Descriptor(), factory, projection), RegisterConstraintFactory(registry, []string{"prepared"}, factory, constraint), RegisterEventMigrationFactory(registry, current.Descriptor(), middle.Descriptor(), factory, migration)} {
			if err != nil {
				t.Fatal(err)
			}
		}
	} else {
		projection, err := projection(t.Context(), preparationDependency{})
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.AddProjection(projection); err != nil {
			t.Fatal(err)
		}
		definitions, err := constraint(t.Context(), preparationDependency{})
		if err != nil {
			t.Fatal(err)
		}
		if err := registry.AddConstraint(definitions[0]); err != nil {
			t.Fatal(err)
		}
		declaration, err := migration(t.Context(), preparationDependency{})
		if err != nil {
			t.Fatal(err)
		}
		registry.migrations = append(registry.migrations, declaration)
	}
	return registry
}

func receiveDefinition[T proto.Message](t *testing.T, ctx context.Context, requests <-chan T) T {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		var zero T
		return zero
	}
}

func TestFactoryAndDirectRegistrationUnionMatchesAndReconnectIsCallbackFree(t *testing.T) {
	var golden []proto.Message
	for _, factories := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct golden", true: "mixed factories"}[factories], func(t *testing.T) {
			var callbacks, classifiers atomic.Int32
			registry := mixedFactoryRegistry(t, factories, &callbacks, &classifiers)
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
			client, ctx := supervisionClient(t, kernel, WithRegistry(registry), WithNamingPolicy(serialization.CamelCase), WithEventTypeGenerationValidation(true))
			store, err := client.EventStore(ctx, "store", WithNamespace("one"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := client.EventStore(ctx, "store", WithNamespace("two")); err != nil {
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
			if !factories {
				golden = first
				return
			}
			for i := range first {
				if !proto.Equal(first[i], golden[i]) {
					t.Fatalf("mixed registration differs from direct golden at stage %d", i)
				}
			}
			if callbacks.Load() != 6 {
				t.Fatal("shared definitions did not prepare exactly once", callbacks.Load())
			}
			classified := classifiers.Load()
			if classified == 0 {
				t.Fatal("classifier witness missing")
			}
			if _, err := RegisterEvent[catalogReplacement](registry); err != nil {
				t.Fatal(err)
			}
			kernel.endStream <- status.Error(codes.Unavailable, "replace generation")
			second := receive()
			for i := range first {
				if !proto.Equal(first[i], second[i]) {
					t.Fatalf("reconnect changed frozen request at stage %d", i)
				}
			}
			after, err := store.WaitForRegistration(ctx)
			if err != nil || after.Generation <= before.Generation {
				t.Fatal(after, err)
			}
			if callbacks.Load() != 6 || classifiers.Load() != classified {
				t.Fatal("reconnect reran preparation")
			}
		})
	}
}
