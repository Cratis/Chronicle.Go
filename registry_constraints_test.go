// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/constraints"
	constraintcontracts "github.com/cratis/chronicle.go/contracts/events/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type EmailReserved struct {
	Tenant string `json:"tenant"`
	Email  string `json:"email"`
}
type EmailChanged struct {
	Tenant  string `json:"tenant"`
	Contact struct {
		Email string `json:"emailAddress"`
	} `json:"contact"`
}
type EmailReleased struct{}
type EmailExpired struct{}

func constraintEvent[T any](t *testing.T, registry *Registry, id events.TypeID) events.Descriptor {
	t.Helper()
	event, err := RegisterEvent[T](registry, events.WithID(id))
	if err != nil {
		t.Fatal(err)
	}
	return event.Descriptor()
}

func addConstraint(t *testing.T, registry *Registry, builder *constraints.Builder) constraints.Definition {
	t.Helper()
	definition, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AddConstraint(definition); err != nil {
		t.Fatal(err)
	}
	return definition
}

func constraintRegistry(t *testing.T) *Registry {
	t.Helper()
	registry := NewRegistry()
	event := constraintEvent[lifecycleEvent](t, registry, "lifecycleEvent")
	addConstraint(t, registry, constraints.UniqueEventTypes(event))
	return registry
}

func TestConstraintRegistrationMatchesCSharpGoldenRequest(t *testing.T) {
	// Derived from ConstraintBuilder, UniqueConstraintBuilder and ConstraintConverters
	// at Chronicle 2e31b0df. In particular, templates are NOT sent on the wire,
	// scope flags use "_scoped_", and the union is Value0 vs Value1 (not an enum alone).
	registry := NewRegistry()
	reserved := constraintEvent[EmailReserved](t, registry, "email-reserved")
	changed := constraintEvent[EmailChanged](t, registry, "email-changed")
	released := constraintEvent[EmailReleased](t, registry, "email-released")
	expired := constraintEvent[EmailExpired](t, registry, "email-expired")
	addConstraint(t, registry, constraints.UniqueValues("UniqueEmail").On(reserved, "tenant", "email").On(changed, "tenant", "contact.emailAddress").IgnoreCasing().WithMessage("Email {PropertyValue} already used").RemovedWith(released).RemovedWith(expired, released).PerEventSourceType().PerEventStreamType().PerEventStreamID().ForEventLog().ForEventSequences("outbox", events.EventLog))
	addConstraint(t, registry, constraints.UniqueEventTypes(reserved, changed, reserved).WithName("EmailLifecycle").RemovedWith(released, expired).ForEventLog().PerEventStreamID())
	addConstraint(t, registry, constraints.UniqueValues("ExactEmail").On(reserved, "email"))
	addConstraint(t, registry, constraints.UniqueEventTypes(reserved))
	data, err := os.ReadFile("constraints/testdata/csharp-registration.json")
	if err != nil {
		t.Fatal(err)
	}
	want := &constraintcontracts.RegisterConstraintsRequest{}
	if err = protojson.Unmarshal(data, want); err != nil {
		t.Fatal(err)
	}
	requests := make(chan *constraintcontracts.RegisterConstraintsRequest, 1)
	kernel := &supervisedKernel{registerConstraints: func(_ context.Context, request *constraintcontracts.RegisterConstraintsRequest) error {
		requests <- proto.Clone(request).(*constraintcontracts.RegisterConstraintsRequest)
		return nil
	}}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
	if _, err = client.EventStore(ctx, "golden-store"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-requests:
		if !proto.Equal(got, want) {
			t.Fatalf("constraint request\ngot: %s\nwant: %s", protojson.Format(got), protojson.Format(want))
		}
	case <-ctx.Done():
		t.Fatal("no constraint registration")
	}
}

func TestConstraintRegistryRejectsInvalidOrForeignDeclarations(t *testing.T) {
	registry := NewRegistry()
	registered := constraintEvent[EmailReserved](t, registry, "registered")
	definition := addConstraint(t, registry, constraints.UniqueEventTypes(registered))
	unregistered, err := events.Define[EmailReleased]()
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := constraints.UniqueEventTypes(unregistered.Descriptor()).Build()
	if err != nil {
		t.Fatal(err)
	}
	unknownRemover, err := constraints.UniqueEventTypes(registered).WithName("remover").RemovedWith(unregistered.Descriptor()).Build()
	if err != nil {
		t.Fatal(err)
	}
	wrongGeneration, err := events.Define[EmailReserved](events.WithID("registered"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	wrong, err := constraints.UniqueEventTypes(wrongGeneration.Descriptor()).WithName("wrong-generation").Build()
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []constraints.Definition{{}, definition, foreign, unknownRemover, wrong} {
		if err = registry.AddConstraint(value); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("error = %v, want invalid configuration", err)
		}
	}
	var absent *Registry
	if err = absent.AddConstraint(definition); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	_, definitions := snapshot(registry)
	if len(definitions) != 1 {
		t.Fatal("invalid registration partially modified registry")
	}
}

func TestConstraintSnapshotsAndStoreOverrides(t *testing.T) {
	registry := constraintRegistry(t)
	client, ctx := supervisionClient(t, &supervisedKernel{}, WithRegistry(registry), WithRegistryForStore("other", NewRegistry()))
	later := constraintEvent[EmailExpired](t, registry, "later")
	addConstraint(t, registry, constraints.UniqueEventTypes(later))
	first, err := client.EventStore(ctx, "first")
	if err != nil {
		t.Fatal(err)
	}
	other, err := client.EventStore(ctx, "other")
	if err != nil {
		t.Fatal(err)
	}
	definitions := first.Constraints()
	if len(definitions) != 1 || len(other.Constraints()) != 0 {
		t.Fatal("client snapshot or store override lost")
	}
	definitions[0] = constraints.Definition{}
	if first.Constraints()[0].Name() != "lifecycleEvent" {
		t.Fatal("store constraints leaked mutable slice")
	}
}

func TestConstraintRegistrationOrderReplayAndAppendBarrier(t *testing.T) {
	var eventPasses, constraintPasses atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	kernel := &supervisedKernel{
		register: func(context.Context) error { eventPasses.Add(1); return nil },
		registerConstraints: func(ctx context.Context, _ *constraintcontracts.RegisterConstraintsRequest) error {
			pass := constraintPasses.Add(1)
			if eventPasses.Load() != pass {
				return errors.New("constraints registered before event types or repeated per namespace")
			}
			if pass == 2 {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		},
	}
	client, ctx := supervisionClient(t, kernel, WithRegistry(constraintRegistry(t)))
	store, err := client.EventStore(ctx, "store", WithNamespace("one"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.EventStore(ctx, "store", WithNamespace("two")); err != nil {
		t.Fatal(err)
	}
	before, err := store.WaitForRegistration(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var stages []string
	for _, artifact := range before.Artifacts {
		stages = append(stages, artifact.Name)
	}
	if !reflect.DeepEqual(stages, []string{"store", "namespace", "event-types", "constraints"}) {
		t.Fatal(stages)
	}
	kernel.endStream <- status.Error(codes.Unavailable, "reconnect")
	awaitSignal(t, ctx, entered) // No caller drives replay.
	appendCtx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	_, err = store.EventLog().Append(appendCtx, "source", lifecycleEvent{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	if !errors.Is(err, context.DeadlineExceeded) || kernel.appends.Load() != 0 {
		t.Fatalf("append bypassed constraint barrier: %v", err)
	}
	close(release)
	if err = client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := store.WaitForRegistration(ctx)
	if err != nil || !after.IsSuccess() || after.Generation <= before.Generation || constraintPasses.Load() != 2 {
		t.Fatalf("replay outcome: %+v, %v", after, err)
	}
	if _, err = store.EventLog().Append(ctx, "source", lifecycleEvent{}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})); err != nil {
		t.Fatal(err)
	}
}

func TestConstraintRegistrationFailureIsReportedAndRetryable(t *testing.T) {
	var calls atomic.Int32
	kernel := &supervisedKernel{registerConstraints: func(context.Context, *constraintcontracts.RegisterConstraintsRequest) error {
		if calls.Add(1) == 1 {
			return status.Error(codes.InvalidArgument, "bad definition")
		}
		return nil
	}}
	client, ctx := supervisionClient(t, kernel, WithRegistry(constraintRegistry(t)))
	_, err := client.EventStore(ctx, "store")
	var failed *RegistrationError
	if !errors.As(err, &failed) || status.Code(err) != codes.InvalidArgument || failed.Outcome.IsSuccess() || failed.Outcome.RetryPending {
		t.Fatalf("registration failure: %v", err)
	}
	artifacts := failed.Outcome.Artifacts
	if len(artifacts) != 4 || artifacts[2].Failure != nil || artifacts[3].Name != "constraints" || artifacts[3].Failure == nil {
		t.Fatalf("lost acknowledged stages: %+v", artifacts)
	}
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := store.WaitForRegistration(ctx)
	if err != nil || !outcome.IsSuccess() || calls.Load() != 2 || kernel.registrations.Load() != 1 {
		t.Fatalf("constraint retry repeated acknowledged events: %+v %v", outcome, err)
	}
}
