// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
	"google.golang.org/protobuf/proto"
)

type DeclaredEmail struct {
	Tenant string `json:"tenant" chronicle:"unique(name=\"email\",sequences=[\"event-log\"])"`
	Email  string `json:"email" chronicle:"unique(name=\"email\",message=\"first {PropertyValue}\",sequences=[\"outbox\",\"event-log\"])"`
}
type DeclaredEmailChanged struct {
	Tenant string `json:"tenant" chronicle:"unique(name=\"email\",message=\"later\",sequences=[\"event-log\"])"`
	Email  string `json:"email" chronicle:"unique(name=\"email\",sequences=[\"event-log\"])"`
}
type DeclaredRemoval struct{}
type DeclaredExpiry struct{}
type UnrestrictedEmail struct {
	Email string `chronicle:"unique(name=\"email\")"`
}
type DefaultUnique struct {
	Address string `json:"renamed" chronicle:"unique"`
}

func declareEvent[T any](t *testing.T, registry *Registry, options ...events.TypeOption) events.Type[T] {
	t.Helper()
	event, err := RegisterEvent[T](registry, options...)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func declarationClient(t *testing.T, registry *Registry, options ...ClientOption) *Client {
	t.Helper()
	client, err := NewClient(append([]ClientOption{WithRegistry(registry)}, options...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return client
}

func TestCompositeDeclarationImprovementMatchesExplicitBuilderGolden(t *testing.T) {
	registry := NewRegistry()
	first := declareEvent[DeclaredEmail](t, registry, events.WithUnique(events.Unique{Name: "lifecycle", Message: "opened", EventSequences: []events.SequenceID{events.EventLog}}))
	second := declareEvent[DeclaredEmailChanged](t, registry, events.WithUnique(events.Unique{Name: "lifecycle", Message: "changed", EventSequences: []events.SequenceID{events.Outbox}}))
	removed := declareEvent[DeclaredRemoval](t, registry, events.WithRemoveConstraints("email", "lifecycle"))
	expired := declareEvent[DeclaredExpiry](t, registry, events.WithRemoveConstraints("email", "lifecycle", "email"))
	client := declarationClient(t, registry)
	golden, err := constraints.UniqueValues("email").On(first.Descriptor(), "tenant", "email").On(second.Descriptor(), "tenant", "email").RemovedWith(removed.Descriptor(), expired.Descriptor()).ForEventSequences(events.EventLog, events.Outbox).WithMessage("first {PropertyValue}").Build()
	if err != nil {
		t.Fatal(err)
	}
	lifecycle, err := constraints.UniqueEventTypes(first.Descriptor(), second.Descriptor()).WithName("lifecycle").RemovedWith(removed.Descriptor(), expired.Descriptor()).ForEventSequences(events.EventLog, events.Outbox).Build()
	if err != nil {
		t.Fatal(err)
	}
	if len(client.constraints) != 2 {
		t.Fatalf("definitions: %d", len(client.constraints))
	}
	for _, expected := range []constraints.Definition{golden, lifecycle} {
		var got constraints.Definition
		for _, candidate := range client.constraints {
			if candidate.Name() == expected.Name() {
				got = candidate
			}
		}
		if !proto.Equal(constraintContract(got), constraintContract(expected)) {
			t.Fatalf("got %v, want %v", constraintContract(got), constraintContract(expected))
		}
		for _, tc := range []struct {
			id   events.TypeID
			want string
		}{{first.Ref().ID, "opened"}, {second.Ref().ID, "changed"}, {"unknown", "opened"}} {
			want := tc.want
			if got.Kind() == constraints.Unique {
				want = "first taken"
			}
			v := got.ResolveMessage(constraints.Violation{ConstraintName: got.Name(), EventTypeID: tc.id, Details: map[string]string{constraints.PropertyValue: "taken"}})
			if v.Message != want {
				t.Fatalf("message = %q, want %q", v.Message, want)
			}
		}
	}
}

func TestDeclarationUnrestrictedSequencesDefaultsAndFrozenStores(t *testing.T) {
	registry := NewRegistry()
	declareEvent[DeclaredEmail](t, registry)
	declareEvent[UnrestrictedEmail](t, registry)
	declareEvent[DefaultUnique](t, registry, events.WithID("persisted-name"), events.WithUnique(events.Unique{}))
	client := declarationClient(t, registry, WithRegistryForStore("empty", NewRegistry()), WithNamingPolicy(serialization.CamelCase))
	if len(client.constraints[0].EventSequences()) != 0 {
		t.Fatal("unrestricted must win")
	}
	if client.constraints[1].Name() != "DefaultUnique" || client.constraints[2].Name() != "Address" {
		t.Fatalf("type/field Go-name defaults lost: %+v", client.constraints)
	}
	if !reflect.DeepEqual(client.constraints[0].Fields()[1].Properties, []string{"email"}) {
		t.Fatal("policy path not taken from plan")
	}
	if !reflect.DeepEqual(client.constraints[2].Fields()[0].Properties, []string{"renamed"}) {
		t.Fatal("JSON override lost")
	}
	declareEvent[DeclaredRemoval](t, registry, events.WithRemoveConstraints("email"))
	if len(client.constraints[0].RemovalTypes()) != 0 {
		t.Fatal("registry mutation leaked")
	}
	other, err := client.selectedStoreSnapshot("empty")
	if err != nil || len(other.constraints) != 0 {
		t.Fatalf("store replacement: %v %v", other.constraints, err)
	}
}

func TestDeclaredConstraintsRequireExplicitComposition(t *testing.T) {
	registry := NewRegistry()
	event := declareEvent[DeclaredEmail](t, registry)
	definition := addConstraint(t, registry, constraints.UniqueValues("email").On(event.Descriptor(), "email"))
	if err := registry.AddConstraint(definition); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal("duplicate protection lost")
	}
	_, err := NewClient(WithRegistry(registry))
	var declaration *declarations.DeclarationError
	if !errors.As(err, &declaration) {
		t.Fatalf("conflict must be typed: %v", err)
	}
	registry = NewRegistry()
	declareEvent[DeclaredEmail](t, registry)
	removed := declareEvent[DeclaredRemoval](t, registry)
	calls := 0
	if err := registry.ConfigureDeclaredConstraint("email", func(builder *constraints.Builder) {
		calls++
		builder.IgnoreCasing().PerEventSourceType().RemovedWith(removed.Descriptor())
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.ConfigureDeclaredConstraint("email", func(*constraints.Builder) {}); err == nil {
		t.Fatal("duplicate composition accepted")
	}
	client := declarationClient(t, registry)
	if calls != 1 || !client.constraints[0].IgnoresCasing() || !client.constraints[0].Scope().PerEventSourceType || len(client.constraints[0].RemovalTypes()) != 1 {
		t.Fatal("composition lost")
	}
	if _, err := client.selectedStoreSnapshot("store"); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("composition repeated")
	}
}

type TwoSubjects struct {
	A string `chronicle:"subject"`
	B string `chronicle:"subject"`
}
type InvalidSubject struct {
	A []string `chronicle:"subject"`
}
type RepeatedUnique struct {
	A string `chronicle:"unique;unique"`
}
type NestedSubject struct {
	Child struct {
		A string `chronicle:"subject"`
	}
}

func TestEventDeclarationsFailAtomicallyAtNewClient(t *testing.T) {
	for name, configure := range map[string]func(*Registry){
		"subjects": func(r *Registry) { declareEvent[TwoSubjects](t, r) },
		"subjects with explicit resolver": func(r *Registry) {
			declareEvent[TwoSubjects](t, r, events.WithSubjectResolver(func(TwoSubjects) (events.Subject, bool) { return "explicit", true }))
		},
		"nonscalar":       func(r *Registry) { declareEvent[InvalidSubject](t, r) },
		"duplicate field": func(r *Registry) { declareEvent[RepeatedUnique](t, r) },
		"nested subject":  func(r *Registry) { declareEvent[NestedSubject](t, r) },
		"duplicate type": func(r *Registry) {
			declareEvent[DeclaredRemoval](t, r, events.WithUnique(events.Unique{}), events.WithUnique(events.Unique{}))
		},
		"blank sequence": func(r *Registry) {
			declareEvent[DeclaredRemoval](t, r, events.WithUnique(events.Unique{EventSequences: []events.SequenceID{" "}}))
		},
		"unknown remover": func(r *Registry) { declareEvent[DeclaredRemoval](t, r, events.WithRemoveConstraints("missing")) },
		"kind conflict":   func(r *Registry) { declareEvent[DeclaredEmail](t, r, events.WithUnique(events.Unique{Name: "email"})) },
		"foreign compensation": func(r *Registry) {
			foreign, err := events.Define[DeclaredEmail]()
			if err != nil {
				t.Fatal(err)
			}
			declareEvent[DeclaredRemoval](t, r, events.WithCompensationFor(foreign))
		},
	} {
		t.Run(name, func(t *testing.T) {
			registry := NewRegistry()
			configure(registry)
			for _, option := range []ClientOption{WithRegistry(registry), WithRegistryForStore("other", registry)} {
				client, err := NewClient(option)
				var declaration *declarations.DeclarationError
				if client != nil || !errors.As(err, &declaration) || !errors.Is(err, ErrInvalidConfiguration) {
					t.Fatalf("client=%v error=%v", client, err)
				}
			}
		})
	}
}
