// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package constraints_test

import (
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
)

type Contact struct {
	Email string `json:"emailAddress"`
	Phone struct {
		Country string `json:"country"`
		Number  string `json:"number"`
	} `json:"phone"`
	Optional *struct {
		Value string `json:"value"`
	} `json:"optional"`
	Ignored string `json:"-"`
}
type Removed struct{}
type Expired struct{}

func descriptor[T any](t *testing.T) events.Descriptor {
	t.Helper()
	event, err := events.Define[T]()
	if err != nil {
		t.Fatal(err)
	}
	return event.Descriptor()
}

func build(t *testing.T, builder *constraints.Builder) constraints.Definition {
	t.Helper()
	definition, err := builder.Build()
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

func TestConstraintDefaultsAndSnapshotOwnership(t *testing.T) {
	contact, removed, expired := descriptor[Contact](t), descriptor[Removed](t), descriptor[Expired](t)
	paths := []string{"phone.country", "phone.number", "optional.value"}
	builder := constraints.UniqueValues("ContactPhone").On(contact, paths...).RemovedWith(removed).RemovedWith(expired, removed)
	paths[0] = "changed"
	definition := build(t, builder)
	builder.WithName("changed").IgnoreCasing().PerEventStreamID().ForEventLog().RemovedWith(contact)
	if definition.Name() != "ContactPhone" || definition.Kind() != constraints.Unique || definition.IgnoresCasing() || definition.Scope() != (constraints.Scope{}) || len(definition.EventSequences()) != 0 {
		t.Fatalf("wrong defaults or mutable definition: %+v", definition)
	}
	want := []string{"phone.country", "phone.number", "optional.value"}
	fields := definition.Fields()
	fields[0].Properties[0] = "changed"
	types, removals := definition.EventTypes(), definition.RemovalTypes()
	types[0], removals[0] = events.Descriptor{}, events.Descriptor{}
	if !reflect.DeepEqual(definition.Fields()[0].Properties, want) || definition.EventTypes()[0].Ref() != contact.Ref() || len(definition.RemovalTypes()) != 2 || definition.RemovalTypes()[0].Ref() != removed.Ref() {
		t.Fatal("definition retained mutable caller data")
	}
	typeConstraint := build(t, constraints.UniqueEventTypes(contact, contact).RemovedWith(removed, removed))
	if typeConstraint.Name() != string(contact.Ref().ID) || typeConstraint.Kind() != constraints.UniqueEventType || len(typeConstraint.EventTypes()) != 1 || len(typeConstraint.RemovalTypes()) != 1 || len(typeConstraint.Fields()) != 0 {
		t.Fatalf("event-type defaults: %+v", typeConstraint)
	}
	scoped := build(t, builder.ForEventSequences("outbox", events.EventLog).PerEventSourceType().PerEventStreamType())
	sequences := scoped.EventSequences()
	sequences[0] = "changed"
	if !reflect.DeepEqual(scoped.EventSequences(), []events.SequenceID{events.EventLog, "outbox"}) || scoped.Scope() != (constraints.Scope{PerEventSourceType: true, PerEventStreamType: true, PerEventStreamID: true}) {
		t.Fatal("scope or additive sequence selection lost")
	}
}

func TestMalformedConstraintDefinitionsFailBeforeRegistration(t *testing.T) {
	contact := descriptor[Contact](t)
	for name, builder := range map[string]*constraints.Builder{
		"nil builder":             nil,
		"zero builder":            {},
		"missing name":            constraints.UniqueValues("").On(contact, "emailAddress"),
		"blank name":              constraints.UniqueValues("  ").On(contact, "emailAddress"),
		"no events":               constraints.UniqueValues("email"),
		"no unique types":         constraints.UniqueEventTypes(),
		"zero descriptor":         constraints.UniqueValues("email").On(events.Descriptor{}, "emailAddress"),
		"zero remover":            constraints.UniqueEventTypes(contact).RemovedWith(events.Descriptor{}),
		"duplicate event":         constraints.UniqueValues("email").On(contact, "emailAddress").On(contact, "phone.number"),
		"missing property":        constraints.UniqueValues("email").On(contact, "missing"),
		"Go name not JSON name":   constraints.UniqueValues("email").On(contact, "Email"),
		"ignored property":        constraints.UniqueValues("email").On(contact, "ignored"),
		"nested missing property": constraints.UniqueValues("email").On(contact, "phone.missing"),
		"empty property":          constraints.UniqueValues("email").On(contact, ""),
		"no properties":           constraints.UniqueValues("email").On(contact),
		"trailing dot":            constraints.UniqueValues("email").On(contact, "phone."),
		"nil provider":            constraints.UniqueEventTypes(contact).WithMessageProvider(nil),
		"empty sequence":          constraints.UniqueEventTypes(contact).ForEventSequences(""),
		"type property":           constraints.UniqueEventTypes(contact).On(contact, "emailAddress"),
		"type ignore casing":      constraints.UniqueEventTypes(contact).IgnoreCasing(),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := builder.Build(); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
				t.Fatalf("error = %v, want ErrInvalidConfiguration", err)
			}
		})
	}
}

func TestConstraintMessageTemplatesPreserveDetailsAndFallback(t *testing.T) {
	contact := descriptor[Contact](t)
	violation := constraints.Violation{ConstraintName: "email", Message: "kernel message", Details: map[string]string{constraints.PropertyName: "emailAddress", constraints.PropertyValue: "already@used.test"}}
	builder := constraints.UniqueValues("email").On(contact, "emailAddress")
	if got := build(t, builder).ResolveMessage(violation); got.Message != violation.Message {
		t.Fatal("missing template changed kernel message")
	}
	if got := build(t, builder.WithMessage("")).ResolveMessage(violation); got.Message != violation.Message {
		t.Fatal("empty template changed kernel message")
	}
	definition := build(t, builder.WithMessage("{PropertyName}: {PropertyValue}; {PropertyValue}; {Unknown}"))
	got := definition.ResolveMessage(violation)
	if got.Message != "emailAddress: already@used.test; already@used.test; {Unknown}" || !reflect.DeepEqual(got.Details, violation.Details) {
		t.Fatalf("resolved violation: %+v", got)
	}
	got.Details[constraints.PropertyValue] = "changed"
	if violation.Details[constraints.PropertyValue] != "already@used.test" {
		t.Fatal("details aliased")
	}
	calls := 0
	definition = build(t, builder.WithMessageProvider(func(input constraints.Violation) string {
		calls++
		input.Details[constraints.PropertyName] = "changed by callback"
		return "localized {PropertyName}"
	}))
	if got = definition.ResolveMessage(violation); got.Message != "localized emailAddress" || got.Details[constraints.PropertyName] != "emailAddress" || calls != 1 {
		t.Fatalf("callback lost details: %+v, calls %d", got, calls)
	}
	violation.ConstraintName = "built-in"
	if got = definition.ResolveMessage(violation); got.Message != violation.Message || calls != 1 {
		t.Fatal("provider ran for unrelated constraint")
	}
}

func TestMessageDetailValuesAreLiteral(t *testing.T) {
	definition := build(t, constraints.UniqueEventTypes(descriptor[Contact](t)).WithMessage("{A} {B}"))
	got := definition.ResolveMessage(constraints.Violation{ConstraintName: definition.Name(), Details: map[string]string{"A": "{B}", "B": "literal"}})
	if got.Message != "{B} literal" {
		t.Fatal(got.Message)
	}
}
