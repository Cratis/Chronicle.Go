// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/eventtypes"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/declarations"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/serialization"
	"github.com/cratis/fundamentals.go/concepts"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type TaggedSubjectEvent struct {
	ID    string
	Owner *string `json:"owner" chronicle:"subject"`
}

type UUIDSubject struct {
	Owner uuid.UUID `chronicle:"subject"`
}
type NumericSubject struct {
	Owner int64 `chronicle:"subject"`
}
type ConceptSubject struct {
	Owner conceptfixtures.Name `chronicle:"subject"`
}
type SharedUUIDSubject struct {
	Owner concepts.UUID `chronicle:"subject"`
}
type SharedDateSubject struct {
	Owner concepts.DateOnly `chronicle:"subject"`
}
type ObjectConceptSubject struct {
	Owner conceptfixtures.Unsigned `chronicle:"subject"`
}
type MetadataOriginal struct{ Value string }
type MetadataCompensation struct{ Value string }

func TestTaggedSubjectPrecedenceAndStaging(t *testing.T) {
	owner := "tagged"
	for _, tc := range []struct {
		name     string
		options  []events.TypeOption
		owner    *string
		override events.Subject
		want     string
	}{
		{name: "tag", owner: &owner, want: "tagged"},
		{name: "nil tag", want: "source"},
		{name: "explicit resolver", owner: &owner, options: []events.TypeOption{events.WithSubjectResolver(func(TaggedSubjectEvent) (events.Subject, bool) { return "resolver", true })}, want: "resolver"},
		{name: "absent resolver bypasses tag", owner: &owner, options: []events.TypeOption{events.WithSubjectResolver(func(TaggedSubjectEvent) (events.Subject, bool) { return "", false })}, want: "source"},
		{name: "append override", owner: &owner, override: "override", options: []events.TypeOption{events.WithSubjectResolver(func(TaggedSubjectEvent) (events.Subject, bool) { t.Error("overridden resolver ran"); return "", false })}, want: "override"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := chronicle.NewRegistry()
			if _, err := chronicle.RegisterEvent[TaggedSubjectEvent](registry, tc.options...); err != nil {
				t.Fatal(err)
			}
			requests := make(chan *sequences.AppendRequest, 1)
			kernel := &fakeKernel{append: func(_ context.Context, request *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
				requests <- proto.Clone(request).(*sequences.AppendRequest)
				return success(request, 0), nil
			}}
			client, _ := testClient(t, kernel, chronicle.WithRegistry(registry))
			ctx := testContext(t)
			store, err := client.EventStore(ctx, "subject-store")
			if err != nil {
				t.Fatal(err)
			}
			options := []eventsequences.AppendOption{eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})}
			if tc.override != "" {
				options = append(options, eventsequences.WithSubject(tc.override))
			}
			_, err = store.EventLog().Append(ctx, "source", &TaggedSubjectEvent{ID: "not-a-subject-fallback", Owner: tc.owner}, options...)
			if err != nil {
				t.Fatal(err)
			}
			if got := (<-requests).Subject; got != tc.want {
				t.Fatalf("subject=%q want=%q", got, tc.want)
			}
		})
	}
	registry := chronicle.NewRegistry()
	var calls atomic.Int32
	if _, err := chronicle.RegisterEvent[TaggedSubjectEvent](registry, events.WithSubjectResolver(func(e TaggedSubjectEvent) (events.Subject, bool) { calls.Add(1); return events.Subject(*e.Owner), true })); err != nil {
		t.Fatal(err)
	}
	requests := make(chan *sequences.AppendManyForEventSourcesRequest, 1)
	kernel := &fakeKernel{appendBatch: func(request *sequences.AppendManyForEventSourcesRequest) *sequences.CommandResult_AppendManyResponse {
		requests <- request
		return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{IsSuccess: true, SequenceNumbers: []uint64{0}}}
	}}
	client, _ := testClient(t, kernel, chronicle.WithRegistry(registry))
	ctx := testContext(t)
	store, err := client.EventStore(ctx, "staging")
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := store.EventLog().PrepareBatch(ctx, []eventsequences.Entry{{Source: "source", Event: TaggedSubjectEvent{Owner: &owner}}})
	if err != nil {
		t.Fatal(err)
	}
	owner = "changed"
	if _, err := store.EventLog().AppendPreparedBatch(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	request := <-requests
	if calls.Load() != 1 || request.Events[0].Subject != "tagged" {
		t.Fatalf("subject re-resolved: %v calls=%d", request, calls.Load())
	}
}

func TestTaggedSubjectScalarConversions(t *testing.T) {
	registry := chronicle.NewRegistry()
	for _, err := range []error{
		registerDeclaration[UUIDSubject](registry), registerDeclaration[NumericSubject](registry), registerDeclaration[ConceptSubject](registry),
		registerDeclaration[SharedUUIDSubject](registry), registerDeclaration[SharedDateSubject](registry), registerDeclaration[ObjectConceptSubject](registry),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	}()
	catalog, _, err := client.Catalogs("store")
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")
	for _, tc := range []struct {
		value any
		want  string
	}{{UUIDSubject{Owner: id}, id.String()}, {NumericSubject{Owner: 42}, "42"}, {ConceptSubject{Owner: conceptfixtures.Name("Ada")}, "Ada"},
		{SharedUUIDSubject{Owner: concepts.UUID(id)}, id.String()}, {SharedDateSubject{}, "0001-01-01"}, {ObjectConceptSubject{Owner: conceptfixtures.Unsigned{Value: 42}}, "42"}} {
		descriptor, ok := catalog.Lookup(tc.value)
		if !ok {
			t.Fatal("missing descriptor")
		}
		if subject, ok := descriptor.ResolveSubject(tc.value); !ok || string(subject) != tc.want {
			t.Fatalf("subject %q %v want %q", subject, ok, tc.want)
		}
	}
}

func registerDeclaration[T any](registry *chronicle.Registry) error {
	_, err := chronicle.RegisterEvent[T](registry)
	return err
}

func TestDescriptorMetadataRegistrationContract(t *testing.T) {
	registry := chronicle.NewRegistry()
	original, err := chronicle.RegisterEvent[MetadataOriginal](registry, events.WithID("stable-original"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[MetadataCompensation](registry, events.WithTombstone(), events.WithCompensationFor(original), events.WithSourceStore("origin"), events.WithTags("static")); err != nil {
		t.Fatal(err)
	}
	requests := make(chan *eventtypes.RegisterEventTypesRequest, 1)
	kernel := &fakeKernel{register: func(request *eventtypes.RegisterEventTypesRequest) {
		requests <- proto.Clone(request).(*eventtypes.RegisterEventTypesRequest)
	}}
	client, _ := testClient(t, kernel, chronicle.WithRegistry(registry), chronicle.WithNamingPolicy(serialization.CamelCase))
	if _, err := client.EventStore(testContext(t), "store"); err != nil {
		t.Fatal(err)
	}
	request := <-requests
	compensation := request.Types[1]
	var schema map[string]any
	if err := json.Unmarshal([]byte(compensation.Schema), &schema); err != nil {
		t.Fatal(err)
	}
	if compensation.Type.Tombstone || compensation.EventStore != "origin" || schema["compensationFor"] != "stable-original" || compensation.Generations[0].Schema != compensation.Schema {
		t.Fatalf("metadata lost: %v", compensation)
	}
	if _, ok := schema["properties"].(map[string]any)["value"]; !ok {
		t.Fatal("naming policy lost")
	}
	catalog, _, err := client.Catalogs("store")
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.ValidateDeclarations(); err != nil {
		t.Fatal(err)
	}
	descriptor, _ := catalog.LookupID("MetadataCompensation")
	if ref, ok := descriptor.CompensationFor(); !ok || ref != original.Ref() || !descriptor.IsTombstone() || descriptor.Tags()[0] != "static" {
		t.Fatal("descriptor metadata lost")
	}
	// 19.29.4 has no registration labels field: labels stay static append tags.
	if compensation.ProtoReflect().Descriptor().Fields().ByName("Tags") != nil {
		t.Fatal("revisit registration label parity")
	}
}

type IgnoredDeclaration struct {
	Secret string `json:"-" chronicle:"subject"`
}
type PIIEvent struct {
	Secret events.SourceID `chronicle:"pii"`
}
type EncryptedEvent struct {
	Secret events.SourceID `chronicle:"encrypted(scope=subject)"`
}

func TestSecurityAndIgnoredEventTagsFailClosed(t *testing.T) {
	for _, err := range []error{registerDeclaration[IgnoredDeclaration](chronicle.NewRegistry()), registerDeclaration[PIIEvent](chronicle.NewRegistry()), registerDeclaration[EncryptedEvent](chronicle.NewRegistry())} {
		var declaration *declarations.DeclarationError
		if !errors.As(err, &declaration) {
			t.Fatalf("untyped error: %v", err)
		}
	}
}
