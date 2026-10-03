// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/conceptfixtures"
	"github.com/cratis/chronicle.go/serialization"
)

type StringSubjectEvent struct {
	Owner string `chronicle:"subject"`
}

func TestEmptyTaggedSubjectsFallBackToSourceAcrossAppendPaths(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
	}{
		{name: "zero string", value: StringSubjectEvent{}},
		{name: "empty concept", value: ConceptSubject{Owner: conceptfixtures.Name("")}},
		{name: "empty pointer", value: TaggedSubjectEvent{Owner: new(string)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := chronicle.NewRegistry()
			for _, err := range []error{registerDeclaration[StringSubjectEvent](registry), registerDeclaration[ConceptSubject](registry), registerDeclaration[TaggedSubjectEvent](registry)} {
				if err != nil {
					t.Fatal(err)
				}
			}
			subjects := make(chan string, 3)
			kernel := &fakeKernel{
				append: func(_ context.Context, request *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
					subjects <- request.Subject
					return success(request, 0), nil
				},
				appendMany: func(request *sequences.AppendManyRequest) *sequences.CommandResult_AppendManyResponse {
					subjects <- request.Events[0].Subject
					return batchSuccess(1)
				},
				appendBatch: func(request *sequences.AppendManyForEventSourcesRequest) *sequences.CommandResult_AppendManyResponse {
					subjects <- request.Events[0].Subject
					return batchSuccess(1)
				},
			}
			client, _ := testClient(t, kernel, chronicle.WithRegistry(registry))
			ctx := testContext(t)
			store, err := client.EventStore(ctx, "subjects")
			if err != nil {
				t.Fatal(err)
			}
			log := store.EventLog()
			unchecked := eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})
			if _, err := log.Append(ctx, "source", tc.value, unchecked); err != nil {
				t.Fatal(err)
			}
			if _, err := log.AppendMany(ctx, "source", []any{tc.value}, unchecked); err != nil {
				t.Fatal(err)
			}
			prepared, err := log.PrepareBatch(ctx, []eventsequences.Entry{{Source: "source", Event: tc.value}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := log.AppendPreparedBatch(ctx, prepared); err != nil {
				t.Fatal(err)
			}
			for _, path := range []string{"append", "append many", "staged batch"} {
				if got := <-subjects; got != "source" {
					t.Errorf("%s subject = %q, want source", path, got)
				}
			}
		})
	}
}

func TestExplicitEmptySubjectResolverStillFails(t *testing.T) {
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[StringSubjectEvent](registry, events.WithSubjectResolver(func(StringSubjectEvent) (events.Subject, bool) { return "", true })); err != nil {
		t.Fatal(err)
	}
	kernel := &fakeKernel{}
	client, _ := testClient(t, kernel, chronicle.WithRegistry(registry))
	ctx := testContext(t)
	store, err := client.EventStore(ctx, "subjects")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.EventLog().Append(ctx, "source", StringSubjectEvent{}); !errors.Is(err, chronicle.ErrInvalidConfiguration) {
		t.Fatalf("error = %v, want invalid configuration", err)
	}
	if kernel.appendCalls.Load() != 0 {
		t.Fatal("invalid subject dispatched")
	}
}

type UnsignedCompensation struct {
	Amount uint64
}

func TestCompensationSchemaPreservesUnsignedBounds(t *testing.T) {
	original, err := events.Define[MetadataOriginal](events.WithID("stable-original"))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := events.Define[UnsignedCompensation]()
	if err != nil {
		t.Fatal(err)
	}
	compensation, err := events.Define[UnsignedCompensation](events.WithCompensationFor(original))
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := compensation.Descriptor().WithNamingPolicy(serialization.PreservePropertyNames)
	if err != nil {
		t.Fatal(err)
	}
	var want, got map[string]json.RawMessage
	if err := json.Unmarshal([]byte(plain.Descriptor().Schema()), &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(compiled.Schema()), &got); err != nil {
		t.Fatal(err)
	}
	if string(got["compensationFor"]) != `"stable-original"` {
		t.Fatalf("compensationFor = %s", got["compensationFor"])
	}
	delete(got, "compensationFor")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("schema changed beyond compensationFor: got %s, want %s", compiled.Schema(), plain.Descriptor().Schema())
	}
}
