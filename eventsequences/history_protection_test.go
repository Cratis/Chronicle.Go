// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"math"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/serialization"
)

type revisionPII struct {
	Name   string `chronicle:"pii"`
	Amount float64
}
type revisionEncryptedSubject struct {
	Secret string `chronicle:"encrypted"`
}
type revisionEncryptedNamespace struct {
	Secret string `chronicle:"encrypted(scope=namespace)"`
}
type revisionEncryptedGlobal struct {
	Secret string `chronicle:"encrypted(scope=global)"`
}
type revisionClassified struct{ FullName string }
type revisionProtectedNode struct {
	Secret string `chronicle:"pii"`
	Next   *revisionProtectedNode
}
type revisionNested struct{ Node *revisionProtectedNode }
type revisionProtectedContainers struct {
	Nodes []revisionProtectedNode
}
type revisionProtectedMap struct {
	ByName map[string]string `chronicle:"encrypted(scope=global)"`
}
type revisionProtectedContainer struct {
	Names []string `chronicle:"encrypted(scope=namespace)"`
}
type revisionPlain struct{ Name string }
type revisionSerializationProbe struct {
	Value string
	calls *atomic.Int32
}

func (p revisionSerializationProbe) IsZero() bool {
	if p.calls != nil {
		p.calls.Add(1)
	}
	return p.Value == ""
}

type revisionProtectedWithSerializationProbe struct {
	Secret string                     `chronicle:"pii"`
	Probe  revisionSerializationProbe `json:",omitzero"`
}

func revisionDescriptor[T any](t *testing.T, options ...events.TypeOption) events.Descriptor {
	t.Helper()
	definition, err := events.Define[T](options...)
	if err != nil {
		t.Fatal(err)
	}
	return definition.Descriptor()
}

func TestProtectedRevisionRejectsBeforeSerializationAndDispatch(t *testing.T) {
	provided := revisionDescriptor[revisionClassified](t, events.WithProtection(compliance.Using(func(target compliance.Target) (compliance.Classification, error) {
		if target.Field == "FullName" {
			return compliance.Classification{Encrypted: true, Scope: compliance.Global}, nil
		}
		return compliance.Classification{}, nil
	})))
	explicit := revisionDescriptor[revisionClassified](t, events.WithProtection(compliance.Property("FullName", compliance.Classification{PII: true})))
	renamed, err := explicit.WithNamingPolicy(serialization.CamelCase)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name        string
		descriptor  events.Descriptor
		replacement any
	}{
		{"field PII before invalid marshal", revisionDescriptor[revisionPII](t), revisionPII{Name: "synthetic-private", Amount: math.NaN()}},
		{"field subject encryption", revisionDescriptor[revisionEncryptedSubject](t), revisionEncryptedSubject{"synthetic-private"}},
		{"field namespace encryption", revisionDescriptor[revisionEncryptedNamespace](t), revisionEncryptedNamespace{"synthetic-private"}},
		{"field global encryption", revisionDescriptor[revisionEncryptedGlobal](t), revisionEncryptedGlobal{"synthetic-private"}},
		{"explicit property", explicit, revisionClassified{"synthetic-private"}},
		{"explicit type", revisionDescriptor[revisionClassified](t, events.WithProtection(compliance.For[revisionClassified](compliance.Classification{PII: true}))), revisionClassified{"synthetic-private"}},
		{"provider", provided, revisionClassified{"synthetic-private"}},
		{"different naming policy", renamed, &revisionClassified{"synthetic-private"}},
		{"nested references", revisionDescriptor[revisionNested](t), revisionNested{&revisionProtectedNode{Secret: "synthetic-private"}}},
		{"nested collection items", revisionDescriptor[revisionProtectedContainers](t), revisionProtectedContainers{Nodes: []revisionProtectedNode{{Secret: "synthetic-private"}}}},
		{"protected map", revisionDescriptor[revisionProtectedMap](t), revisionProtectedMap{map[string]string{"name": "synthetic-private"}}},
		{"protected container", revisionDescriptor[revisionProtectedContainer](t), revisionProtectedContainer{[]string{"synthetic-private"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog, err := events.NewCatalog(tc.descriptor)
			if err != nil {
				t.Fatal(err)
			}
			sequence, calls := parityFixture(t, nil, catalog, eventsequences.ConcurrencyPolicy{})
			defer sequence.OnAppend(func(eventsequences.AppendNotification) { t.Error("rejection notified append") })()
			assertProtectedRevisionRejected(t, sequence.Revise(testContext(t), 0, tc.replacement))
			if calls.Load() != 0 {
				t.Fatalf("protected revision dispatched %d RPCs", calls.Load())
			}
		})
	}
}

func TestProtectedRevisionNeverExecutesSerializerCallbacks(t *testing.T) {
	var serializerCalls atomic.Int32
	replacement := revisionProtectedWithSerializationProbe{
		Secret: "synthetic-private",
		Probe:  revisionSerializationProbe{Value: "ordinary", calls: &serializerCalls},
	}
	descriptor := revisionDescriptor[revisionProtectedWithSerializationProbe](t)
	catalog, err := events.NewCatalog(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if got := serializerCalls.Load(); got != 0 {
		t.Fatalf("catalog registration executed serializer %d times", got)
	}
	serializerCalls.Store(0)
	sequence, calls := parityFixture(t, nil, catalog, eventsequences.ConcurrencyPolicy{})
	var notifications atomic.Int32
	defer sequence.OnAppend(func(eventsequences.AppendNotification) { notifications.Add(1) })()

	assertProtectedRevisionRejected(t, sequence.Revise(testContext(t), 0, replacement))
	if got := serializerCalls.Load(); got != 0 {
		t.Errorf("protected revision executed serializer %d times", got)
	}
	if got := calls.Load(); got != 0 {
		t.Errorf("protected revision dispatched %d RPCs", got)
	}
	if got := notifications.Load(); got != 0 {
		t.Errorf("protected revision emitted %d append notifications", got)
	}

	// Positive control: this valid shape executes the callback when serialized.
	if _, err := descriptor.Marshal(replacement); err != nil {
		t.Fatal(err)
	}
	if got := serializerCalls.Load(); got != 1 {
		t.Fatalf("serialization control executed callback %d times, want 1", got)
	}
}

func assertProtectedRevisionRejected(t *testing.T, err error) {
	t.Helper()
	var unknown *eventsequences.MutationOutcomeUnknownError
	if !errors.Is(err, chronicle.ErrUnsupported) || errors.As(err, &unknown) {
		t.Fatalf("protected revision error = %v", err)
	}
	if err.Error() != "chronicle: unsupported capability: protected revision is unsupported" {
		t.Fatalf("unstable or payload-bearing error = %q", err)
	}
}

func TestRevisionChecksAllGenerationsOfPersistedIdentity(t *testing.T) {
	protectedCurrent, err := events.Define[revisionPII](events.WithID("shared-revision-id"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	plainPrevious, err := events.DefineGeneration[revisionPlain](protectedCurrent, 1)
	if err != nil {
		t.Fatal(err)
	}
	plainCurrent, err := events.Define[revisionPlain](events.WithID("shared-revision-id"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	protectedPrevious, err := events.DefineGeneration[revisionPII](plainCurrent, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name        string
		descriptors []events.Descriptor
	}{
		{"unclassified historical replacement", []events.Descriptor{plainPrevious.Descriptor(), protectedCurrent.Descriptor()}},
		{"unclassified current replacement", []events.Descriptor{plainCurrent.Descriptor(), protectedPrevious.Descriptor()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog, err := events.NewCatalog(tc.descriptors...)
			if err != nil {
				t.Fatal(err)
			}
			sequence, calls := parityFixture(t, nil, catalog, eventsequences.ConcurrencyPolicy{})
			assertProtectedRevisionRejected(t, sequence.Revise(testContext(t), 0, revisionPlain{"synthetic-private"}))
			if calls.Load() != 0 {
				t.Fatalf("generation bypass dispatched %d RPCs", calls.Load())
			}
		})
	}
}

func TestRevisionProtectionUsesOnlySelectedCatalogAndMatchingID(t *testing.T) {
	plain := revisionDescriptor[revisionClassified](t)
	protected := revisionDescriptor[revisionClassified](t, events.WithProtection(compliance.For[revisionClassified](compliance.Classification{PII: true})))
	unrelated := revisionDescriptor[revisionPII](t)
	plainCatalog, err := events.NewCatalog(plain, unrelated)
	if err != nil {
		t.Fatal(err)
	}
	protectedCatalog, err := events.NewCatalog(protected)
	if err != nil {
		t.Fatal(err)
	}
	blocked, blockedCalls := parityFixture(t, nil, protectedCatalog, eventsequences.ConcurrencyPolicy{})
	assertProtectedRevisionRejected(t, blocked.Revise(testContext(t), 0, revisionClassified{"synthetic-private"}))
	allowed, allowedCalls := parityFixture(t, map[string]rpcHandler{"Revise": func(context.Context, any) (any, error) {
		return &sequences.CommandResult{IsAuthorized: true}, nil
	}}, plainCatalog, eventsequences.ConcurrencyPolicy{})
	if err := allowed.Revise(testContext(t), 0, revisionClassified{"ordinary"}); err != nil {
		t.Fatal(err)
	}
	if blockedCalls.Load() != 0 || allowedCalls.Load() != 1 {
		t.Fatalf("blocked calls=%d allowed calls=%d", blockedCalls.Load(), allowedCalls.Load())
	}
}
