// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/kernelcapability"
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

func revisionDescriptor[T any](t *testing.T, options ...events.TypeOption) events.Descriptor {
	t.Helper()
	definition, err := events.Define[T](options...)
	if err != nil {
		t.Fatal(err)
	}
	return definition.Descriptor()
}

// reviseCapture dispatches one revision and returns the request the kernel received.
func reviseCapture(t *testing.T, catalog *events.Catalog, replacement any) *sequences.ReviseRequest {
	t.Helper()
	var captured *sequences.ReviseRequest
	sequence, calls := kernelFixture(t, protectedReleaseKernel, map[string]rpcHandler{"Revise": func(_ context.Context, request any) (any, error) {
		captured = request.(*sequences.ReviseRequest)
		return &sequences.CommandResult{IsAuthorized: true}, nil
	}}, catalog, eventsequences.ConcurrencyPolicy{})
	defer sequence.OnAppend(func(eventsequences.AppendNotification) { t.Error("revision notified append") })()
	if err := sequence.Revise(testContext(t), 0, replacement); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || captured == nil {
		t.Fatalf("revision dispatched %d RPCs", calls.Load())
	}
	return captured
}

// Chronicle#4525 is fixed in 19.32.2: like append, revision sends plaintext and
// the kernel protects it with the original event's subject.
func TestProtectedRevisionDispatchesPlaintextForKernelProtection(t *testing.T) {
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
		{"field PII", revisionDescriptor[revisionPII](t), revisionPII{Name: "synthetic-private", Amount: 1}},
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
			request := reviseCapture(t, catalog, tc.replacement)
			ref := tc.descriptor.Ref()
			if !strings.Contains(request.Content, "synthetic-private") || request.EventType.GetId() != string(ref.ID) || request.EventType.GetGeneration() != uint32(ref.Generation) {
				t.Fatalf("revision request = %+v", request)
			}
		})
	}
}

func TestProtectedRevisionStillRejectsInvalidContentBeforeDispatch(t *testing.T) {
	catalog, err := events.NewCatalog(revisionDescriptor[revisionPII](t))
	if err != nil {
		t.Fatal(err)
	}
	sequence, calls := kernelFixture(t, protectedReleaseKernel, nil, catalog, eventsequences.ConcurrencyPolicy{})
	err = sequence.Revise(testContext(t), 0, revisionPII{Name: "synthetic-private", Amount: math.NaN()})
	var unknown *eventsequences.MutationOutcomeUnknownError
	if err == nil || errors.As(err, &unknown) || calls.Load() != 0 {
		t.Fatalf("invalid revision error=%v calls=%d", err, calls.Load())
	}
}

// Kernels before 19.32.2, and connections whose kernel version was not
// verified, persist revised content without protection (Chronicle#4525).
func TestProtectedRevisionRefusesWithoutProtectedReleaseKernel(t *testing.T) {
	catalog, err := events.NewCatalog(revisionDescriptor[revisionEncryptedGlobal](t))
	if err != nil {
		t.Fatal(err)
	}
	for name, kernel := range map[string]*kernelcapability.Capabilities{"unreported": nil, "19.32.1": {MixedAllReplay: true}} {
		sequence, calls := kernelFixture(t, kernel, nil, catalog, eventsequences.ConcurrencyPolicy{})
		err := sequence.Revise(testContext(t), 0, revisionEncryptedGlobal{"synthetic-private"})
		var unknown *eventsequences.MutationOutcomeUnknownError
		if !errors.Is(err, chronicle.ErrUnsupported) || errors.As(err, &unknown) || strings.Contains(err.Error(), "synthetic-private") || calls.Load() != 0 {
			t.Fatalf("%s: protected revision error=%v calls=%d", name, err, calls.Load())
		}
	}
	plain, err := events.NewCatalog(revisionDescriptor[revisionPlain](t))
	if err != nil {
		t.Fatal(err)
	}
	sequence, calls := kernelFixture(t, nil, map[string]rpcHandler{"Revise": func(context.Context, any) (any, error) {
		return &sequences.CommandResult{IsAuthorized: true}, nil
	}}, plain, eventsequences.ConcurrencyPolicy{})
	if err := sequence.Revise(testContext(t), 0, revisionPlain{"ordinary"}); err != nil || calls.Load() != 1 {
		t.Fatalf("unprotected revision needs no kernel capability: %v %d", err, calls.Load())
	}
}

// The kernel protects a revision with the replacement generation's schema only.
// An unclassified generation of an identity another generation classifies would
// store plaintext that erasure cannot remove, so it is refused on every kernel.
// C# dispatches it; this is a deliberate Go-specific safety difference.
func TestRevisionRefusesUnclassifiedGenerationOfProtectedIdentity(t *testing.T) {
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
	protectedPrevious, err := events.DefineGeneration[revisionClassified](plainCurrent, 1, events.WithProtection(compliance.For[revisionClassified](compliance.Classification{PII: true})))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name              string
		descriptors       []events.Descriptor
		plain, classified any
	}{
		{"unclassified historical generation", []events.Descriptor{plainPrevious.Descriptor(), protectedCurrent.Descriptor()}, revisionPlain{"synthetic-private"}, revisionPII{Name: "current"}},
		{"unclassified current generation", []events.Descriptor{plainCurrent.Descriptor(), protectedPrevious.Descriptor()}, revisionPlain{"synthetic-private"}, revisionClassified{"historical"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog, err := events.NewCatalog(tc.descriptors...)
			if err != nil {
				t.Fatal(err)
			}
			sequence, calls := kernelFixture(t, protectedReleaseKernel, map[string]rpcHandler{"Revise": func(context.Context, any) (any, error) {
				return &sequences.CommandResult{IsAuthorized: true}, nil
			}}, catalog, eventsequences.ConcurrencyPolicy{})
			err = sequence.Revise(testContext(t), 0, tc.plain)
			if !errors.Is(err, chronicle.ErrUnsupported) || strings.Contains(err.Error(), "synthetic-private") || calls.Load() != 0 {
				t.Fatalf("unclassified generation of a protected identity: %v calls=%d", err, calls.Load())
			}
			if err := sequence.Revise(testContext(t), 0, tc.classified); err != nil || calls.Load() != 1 {
				t.Fatalf("classified generation: %v calls=%d", err, calls.Load())
			}
		})
	}
}
