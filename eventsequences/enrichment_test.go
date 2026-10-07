// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/kernelcapability"
	"github.com/cratis/chronicle.go/internal/outgoing"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
)

type enrichmentConnection struct {
	grpc.ClientConnInterface
	config   outgoing.Config
	calls    int
	revision *sequences.ReviseRequest
	kernel   kernelcapability.Capabilities
}

func (c *enrichmentConnection) KernelCapabilities(context.Context) (kernelcapability.Capabilities, error) {
	return c.kernel, nil
}

func (c *enrichmentConnection) OutgoingConfiguration() outgoing.Config { return c.config }
func (c *enrichmentConnection) Invoke(_ context.Context, _ string, input, output any, _ ...grpc.CallOption) error {
	c.calls++
	if request, ok := input.(*sequences.ReviseRequest); ok {
		c.revision = request
		output.(*sequences.CommandResult).IsAuthorized = true
		return nil
	}
	return errors.New("unexpected RPC")
}

func TestProtectedRevisionRunsEveryProviderOnce(t *testing.T) {
	current, err := events.Define[revisionPII](events.WithID("protected"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := events.DefineGeneration[revisionPlain](current, 1)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(current.Descriptor(), previous.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	called := 0
	connection := &enrichmentConnection{kernel: *protectedReleaseKernel, config: outgoing.Config{
		Identity: func(context.Context) (identities.Identity, bool, error) {
			called++
			return identities.Identity{}, false, nil
		},
		Correlation: func(context.Context) (metadata.CorrelationID, bool, error) {
			called++
			return metadata.CorrelationID{}, false, nil
		},
		Causation: func(context.Context) ([]metadata.Causation, bool, error) { called++; return nil, false, nil },
		Enrichers: []events.EventEnricher{func(context.Context, events.TypeRef, *events.EventContent) error { called++; return nil }},
	}}
	sequence, err := eventsequences.New("store", "namespace", events.EventLog, catalog, connection)
	if err != nil {
		t.Fatal(err)
	}
	if err := sequence.Revise(t.Context(), 0, revisionPII{Name: "before"}); err != nil {
		t.Fatal(err)
	}
	// Protected revisions resolve identity, correlation, causation and enrichers
	// exactly as unprotected ones; the kernel protects the content (Chronicle#4525).
	if called != 4 || connection.calls != 1 || connection.revision.EventType.GetGeneration() != 2 {
		t.Fatal("protected revision did not run the outgoing pipeline once", called, connection.calls)
	}
}

func TestUnprotectedRevisionUsesOutgoingContentAndAuditOnce(t *testing.T) {
	descriptor := revisionDescriptor[revisionPlain](t)
	catalog, err := events.NewCatalog(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	called := 0
	connection := &enrichmentConnection{config: outgoing.Config{
		Identity: func(context.Context) (identities.Identity, bool, error) {
			return identities.Identity{Subject: "provider"}, true, nil
		},
		Enrichers: []events.EventEnricher{func(_ context.Context, _ events.TypeRef, c *events.EventContent) error {
			called++
			return c.Set("Name", "after")
		}},
	}}
	sequence, err := eventsequences.New("store", "namespace", events.EventLog, catalog, connection)
	if err != nil {
		t.Fatal(err)
	}
	if err := sequence.Revise(t.Context(), 0, revisionPlain{"before"}); err != nil {
		t.Fatal(err)
	}
	if called != 1 || connection.calls != 1 || connection.revision.Content != `{"Name":"after"}` || connection.revision.CausedBy.Subject != "provider" {
		t.Fatal(called, connection.calls, connection.revision)
	}
}

func TestInvalidLaterEntryPrecedesAllAuditCallbacks(t *testing.T) {
	descriptor := revisionDescriptor[revisionPlain](t)
	catalog, err := events.NewCatalog(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	called := 0
	connection := &enrichmentConnection{config: outgoing.Config{Identity: func(context.Context) (identities.Identity, bool, error) {
		called++
		return identities.Identity{}, false, nil
	}}}
	sequence, err := eventsequences.New("store", "namespace", events.EventLog, catalog, connection)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sequence.AppendBatch(t.Context(), []eventsequences.Entry{{Source: "A", Event: revisionPlain{}}, {Source: "B", Event: struct{ Missing string }{}}})
	if err == nil || called != 0 || connection.calls != 0 {
		t.Fatal(err, called, connection.calls)
	}
}
