//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
)

type AddressClaimed struct {
	Tenant string `json:"tenant"`
	Email  string `json:"email"`
}
type AddressChanged struct {
	Tenant  string `json:"tenant"`
	Address string `json:"address"`
}
type AddressReleased struct{}
type AddressExpired struct{}

type constraintEvents struct {
	claimed, changed, released, expired events.Descriptor
}

func registerConstraintEvent[T any](t *testing.T, registry *chronicle.Registry) events.Descriptor {
	t.Helper()
	event, err := chronicle.RegisterEvent[T](registry)
	if err != nil {
		t.Fatal(err)
	}
	return event.Descriptor()
}

func constraintsFixture(t *testing.T, configure func(constraintEvents) *constraints.Builder) (context.Context, *chronicle.Client, *chronicle.EventStore) {
	t.Helper()
	endpoint := os.Getenv("CHRONICLE_INTEGRATION_CONNECTION_STRING")
	if endpoint == "" {
		t.Fatal("set CHRONICLE_INTEGRATION_CONNECTION_STRING; integration never silently skips")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	t.Cleanup(cancel)
	registry := chronicle.NewRegistry()
	eventTypes := constraintEvents{
		claimed: registerConstraintEvent[AddressClaimed](t, registry), changed: registerConstraintEvent[AddressChanged](t, registry),
		released: registerConstraintEvent[AddressReleased](t, registry), expired: registerConstraintEvent[AddressExpired](t, registry),
	}
	definition, err := configure(eventTypes).Build()
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AddConstraint(definition); err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.Dial(ctx, chronicle.WithConnectionString(endpoint), chronicle.WithDevelopmentDefaults(), chronicle.WithRegistry(registry))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := client.EventStore(ctx, chronicle.StoreName("go-constraints-"+rand.Text()))
	if err != nil {
		t.Fatal(err)
	}
	return ctx, client, store
}

func appendConstraintEvent(t *testing.T, ctx context.Context, sequence *eventsequences.Sequence, source events.SourceID, value any, options ...eventsequences.AppendOption) eventsequences.AppendResult {
	t.Helper()
	options = append([]eventsequences.AppendOption{eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})}, options...)
	result, err := sequence.Append(ctx, source, value, options...)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func requireConstraintCommit(t *testing.T, result eventsequences.AppendResult) {
	t.Helper()
	if result.Disposition != eventsequences.Committed || result.Err() != nil || result.Position == nil {
		t.Fatalf("expected commitment: %+v", result)
	}
}

func requireConstraintRejection(t *testing.T, result eventsequences.AppendResult, name string) {
	t.Helper()
	var violation *eventsequences.ConstraintError
	if result.Disposition != eventsequences.Rejected || result.Position != nil || !errors.As(result.Err(), &violation) || len(violation.Violations) == 0 {
		t.Fatalf("expected constraint rejection: %+v", result)
	}
	for _, detail := range violation.Violations {
		if detail.ConstraintName != name || detail.Message == "" {
			t.Fatalf("lost violation identity/message: %+v", detail)
		}
	}
}

func requireSourceCount(t *testing.T, ctx context.Context, sequence *eventsequences.Sequence, source events.SourceID, count int) {
	t.Helper()
	history, err := sequence.ReadSource(ctx, source, eventsequences.SourceFilter{})
	if err != nil || len(history) != count {
		t.Fatalf("source %s: persisted %d events, want %d; %v", source, len(history), count, err)
	}
}

func TestKernelUniqueValuesAndOwnerRemoval(t *testing.T) {
	ctx, _, store := constraintsFixture(t, func(e constraintEvents) *constraints.Builder {
		return constraints.UniqueValues("UniqueEmail").On(e.claimed, "email").On(e.changed, "address").IgnoreCasing().RemovedWith(e.released).RemovedWith(e.expired)
	})
	sequence := store.EventLog()
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "owner", AddressClaimed{Email: "Ada@example.test"}))
	// The owner may reclaim the value; casing is compared by the kernel, not Go.
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "owner", AddressChanged{Address: "ADA@example.test"}))
	rejected := appendConstraintEvent(t, ctx, sequence, "competitor", AddressClaimed{Email: "ada@example.test"})
	requireConstraintRejection(t, rejected, "UniqueEmail")
	if len(rejected.ConstraintViolations) != 1 || rejected.ConstraintViolations[0].EventTypeID != "AddressClaimed" || rejected.ConstraintViolations[0].Type != constraints.Unique || rejected.ConstraintViolations[0].Details[constraints.PropertyName] != "email" || rejected.ConstraintViolations[0].Details[constraints.PropertyValue] != "ada@example.test" {
		t.Fatalf("property diagnostics: %+v", rejected.ConstraintViolations)
	}
	requireSourceCount(t, ctx, sequence, "competitor", 0)
	// A non-owner removal must not free another source's claim.
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "competitor", AddressReleased{}))
	requireConstraintRejection(t, appendConstraintEvent(t, ctx, sequence, "competitor", AddressChanged{Address: "ada@example.test"}), "UniqueEmail")
	requireSourceCount(t, ctx, sequence, "competitor", 1)
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "owner", AddressReleased{}))
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "competitor", AddressChanged{Address: "ada@example.test"}))
	// A second remover is additive, not a replacement for the first.
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "competitor", AddressExpired{}))
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "third", AddressClaimed{Email: "ADA@example.test"}))
	// No trimming is applied.
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "whitespace", AddressClaimed{Email: " ADA@example.test "}))
}

func TestKernelCompositeUniquenessAcrossEventTypes(t *testing.T) {
	ctx, _, store := constraintsFixture(t, func(e constraintEvents) *constraints.Builder {
		return constraints.UniqueValues("TenantEmail").On(e.claimed, "tenant", "email").On(e.changed, "tenant", "address").WithMessage("{PropertyName} conflicts: {PropertyValue}")
	})
	sequence := store.EventLog()
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "one", AddressClaimed{Tenant: "tenant-one", Email: "shared"}))
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "two", AddressChanged{Tenant: "tenant-two", Address: "shared"}))
	rejected := appendConstraintEvent(t, ctx, sequence, "three", AddressChanged{Tenant: "tenant-one", Address: "shared"})
	requireConstraintRejection(t, rejected, "TenantEmail")
	if len(rejected.ConstraintViolations) != 2 {
		t.Fatalf("composite must retain both property violations: %+v", rejected)
	}
	for _, violation := range rejected.ConstraintViolations {
		if violation.Message != violation.Details[constraints.PropertyName]+" conflicts: "+violation.Details[constraints.PropertyValue] {
			t.Fatalf("template not resolved: %+v", violation)
		}
	}
	requireSourceCount(t, ctx, sequence, "three", 0)
	// Default comparison is case sensitive.
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "four", AddressChanged{Tenant: "tenant-one", Address: "SHARED"}))
}

func TestKernelUniqueEventTypeLifecycleAndBatchCycles(t *testing.T) {
	ctx, _, store := constraintsFixture(t, func(e constraintEvents) *constraints.Builder {
		return constraints.UniqueEventTypes(e.claimed, e.changed).WithName("OpenAddress").RemovedWith(e.released).RemovedWith(e.expired)
	})
	sequence := store.EventLog()
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "one", AddressClaimed{Email: "one"}))
	requireConstraintRejection(t, appendConstraintEvent(t, ctx, sequence, "one", AddressChanged{Address: "different value"}), "OpenAddress")
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "two", AddressChanged{}))
	requireSourceCount(t, ctx, sequence, "one", 1)
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "one", AddressReleased{}))
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "one", AddressChanged{}))
	unchecked := eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()})
	// The kernel observes a removal before validating later events in a batch.
	cycle, err := sequence.AppendMany(ctx, "one", []any{AddressExpired{}, AddressClaimed{}}, unchecked)
	if err != nil || cycle.Err() != nil || len(cycle.Positions) != 2 {
		t.Fatalf("new lifecycle in batch: %+v, %v", cycle, err)
	}
	rejected, err := sequence.AppendMany(ctx, "fresh", []any{AddressClaimed{}, AddressChanged{}}, unchecked)
	if err != nil || rejected.Disposition != eventsequences.Rejected || len(rejected.ConstraintViolations) == 0 || rejected.ConstraintViolations[0].Type != constraints.UniqueEventType {
		t.Fatalf("mutually exclusive types in batch: %+v, %v", rejected, err)
	}
	requireSourceCount(t, ctx, sequence, "fresh", 0)
}

func TestKernelConstraintBatchRejectionsAreAtomic(t *testing.T) {
	ctx, _, store := constraintsFixture(t, func(e constraintEvents) *constraints.Builder {
		return constraints.UniqueValues("UniqueEmail").On(e.claimed, "email")
	})
	sequence := store.EventLog()
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, sequence, "owner", AddressClaimed{Email: "taken"}))
	many, err := sequence.AppendMany(ctx, "competitor", []any{AddressClaimed{Email: "free"}, AddressClaimed{Email: "taken"}}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	if err != nil || many.Disposition != eventsequences.Rejected || len(many.Positions) != 0 || len(many.ConstraintViolations) != 1 || many.ConstraintViolations[0].Details[constraints.PropertyValue] != "taken" {
		t.Fatalf("AppendMany rejection: %+v, %v", many, err)
	}
	requireSourceCount(t, ctx, sequence, "competitor", 0)
	batch, err := sequence.AppendBatch(ctx, []eventsequences.Entry{
		{Source: "left", Event: AddressClaimed{Email: "new"}},
		{Source: "right", Event: AddressClaimed{Email: "new"}},
	})
	if err != nil || batch.Disposition != eventsequences.Rejected || len(batch.Positions) != 0 || len(batch.ConstraintViolations) == 0 {
		t.Fatalf("AppendBatch conflict within batch: %+v, %v", batch, err)
	}
	requireSourceCount(t, ctx, sequence, "left", 0)
	requireSourceCount(t, ctx, sequence, "right", 0)
}

func TestKernelConstraintScopesSequencesAndNamespaces(t *testing.T) {
	ctx, client, store := constraintsFixture(t, func(e constraintEvents) *constraints.Builder {
		return constraints.UniqueValues("ScopedEmail").On(e.claimed, "email").PerEventSourceType().PerEventStreamType().PerEventStreamID().ForEventLog()
	})
	base := eventsequences.Route{SourceType: "person", StreamType: "account", StreamID: "one"}
	routes := []eventsequences.Route{base,
		{SourceType: "company", StreamType: "account", StreamID: "one"},
		{SourceType: "person", StreamType: "profile", StreamID: "one"},
		{SourceType: "person", StreamType: "account", StreamID: "two"},
	}
	for _, route := range routes {
		requireConstraintCommit(t, appendConstraintEvent(t, ctx, store.EventLog(), events.SourceID(rand.Text()), AddressClaimed{Email: "shared"}, eventsequences.WithRoute(route)))
	}
	requireConstraintRejection(t, appendConstraintEvent(t, ctx, store.EventLog(), "duplicate", AddressClaimed{Email: "shared"}, eventsequences.WithRoute(base)), "ScopedEmail")
	outbox, err := store.EventSequence("outbox")
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []events.SourceID{"one", "two"} {
		requireConstraintCommit(t, appendConstraintEvent(t, ctx, outbox, source, AddressClaimed{Email: "shared"}, eventsequences.WithRoute(base)))
	}
	other, err := client.EventStore(ctx, store.Name(), chronicle.WithNamespace("other"))
	if err != nil {
		t.Fatal(err)
	}
	requireConstraintCommit(t, appendConstraintEvent(t, ctx, other.EventLog(), "other-owner", AddressClaimed{Email: "shared"}, eventsequences.WithRoute(base)))
}
