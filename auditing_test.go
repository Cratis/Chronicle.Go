// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/transactions"
)

func TestAuditingSelectsSnapshotsAndRunsDuplicateEnrichersInOrder(t *testing.T) {
	ctx := testContext(t)
	id, err := metadata.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	actor := identities.Identity{Subject: "provider", OnBehalfOf: &identities.Identity{Subject: "represented"}}
	causes := []metadata.Causation{{Occurred: time.Now(), Type: "command", Properties: map[string]string{"name": "original"}}}
	var trace []string
	provider := func(ctx context.Context, _ events.TypeRef, content *events.EventContent) error {
		if metadata.Identity(ctx).Subject != "provider" || metadata.Correlation(ctx) != id {
			t.Error("unselected audit context")
		}
		raw, _, err := content.Get("name")
		if err != nil {
			return err
		}
		trace = append(trace, string(raw))
		return content.Set("name", "enriched")
	}
	var captured *sequences.AppendRequest
	kernel := &fakeKernel{append: func(_ context.Context, r *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
		captured = r
		return success(r, 0), nil
	}}
	client, _ := testClient(t, kernel,
		chronicle.WithIdentityProvider(func(context.Context) (identities.Identity, bool, error) { return actor, true, nil }),
		chronicle.WithCorrelationProvider(func(context.Context) (metadata.CorrelationID, bool, error) {
			actor.OnBehalfOf.Subject = "changed"
			return id, true, nil
		}),
		chronicle.WithCausationProvider(func(context.Context) ([]metadata.Causation, bool, error) { return causes, true, nil }),
		chronicle.WithRootCausation(metadata.RootCausation{SoftwareVersion: "app"}),
		chronicle.WithEventEnrichers(provider), chronicle.WithEventEnrichers(provider))
	if len(trace) != 0 {
		t.Fatal("construction ran providers")
	}
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.EventLog().Append(ctx, "A", CustomerRegistered{Name: "original"}, eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	if err != nil {
		t.Fatal(err)
	}
	causes[0].Properties["name"] = "changed"
	if !reflect.DeepEqual(trace, []string{`"original"`, `"enriched"`}) || captured.Content != `{"name":"enriched"}` {
		t.Fatal(trace, captured)
	}
	if captured.CausedBy.OnBehalfOf.Subject != "represented" || captured.Causation[1].Properties["name"] != "original" {
		t.Fatal("audit alias leaked", captured)
	}
	if captured.Causation[0].Type != "Root" || captured.Causation[0].Properties["softwareVersion"] != "app" {
		t.Fatal(captured.Causation)
	}
	if _, ok := captured.Causation[0].Properties["machineName"]; ok {
		t.Fatal("implicit machine data")
	}
}

func TestExplicitZeroCorrelationBypassesProvider(t *testing.T) {
	var captured *sequences.AppendRequest
	client, _ := testClient(t, &fakeKernel{append: func(_ context.Context, r *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
		captured = r
		return success(r, 0), nil
	}}, chronicle.WithCorrelationProvider(func(context.Context) (metadata.CorrelationID, bool, error) { panic("bypassed") }))
	ctx := testContext(t)
	parent, err := metadata.NewCorrelationID()
	if err != nil {
		t.Fatal(err)
	}
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.EventLog().Append(metadata.WithCorrelation(ctx, parent), "A", CustomerRegistered{}, eventsequences.WithCorrelation(metadata.CorrelationID{}))
	if err != nil {
		t.Fatal(err)
	}
	if wire.Correlation(captured.CorrelationId) == parent || wire.Correlation(captured.CorrelationId) == (metadata.CorrelationID{}) {
		t.Fatal("explicit zero inherited parent")
	}
}

type hostileAuditError struct{ hooks *int }

func (e hostileAuditError) Error() string { *e.hooks++; return "secret" }
func (e hostileAuditError) Is(error) bool { *e.hooks++; return true }
func (e hostileAuditError) As(any) bool   { *e.hooks++; return true }
func (e hostileAuditError) Unwrap() error { *e.hooks++; return errors.New("secret") }

func TestEnrichmentFailuresArePayloadFreeAndPreventAllSDKIO(t *testing.T) {
	for _, mode := range []string{"error", "panic", "ignored setter"} {
		t.Run(mode, func(t *testing.T) {
			hooks, tails, notifications, calls := 0, 0, 0, 0
			kernel := &fakeKernel{tail: func(context.Context, *sequences.TailSequenceNumberRequest) (*sequences.QueryResult_EventSequenceTailResponse, error) {
				tails++
				return nil, errors.New("unexpected tail")
			}}
			client, _ := testClient(t, kernel, chronicle.WithEventEnrichers(func(_ context.Context, _ events.TypeRef, c *events.EventContent) error {
				calls++
				if calls != 3 {
					return c.Set("name", "enriched")
				}
				switch mode {
				case "error":
					return hostileAuditError{&hooks}
				case "panic":
					panic(hostileAuditError{&hooks})
				default:
					_ = c.Set("name", 123)
					return nil
				}
			}))
			ctx := testContext(t)
			store, err := client.EventStore(ctx, "customers")
			if err != nil {
				t.Fatal(err)
			}
			defer store.EventLog().OnAppend(func(eventsequences.AppendNotification) { notifications++ })()
			result, err := store.EventLog().AppendBatch(ctx, []eventsequences.Entry{{Source: "A", Event: CustomerRegistered{}}, {Source: "B", Event: CustomerRegistered{}}, {Source: "A", Event: CustomerRegistered{}}})
			var failure *events.PreparationError
			if !errors.As(err, &failure) || failure.EventIndex != 2 || failure.ProviderIndex != 0 || failure.Panicked != (mode == "panic") {
				t.Fatalf("failure=%#v", err)
			}
			if strings.Contains(fmt.Sprintf("%+v %#v", err, err), "secret") || hooks != 0 || tails != 0 || notifications != 0 || kernel.appendCalls.Load() != 0 || len(result.Positions) != 0 {
				t.Fatal("failure escaped boundary")
			}
		})
	}
}

func TestUnitOfWorkResolvesBeginThenStagesOnce(t *testing.T) {
	ctx := testContext(t)
	identityCalls, correlationCalls, enrichCalls := 0, 0, 0
	var request *sequences.AppendManyForEventSourcesRequest
	kernel := &fakeKernel{appendBatch: func(r *sequences.AppendManyForEventSourcesRequest) *sequences.CommandResult_AppendManyResponse {
		request = r
		return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{IsSuccess: true, CorrelationId: r.CorrelationId, SequenceNumbers: []uint64{0}}}
	}}
	client, _ := testClient(t, kernel, chronicle.WithIdentityProvider(func(context.Context) (identities.Identity, bool, error) {
		identityCalls++
		return identities.NotSet(), true, nil
	}), chronicle.WithCorrelationProvider(func(context.Context) (metadata.CorrelationID, bool, error) {
		correlationCalls++
		return metadata.CorrelationID{}, false, nil
	}), chronicle.WithEventEnrichers(func(_ context.Context, _ events.TypeRef, c *events.EventContent) error {
		enrichCalls++
		return c.Set("name", "staged")
	}))
	store, err := client.EventStore(ctx, "customers")
	if err != nil {
		t.Fatal(err)
	}
	unit, owner, err := transactions.Begin(ctx, store.EventLog())
	if err != nil {
		t.Fatal(err)
	}
	if enrichCalls != 0 || identityCalls != 1 || correlationCalls != 1 {
		t.Fatal("Begin callbacks", identityCalls, correlationCalls, enrichCalls)
	}
	if err := unit.Stage(ctx, []eventsequences.Entry{{Source: "A", Event: CustomerRegistered{Name: "original"}}}, eventsequences.LabeledScope{Label: "A", Scope: eventsequences.Scope{Expectation: eventsequences.NoCheck()}}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if enrichCalls != 1 || identityCalls != 2 || correlationCalls != 2 || request.Events[0].Content != `{"name":"staged"}` {
		t.Fatal("callbacks reran", identityCalls, correlationCalls, enrichCalls, request)
	}
}
