//go:build integration

// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package integration_test

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/transactions"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type DecisionAccountChanged struct {
	Name string `json:"name"`
}
type DecisionAccountRemoved struct{}
type DecisionUnrelated struct {
	Value string `json:"value"`
}
type DecisionAccount struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name" chronicle:"set(DecisionAccountChanged)"`
}

func decisionRegistry(t *testing.T) (*chronicle.Registry, readmodels.Model[DecisionAccount]) {
	t.Helper()
	registry := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[DecisionAccountChanged](registry); err != nil {
		t.Fatal(err)
	}
	removed, err := chronicle.RegisterEvent[DecisionAccountRemoved](registry)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = chronicle.RegisterEvent[DecisionUnrelated](registry); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[DecisionAccount](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err = registry.AddProjection(projections.ModelBound(model, projections.Passive(), projections.RemovedWith(removed))); err != nil {
		t.Fatal(err)
	}
	return registry, model
}
func decisionUnit(t *testing.T, ctx context.Context, store *chronicle.EventStore) (*transactions.UnitOfWork, *transactions.Owner) {
	t.Helper()
	unit, owner, err := transactions.Begin(ctx, store.EventLog())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Rollback(); err != nil {
			t.Error(err)
		}
	})
	return unit, owner
}
func appendDecisionEvent(t *testing.T, ctx context.Context, store *chronicle.EventStore, source events.SourceID, event any) {
	t.Helper()
	result, err := store.EventLog().Append(ctx, source, event)
	if err != nil {
		t.Fatal(err)
	}
	if err = result.Err(); err != nil {
		t.Fatal(err)
	}
}

func TestKernelDecisionCompetingWriteRejectsEntireMultisourceBatch(t *testing.T) {
	fixture := newKernelFixture(t)
	registry, model := decisionRegistry(t)
	first := fixture.client(registry)
	second := fixture.client(registry)
	store, err := first.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	competitor, err := second.EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	appendDecisionEvent(t, fixture.ctx, store, "source", DecisionAccountChanged{Name: "initial"})
	reader := readmodels.DecisionsFor(store.ReadModels(), model)
	if admission := reader.Admit(); !admission.IsAdmitted {
		t.Fatalf("admission: %+v", admission)
	}
	unit, owner := decisionUnit(t, fixture.ctx, store)
	read, err := reader.Get(transactions.WithUnitOfWork(fixture.ctx, unit), "source")
	if err != nil || !read.Instance.Exists || read.Instance.Value.Name != "initial" {
		t.Fatalf("read: %+v %v", read, err)
	}
	otherUnit, _ := decisionUnit(t, fixture.ctx, competitor)
	if err = otherUnit.Enroll(read.Token); !errors.Is(err, transactions.ErrDecisionTarget) {
		t.Fatalf("foreign client: %v", err)
	}
	if err = unit.Stage(fixture.ctx, []eventsequences.Entry{{Source: "source", Event: DecisionAccountChanged{Name: "stale"}}, {Source: "effect", Event: DecisionUnrelated{Value: "must-not-persist"}}, {Source: "source", Event: DecisionAccountChanged{Name: "also-stale"}}}); err != nil {
		t.Fatal(err)
	}
	appendDecisionEvent(t, fixture.ctx, competitor, "source", DecisionAccountChanged{Name: "winner"})
	result, err := owner.Commit(fixture.ctx)
	if err != nil || result.Disposition != eventsequences.Rejected || len(result.ConcurrencyViolations) != 1 || len(result.Positions) != 0 || unit.IsSuccess() {
		t.Fatalf("stale commit: %+v %v", result, err)
	}
	if len(fixture.read("source")) != 2 || len(fixture.read("effect")) != 0 || len(unit.GetDecisionConflicts()) != 1 {
		t.Fatal("atomic rejection/diagnostics lost")
	}
}

func TestKernelDecisionFirstEventAbsenceAndEventlessCompletion(t *testing.T) {
	for _, scenario := range []string{"empty-success", "empty-conflict", "absence-with-events", "unaffected-source", "unaffected-type", "removed-success", "removed-conflict"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := newKernelFixture(t)
			registry, model := decisionRegistry(t)
			store, err := fixture.client(registry).EventStore(fixture.ctx, fixture.storeName)
			if err != nil {
				t.Fatal(err)
			}
			competitor, err := fixture.client(registry).EventStore(fixture.ctx, fixture.storeName)
			if err != nil {
				t.Fatal(err)
			}
			removed := strings.HasPrefix(scenario, "removed")
			if removed {
				appendDecisionEvent(t, fixture.ctx, store, "source", DecisionAccountChanged{Name: "present"})
				appendDecisionEvent(t, fixture.ctx, store, "source", DecisionAccountRemoved{})
			}
			read, err := readmodels.DecisionsFor(store.ReadModels(), model).GetDetached(fixture.ctx, "source")
			if err != nil || read.Instance.Exists || read.Token.IsZero() {
				t.Fatalf("absent read: %+v %v", read, err)
			}
			if (read.Instance.LastHandled != nil) != removed {
				t.Fatalf("absence/removal progress: %+v", read.Instance)
			}
			unit, owner := decisionUnit(t, fixture.ctx, store)
			if err = unit.Enroll(read.Token); err != nil {
				t.Fatal(err)
			}
			conflict := scenario == "empty-conflict" || scenario == "absence-with-events" || scenario == "removed-conflict"
			if conflict {
				appendDecisionEvent(t, fixture.ctx, competitor, "source", DecisionAccountChanged{Name: "winner"})
			}
			if scenario == "unaffected-source" {
				appendDecisionEvent(t, fixture.ctx, competitor, "another", DecisionAccountChanged{Name: "unrelated"})
			}
			if scenario == "unaffected-type" {
				appendDecisionEvent(t, fixture.ctx, competitor, "source", DecisionUnrelated{Value: "unrelated"})
			}
			if scenario == "absence-with-events" {
				if err = unit.Stage(fixture.ctx, []eventsequences.Entry{{Source: "source", Event: DecisionAccountChanged{Name: "loser"}}, {Source: "effect", Event: DecisionUnrelated{Value: "loser"}}}); err != nil {
					t.Fatal(err)
				}
			}
			before, existsBefore, err := store.EventLog().Tail(fixture.ctx, eventsequences.TailFilter{})
			if err != nil {
				t.Fatal(err)
			}
			result, err := owner.Commit(fixture.ctx)
			if err != nil {
				t.Fatal(err)
			}
			if conflict {
				if result.Disposition != eventsequences.Rejected || len(result.ConcurrencyViolations) == 0 {
					t.Fatalf("absence conflict: %+v", result)
				}
			} else if !unit.IsSuccess() || !result.ConcurrencyCheckPerformed {
				t.Fatalf("eventless success: %+v", result)
			}
			if len(result.Positions) != 0 || len(fixture.read("effect")) != 0 {
				t.Fatalf("validation appended events: %+v", result)
			}
			after, existsAfter, err := store.EventLog().Tail(fixture.ctx, eventsequences.TailFilter{})
			if err != nil || before != after || existsBefore != existsAfter {
				t.Fatalf("validation changed log: before=%d/%v after=%d/%v err=%v", before, existsBefore, after, existsAfter, err)
			}
		})
	}
}

func TestKernelDecisionSimultaneousOwnersHaveOneWinner(t *testing.T) {
	fixture := newKernelFixture(t)
	registry, model := decisionRegistry(t)
	owners := make([]*transactions.Owner, 2)
	units := make([]*transactions.UnitOfWork, 2)
	for i := range owners {
		store, err := fixture.client(registry).EventStore(fixture.ctx, fixture.storeName)
		if err != nil {
			t.Fatal(err)
		}
		units[i], owners[i] = decisionUnit(t, fixture.ctx, store)
		if _, err = readmodels.DecisionsFor(store.ReadModels(), model).Get(transactions.WithUnitOfWork(fixture.ctx, units[i]), "source"); err != nil {
			t.Fatal(err)
		}
		if err = units[i].Stage(fixture.ctx, []eventsequences.Entry{{Source: "source", Event: DecisionAccountChanged{Name: "winner"}}}); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	var group sync.WaitGroup
	for _, owner := range owners {
		group.Go(func() {
			<-start
			if _, err := owner.Commit(fixture.ctx); err != nil {
				t.Error(err)
			}
		})
	}
	close(start)
	group.Wait()
	winners := 0
	for _, unit := range units {
		if unit.IsSuccess() {
			winners++
		} else if unit.State() != transactions.Rejected {
			t.Fatalf("unexpected state %v", unit.State())
		}
	}
	if winners != 1 || len(fixture.read("source")) != 1 {
		t.Fatalf("winners=%d", winners)
	}
}

func TestKernelDecisionCleanupFailureAfterRealFoldIssuesNoToken(t *testing.T) {
	fixture := newKernelFixture(t)
	registry, model := decisionRegistry(t)
	uri, err := chronicle.ParseConnectionString(fixture.endpoint)
	if err != nil {
		t.Fatal(err)
	}
	cause := errors.New("injected cleanup acknowledgement loss")
	var folded, cleaned, appended atomic.Int32
	conn, err := grpc.NewClient(uri.Addresses()[0].String(), grpc.WithDisableRetry(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true})), grpc.WithUnaryInterceptor(func(ctx context.Context, method string, args, reply any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, options ...grpc.CallOption) error {
		err := invoke(ctx, method, args, reply, conn, options...)
		if err != nil {
			return err
		}
		if strings.HasSuffix(method, "/GetInstanceByKey") {
			folded.Add(1)
		}
		if strings.Contains(method, "/Append") {
			appended.Add(1)
		}
		if strings.HasSuffix(method, "/DehydrateSession") {
			cleaned.Add(1)
			return cause
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := fixture.client(registry, chronicle.WithGRPCConnection(conn)).EventStore(fixture.ctx, fixture.storeName)
	if err != nil {
		t.Fatal(err)
	}
	appendDecisionEvent(t, fixture.ctx, store, "source", DecisionAccountChanged{Name: "present"})
	unit, owner := decisionUnit(t, fixture.ctx, store)
	read, err := readmodels.DecisionsFor(store.ReadModels(), model).Get(transactions.WithUnitOfWork(fixture.ctx, unit), "source")
	if !errors.Is(err, cause) || !read.Token.IsZero() || read.Instance.Exists || folded.Load() != 1 || cleaned.Load() != 1 {
		t.Fatalf("cleanup: %+v %v fold=%d cleanup=%d", read, err, folded.Load(), cleaned.Load())
	}
	if err = unit.Enroll(read.Token); !errors.Is(err, transactions.ErrInvalidDecision) {
		t.Fatal("failed read supplied evidence")
	}
	before := appended.Load()
	result, err := owner.Commit(fixture.ctx)
	if err != nil || len(result.Positions) != 0 || appended.Load() != before || len(fixture.read("source")) != 1 {
		t.Fatalf("failed read enrolled work: %+v %v", result, err)
	}
}
