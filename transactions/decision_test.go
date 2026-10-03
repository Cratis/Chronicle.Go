// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package transactions_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/transactions"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type decisionTransport struct {
	t           *testing.T
	catalog     *decision.Catalog
	target      decision.Target
	generation  atomic.Uint64
	unsupported atomic.Bool
	sequence    *eventsequences.Sequence
	handle      func(any) (*sequences.CommandResult_AppendManyResponse, error)
	requests    []proto.Message
}

func decisionSequence(t *testing.T) *decisionTransport {
	t.Helper()
	event, err := events.Define[changed](events.WithID("changed"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	f := &decisionTransport{t: t, catalog: decision.NewCatalog(nil, catalog)}
	f.target = decision.Target{Client: f, Store: "store", Namespace: "tenant", Sequence: string(events.EventLog)}
	f.generation.Store(1)
	f.sequence, err = eventsequences.New("store", "tenant", events.EventLog, catalog, f)
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *decisionTransport) DecisionCatalog() *decision.Catalog { return f.catalog }
func (f *decisionTransport) DecisionTarget(sequence events.SequenceID) decision.Target {
	target := f.target
	target.Sequence = string(sequence)
	return target
}
func (f *decisionTransport) AcquireDecision(ctx context.Context) (*decision.Lease, error) {
	if f.unsupported.Load() {
		return nil, decision.Unsupported()
	}
	generation := f.generation.Load()
	return &decision.Lease{Context: ctx, Conn: f, Generation: generation, Check: func() error {
		if f.generation.Load() != generation {
			return decision.ErrStale
		}
		return nil
	}, Release: func() {}}, nil
}
func (f *decisionTransport) Invoke(_ context.Context, _ string, request, reply any, _ ...grpc.CallOption) error {
	f.requests = append(f.requests, proto.Clone(request.(proto.Message)))
	response, err := f.handle(request)
	if err == nil {
		proto.Merge(reply.(proto.Message), response)
	}
	return err
}
func (*decisionTransport) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("unexpected stream")
}
func (f *decisionTransport) token(boundary events.SequenceNumber, types ...string) transactions.DecisionToken {
	refs := make([]events.TypeRef, len(types))
	for i, id := range types {
		refs[i] = events.TypeRef{ID: events.TypeID(id), Generation: 1}
	}
	generation := f.generation.Load()
	return decision.Issue(decision.Evidence{Target: f.target, Model: "model", Key: "source", Types: refs, Boundary: boundary, Catalog: f.catalog, Epoch: f.catalog.Epoch.Load(), Generation: generation, Check: func() error {
		if f.generation.Load() != generation {
			return decision.ErrStale
		}
		return nil
	}})
}

func TestDecisionEnrollmentValidatesTargetEpochOwnerAndState(t *testing.T) {
	for _, kind := range []string{"zero", "client", "store", "namespace", "sequence", "epoch", "generation", "owner", "rollback", "completed", "completing"} {
		t.Run(kind, func(t *testing.T) {
			f := decisionSequence(t)
			u, o := begin(t, t.Context(), f.sequence)
			token := f.token(5, "changed")
			want := transactions.ErrDecisionTarget
			switch kind {
			case "zero":
				token = transactions.DecisionToken{}
				want = transactions.ErrInvalidDecision
			case "client":
				f.target.Client = &struct{ ID int }{1}
			case "store":
				f.target.Store = "other"
			case "namespace":
				f.target.Namespace = "other"
			case "sequence":
				var err error
				other, err := eventsequences.New("store", "tenant", "other", f.catalog.Events, f)
				if err != nil {
					t.Fatal(err)
				}
				u, _ = begin(t, t.Context(), other)
			case "epoch":
				f.catalog.Epoch.Add(1)
				want = transactions.ErrStaleDecision
			case "generation":
				f.generation.Add(1)
				want = transactions.ErrStaleDecision
			case "owner", "rollback":
				other, owner := begin(t, t.Context(), f.sequence)
				if err := other.Enroll(token); err != nil {
					t.Fatal(err)
				}
				if kind == "rollback" {
					if err := owner.Rollback(); err != nil {
						t.Fatal(err)
					}
				}
				want = transactions.ErrDecisionOwner
			case "completed":
				if err := o.Rollback(); err != nil {
					t.Fatal(err)
				}
				want = transactions.ErrCompleted
			case "completing":
				if err := u.Enroll(token); err != nil {
					t.Fatal(err)
				}
				entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
				f.handle = func(any) (*sequences.CommandResult_AppendManyResponse, error) {
					close(entered)
					<-release
					return success(0), nil
				}
				go func() {
					defer close(done)
					if _, err := o.Commit(t.Context()); err != nil {
						t.Error(err)
					}
				}()
				<-entered
				if err := u.Enroll(token); !errors.Is(err, transactions.ErrCompleting) {
					t.Errorf("enrollment during commit: %v", err)
				}
				close(release)
				<-done
				return
			}
			if err := u.Enroll(token); !errors.Is(err, want) {
				t.Fatalf("enroll=%v want %v", err, want)
			}
			if len(u.GetEvents()) != 0 || len(f.requests) != 0 {
				t.Fatal("failed enrollment mutated or dispatched")
			}
		})
	}
}

func TestDecisionScopeCollisionsAndUncheckedMixturesAreAtomicBothOrders(t *testing.T) {
	for _, order := range []string{"explicit-first", "decision-first"} {
		for _, kind := range []string{"collision", "unchecked", "unresolved", "checked-independent"} {
			t.Run(order+"/"+kind, func(t *testing.T) {
				f := decisionSequence(t)
				u, _ := begin(t, t.Context(), f.sequence)
				token := f.token(5, "changed")
				label := "independent"
				expectation := eventsequences.Exact(4)
				switch kind {
				case "collision":
					label = "source"
				case "unchecked":
					expectation = eventsequences.NoCheck()
				case "unresolved":
					expectation = eventsequences.Resolve()
				}
				explicit := eventsequences.LabeledScope{Label: label, Scope: eventsequences.Scope{Expectation: expectation}}
				var err error
				if order == "explicit-first" {
					if err = u.Stage(t.Context(), nil, explicit); err != nil {
						t.Fatal(err)
					}
					err = u.Enroll(token)
				} else {
					if err = u.Enroll(token); err != nil {
						t.Fatal(err)
					}
					err = u.Stage(t.Context(), []eventsequences.Entry{{Source: "other", Event: changed{Value: "must-not-stage"}}}, explicit)
				}
				if kind == "checked-independent" {
					if err != nil {
						t.Fatal(err)
					}
					return
				}
				if !errors.Is(err, transactions.ErrDecisionScope) || len(u.GetEvents()) != 0 {
					t.Fatalf("scope failed open: %v %+v", err, u.GetEvents())
				}
				if order == "explicit-first" {
					other, _ := begin(t, t.Context(), f.sequence)
					if err = other.Enroll(token); err != nil {
						t.Fatalf("failed enrollment redeemed token: %v", err)
					}
				}
			})
		}
	}
}

func TestDecisionScopesMergeEarliestBoundaryAndUnionIDs(t *testing.T) {
	for _, absence := range []bool{false, true} {
		t.Run(map[bool]string{false: "positions", true: "absence"}[absence], func(t *testing.T) {
			f := decisionSequence(t)
			u, o := begin(t, t.Context(), f.sequence)
			first := events.SequenceNumber(9)
			if absence {
				first = events.Unavailable
			}
			for _, token := range []transactions.DecisionToken{f.token(first, "changed"), f.token(5, "removed", "changed"), f.token(11, "other")} {
				if err := u.Enroll(token); err != nil {
					t.Fatal(err)
				}
			}
			f.handle = func(request any) (*sequences.CommandResult_AppendManyResponse, error) {
				req, ok := request.(*sequences.AppendManyForEventSourcesRequest)
				if !ok {
					t.Fatalf("eventless used named/other RPC %T", request)
				}
				if len(req.Events) != 0 || len(req.ConcurrencyScopes) != 1 {
					t.Fatalf("request: %+v", req)
				}
				scope := req.ConcurrencyScopes[0].Scope
				want := uint64(5)
				if absence {
					want = uint64(events.Unavailable)
				}
				if scope.SequenceNumber != want || scope.ExpectsNoMatchingEvent != absence || !scope.EventSourceId {
					t.Fatalf("scope: %+v", scope)
				}
				var ids []string
				for _, event := range scope.EventTypes {
					ids = append(ids, event.Id)
				}
				if !slices.Equal(ids, []string{"changed", "other", "removed"}) {
					t.Fatalf("types: %v", ids)
				}
				return success(0), nil
			}
			result, err := o.Commit(t.Context())
			if err != nil || !u.IsSuccess() || len(result.Positions) != 0 || !result.ConcurrencyCheckPerformed {
				t.Fatalf("eventless: %+v %v", result, err)
			}
		})
	}
}

func TestDecisionCompletionPreservesSnapshotGlobalOrderAndAudit(t *testing.T) {
	f := decisionSequence(t)
	ctx := metadata.WithIdentity(t.Context(), identities.Identity{Subject: "actor"})
	ctx = metadata.WithCausation(ctx, metadata.Causation{Type: "outer", Occurred: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)})
	u, o := begin(t, ctx, f.sequence)
	value := &changed{Value: "original", Items: []string{"item"}}
	first := []eventsequences.Entry{{Source: "source", Event: value}, {Source: "effect", Event: changed{Value: "middle"}}}
	if err := u.Stage(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := u.Enroll(f.token(5, "changed")); err != nil {
		t.Fatal(err)
	}
	if err := u.Stage(ctx, []eventsequences.Entry{{Source: "source", Event: changed{Value: "last"}, NamedTags: []events.NamedTag{{Name: "named", Value: "tag"}}}}); err != nil {
		t.Fatal(err)
	}
	value.Value = "mutated"
	value.Items[0] = "mutated"
	f.handle = func(request any) (*sequences.CommandResult_AppendManyResponse, error) {
		req, ok := request.(*sequences.AppendManyForEventSourcesWithNamedTagsRequest)
		if !ok {
			t.Fatalf("lost named tags: %T", request)
		}
		if len(req.Events) != 3 || req.Events[0].EventSourceId != "source" || req.Events[1].EventSourceId != "effect" || req.Events[2].EventSourceId != "source" {
			t.Fatalf("order: %+v", req)
		}
		if req.Events[0].Content != `{"value":"original","items":["item"]}` || len(req.Events[2].NamedTags) != 1 || len(req.ConcurrencyScopes) != 1 {
			t.Fatalf("snapshot/defaults: %+v", req)
		}
		if req.CausedBy.GetSubject() != "actor" || wire.Correlation(req.CorrelationId) != u.CorrelationID() || len(req.Causation) != 0 {
			t.Fatalf("batch audit changed: %+v", req)
		}
		for _, event := range req.Events {
			if len(event.Causation) != 1 || event.Causation[0].Type != "outer" {
				t.Fatalf("stage audit lost: %+v", event)
			}
		}
		return success(3), nil
	}
	commitContext := metadata.WithIdentity(ctx, identities.Identity{Subject: "must-not-replace-actor"})
	if _, err := o.Commit(eventsequences.WithOrigin(commitContext, eventsequences.NewOrigin())); err != nil {
		t.Fatal(err)
	}
	if len(f.requests) != 1 {
		t.Fatal("split validation/append")
	}
}

func TestDecisionCommitRechecksCapabilitiesAndEpochBeforeDispatch(t *testing.T) {
	for _, kind := range []string{"capability", "epoch", "generation"} {
		t.Run(kind, func(t *testing.T) {
			f := decisionSequence(t)
			u, o := begin(t, t.Context(), f.sequence)
			if err := u.Enroll(f.token(5, "changed")); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "capability":
				f.unsupported.Store(true)
			case "epoch":
				f.catalog.Epoch.Add(1)
			case "generation":
				f.generation.Add(1)
			}
			result, err := o.Commit(t.Context())
			if err == nil || result.Disposition != eventsequences.Rejected || u.State() != transactions.Rejected || len(f.requests) != 0 {
				t.Fatalf("dispatch: %+v %v", result, err)
			}
			if _, err = o.Commit(t.Context()); !errors.Is(err, transactions.ErrCompleted) {
				t.Fatal("retry permitted")
			}
		})
	}
}

func TestDecisionCompletionPreservesUnsupportedUnknownAndRejection(t *testing.T) {
	for _, kind := range []string{"unchecked-reply", "unknown", "rejected", "old-eventless"} {
		t.Run(kind, func(t *testing.T) {
			f := decisionSequence(t)
			u, o := begin(t, t.Context(), f.sequence)
			if err := u.Enroll(f.token(5, "changed")); err != nil {
				t.Fatal(err)
			}
			f.handle = func(any) (*sequences.CommandResult_AppendManyResponse, error) {
				switch kind {
				case "unknown":
					return nil, errors.New("lost response")
				case "old-eventless":
					return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, ValidationResults: []*sequences.ValidationResult{{Message: "At least one event is required."}}}, nil
				case "rejected":
					return &sequences.CommandResult_AppendManyResponse{IsAuthorized: true, Response: &sequences.AppendManyResponse{ConcurrencyCheckPerformed: true, HasConcurrencyViolations: true, ConcurrencyViolations: []*sequences.ConcurrencyViolation{{EventSourceId: "source", ExpectedSequenceNumber: 5, ActualSequenceNumber: 6}}}}, nil
				default:
					response := success(0)
					response.Response.ConcurrencyCheckPerformed = false
					return response, nil
				}
			}
			result, err := o.Commit(t.Context())
			switch kind {
			case "unchecked-reply":
				if !errors.Is(err, faults.ErrUnsupported) || u.State() != transactions.Committed || u.IsSuccess() {
					t.Fatalf("unsupported confirmed: %+v %v", result, err)
				}
			case "unknown":
				if u.State() != transactions.OutcomeUnknown {
					t.Fatalf("unknown: %+v %v", result, err)
				}
			case "rejected":
				if err != nil || u.State() != transactions.Rejected || len(u.GetDecisionConflicts()) != 1 {
					t.Fatalf("rejected: %+v %v conflicts=%v", result, err, u.GetDecisionConflicts())
				}
			case "old-eventless":
				if !errors.Is(err, faults.ErrUnsupported) || u.State() != transactions.Rejected {
					t.Fatalf("old: %+v %v", result, err)
				}
			}
		})
	}
}

func TestDecisionTokenCopiesHaveOneConcurrentFirstOwner(t *testing.T) {
	f := decisionSequence(t)
	token := f.token(5, "changed")
	start := make(chan struct{})
	var group sync.WaitGroup
	var won atomic.Int32
	for range 16 {
		u, _ := begin(t, t.Context(), f.sequence)
		group.Go(func() {
			<-start
			err := u.Enroll(token)
			if err == nil {
				won.Add(1)
			} else if !errors.Is(err, transactions.ErrDecisionOwner) {
				t.Error(err)
			}
		})
	}
	close(start)
	group.Wait()
	if won.Load() != 1 {
		t.Fatalf("owners=%d", won.Load())
	}
}
