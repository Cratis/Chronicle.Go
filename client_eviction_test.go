// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/cratis/chronicle.go/contracts/eventstores"
	"github.com/cratis/chronicle.go/contracts/namespaces"
	projectioncontracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Keep the missing-API regression executable against the source baseline.
func evictStores(t *testing.T, c *Client) {
	t.Helper()
	evictor, ok := any(c).(interface{ EvictEventStores() error })
	if !ok {
		t.Fatal("Client.EvictEventStores is missing")
	}
	if err := evictor.EvictEventStores(); err != nil {
		t.Fatal(err)
	}
}

func TestEvictionCacheIdentityAndCoordinates(t *testing.T) {
	client, ctx := supervisionClient(t, &supervisedKernel{})
	for _, key := range []storeKey{{"store", DefaultNamespace}, {"store", "two"}, {"other", DefaultNamespace}} {
		old, err := client.EventStore(ctx, key.name, WithNamespace(key.namespace))
		if err != nil {
			t.Fatal(err)
		}
		same, err := client.EventStore(ctx, key.name, WithNamespace(key.namespace))
		if err != nil || same != old {
			t.Fatal("cache did not share", err)
		}
		evictStores(t, client)
		if len(client.storeSnapshot()) != 0 {
			t.Fatal("eviction retained cache membership")
		}
		fresh, err := client.EventStore(ctx, key.name, WithNamespace(key.namespace))
		if err != nil || fresh == old || fresh.Name() != key.name || fresh.Namespace() != key.namespace {
			t.Fatal("facade identity or coordinates", err)
		}
		if fresh.EventLog() != old.EventLog() || fresh.ReadModels() != old.ReadModels() || fresh.Compliance() != old.Compliance() {
			t.Fatal("eviction duplicated coordinate resources")
		}
	}
	evictStores(t, client)
	evictStores(t, client)
}

func TestEvictionDuringAcquisitionSharesRegistrationFlight(t *testing.T) {
	for _, cancelStarter := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "cancel_starter"}[cancelStarter], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				entered, release := make(chan struct{}), make(chan struct{})
				var registrations int
				client, _ := memoryLifecycleClient(t, func(ctx context.Context, method string) error {
					if method != namespaces.Namespaces_EnsureNamespace_FullMethodName {
						return nil
					}
					registrations++
					if registrations == 1 {
						close(entered)
						select {
						case <-release:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return nil
				}, singleRegistrationAttempt())
				callCtx, cancel := context.WithCancel(t.Context())
				defer cancel()
				type result struct {
					store *EventStore
					err   error
				}
				first, second := make(chan result, 1), make(chan result, 1)
				go func() { s, err := client.EventStore(callCtx, "store"); first <- result{s, err} }()
				<-entered
				old := client.storeSnapshot()[0]
				evictStores(t, client)
				go func() { s, err := client.EventStore(t.Context(), "store"); second <- result{s, err} }()
				synctest.Wait()
				fresh := client.storeSnapshot()[0]
				if old == fresh {
					t.Fatal("acquisition after eviction returned old facade")
				}
				if registrations != 1 {
					t.Fatal("joiner started a competing registration")
				}
				if cancelStarter {
					cancel()
				} else {
					close(release)
				}
				a, b := <-first, <-second
				if cancelStarter {
					if !errors.Is(a.err, context.Canceled) || registrations != 2 {
						t.Fatal("starter cancellation contract", a.err, registrations)
					}
				} else if a.err != nil || a.store != old || registrations != 1 {
					t.Fatal("old completion", a.err, registrations)
				}
				if b.err != nil || b.store != fresh || client.storeSnapshot()[0] != fresh {
					t.Fatal("stale completion changed new cache entry", b.err)
				}
			})
		})
	}
}

func TestEvictionDetachesLaterReadyAndReplaySnapshots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registrations := 0
		client, streams := memoryLifecycleClient(t, func(_ context.Context, method string) error {
			if method == namespaces.Namespaces_EnsureNamespace_FullMethodName {
				registrations++
			}
			return nil
		}, singleRegistrationAttempt())
		old, err := client.EventStore(t.Context(), "store")
		if err != nil {
			t.Fatal(err)
		}
		stream := <-streams
		evictStores(t, client)
		stream.end <- status.Error(codes.Unavailable, "reconnect")
		synctest.Wait()
		if err := client.Ready(t.Context()); err != nil {
			t.Fatal(err)
		}
		if registrations != 1 || len(client.storeSnapshot()) != 0 {
			t.Fatal("detached store automatically replayed")
		}
		if _, err := old.WaitForRegistration(t.Context()); err != nil {
			t.Fatal(err)
		}
		if registrations != 2 || len(client.storeSnapshot()) != 0 {
			t.Fatal("explicit retained use failed or recached")
		}
	})
}

func TestEvictionKeepsAlreadyCapturedReadyWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		entered, release := make(chan struct{}), make(chan struct{})
		calls := 0
		client, _ := memoryLifecycleClient(t, func(ctx context.Context, method string) error {
			if method == namespaces.Namespaces_EnsureNamespace_FullMethodName {
				calls++
				if calls == 1 {
					return status.Error(codes.InvalidArgument, "initial refusal")
				}
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}, singleRegistrationAttempt())
		if _, err := client.EventStore(t.Context(), "store"); err == nil {
			t.Fatal("fixture failure missing")
		}
		done := make(chan error, 1)
		go func() { done <- client.Ready(t.Context()) }()
		<-entered
		evictStores(t, client)
		if len(client.storeSnapshot()) != 0 {
			t.Fatal("cached membership retained")
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatal("eviction canceled admitted Ready", err)
		}
		if err := client.Ready(t.Context()); err != nil || calls != 2 {
			t.Fatal("later Ready used detached membership", err, calls)
		}
	})
}

func TestEvictionRetainsRootPublicationAndDecisionEvidence(t *testing.T) {
	registry, model := projectionRegistry(t)
	client, ctx := supervisionClient(t, runtimeKernel(), WithRegistry(registry))
	old, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	issuedReader := old.ReadModels()
	decisionReader := readmodels.DecisionsFor(issuedReader, model)
	catalog := old.definitionRoot().decisions
	target := (&clientTransport{client: client, store: old}).DecisionTarget(events.EventLog)
	token := decision.Issue(decision.Evidence{Target: target, Model: string(model.Identifier()), Key: "key", Types: []events.TypeRef{{ID: "ProjectionOpened", Generation: 1}}, Catalog: catalog, Epoch: catalog.ExpectedEpoch, Generation: 1, Check: func() error { return nil }})
	var guard *decision.Guard
	if err := decision.Enroll(token, target, old, func(g *decision.Guard) error { guard = g; return nil }); err != nil {
		t.Fatal(err)
	}
	evictStores(t, client)
	if err := guard.Validate(target, 1); err != nil {
		t.Fatal("eviction invalidated guard", err)
	}
	added, declaration := runtimeModel(t)
	result, err := old.RegisterProjection(ctx, declaration)
	if err != nil || !result.Published {
		t.Fatal(result, err)
	}
	if old.ReadModels() == issuedReader || len(issuedReader.Catalog().Descriptors()) != 1 || len(old.ReadModels().Catalog().Descriptors()) != 2 {
		t.Fatal("detached handle did not follow local root publication")
	}
	if err := guard.Validate(target, 1); !errors.Is(err, decision.ErrStale) {
		t.Fatal("publication did not stale enrollment", err)
	}
	if err := decision.Enroll(token, target, old, func(*decision.Guard) error { return nil }); !errors.Is(err, decision.ErrStale) {
		t.Fatal("publication did not stale issued token", err)
	}
	if _, err := decisionReader.GetDetached(ctx, "key"); !errors.Is(err, decision.ErrStale) {
		t.Fatal("old reader minted fresh evidence", err)
	}
	for _, ns := range []Namespace{DefaultNamespace, "new"} {
		fresh, err := client.EventStore(ctx, "store", WithNamespace(ns))
		if err != nil || fresh == old || fresh.definitions != old.definitions || len(fresh.ReadModels().Catalog().Descriptors()) != 2 {
			t.Fatal("lost cumulative root", err)
		}
		if _, err := readmodels.For(fresh.ReadModels(), added).Get(ctx, "key"); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEvictionPreservesDestructiveUnknownAndOrdinaryReady(t *testing.T) {
	registry, _ := projectionRegistry(t)
	kernel := runtimeKernel()
	var calls atomic.Int32
	kernel.projections.(*projectionKernel).register = func(context.Context, *projectioncontracts.RegisterRequest) error {
		if calls.Add(1) == 1 {
			return status.Error(codes.DeadlineExceeded, "unknown")
		}
		return nil
	}
	client, ctx := supervisionClient(t, kernel, WithRegistry(registry), singleRegistrationAttempt())
	if _, err := client.EventStore(ctx, "store"); err == nil {
		t.Fatal("missing fixture failure")
	}
	old := client.storeSnapshot()[0]
	evictStores(t, client)
	fresh, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal("ordinary ready poisoned", err)
	}
	if err := client.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	_, declaration := runtimeModel(t)
	for _, s := range []*EventStore{old, fresh} {
		result, err := s.RegisterProjection(ctx, declaration)
		if result.Published || !errors.Is(err, ErrDestructiveRegistrationUnknown) {
			t.Fatal("eviction forgot uncertain mutation", result, err)
		}
	}
}

type evictionRegistrationTransport struct {
	grpc.ClientConnInterface
	calls int
}

func (t *evictionRegistrationTransport) Invoke(ctx context.Context, method string, args, reply any, options ...grpc.CallOption) error {
	switch response := reply.(type) {
	case *eventstores.CommandResult:
		t.calls++
		response.IsAuthorized = true
		return nil
	case *namespaces.CommandResult:
		t.calls++
		response.IsAuthorized = true
		return nil
	}
	return t.ClientConnInterface.Invoke(ctx, method, args, reply, options...)
}

func TestEvictionIdentityReadyIsNotCacheMembership(t *testing.T) {
	kernel := identityHappyKernel()
	old, g := identityStore(t, identityConnection(t, kernel))
	manager := old.Identities()
	evictStores(t, old.client)
	if result, err := manager.Rename(t.Context(), "subject", "new"); err != nil || result.Disposition != IdentityRenameObserved {
		t.Fatal("eviction revoked ready manager", result, err)
	}
	// Deterministically install a new connection generation without namespace
	// registration. Unlike ordinary transport this manager must not produce it.
	g.cancel()
	ctx, cancel := context.WithCancel(old.client.life)
	defer cancel()
	raw := &evictionRegistrationTransport{ClientConnInterface: g.raw}
	fresh := &generation{client: old.client, number: g.number + 1, ctx: ctx, cancel: cancel, raw: raw}
	fresh.transport = &generationTransport{generation: fresh}
	old.client.mu.Lock()
	old.client.current = fresh
	old.client.mu.Unlock()
	result, err := manager.Rename(t.Context(), "subject", "new")
	var failure *IdentityRenameError
	if result.Disposition != IdentityRenameNotDispatched || !errors.As(err, &failure) || failure.Reason() != "registration_not_ready" {
		t.Fatal(result, err)
	}
	if kernel.reads != 2 || kernel.commands != 1 || raw.calls != 0 {
		t.Fatal("not-ready manager dispatched or joined readiness")
	}
	if err := old.client.Ready(t.Context()); err != nil || raw.calls != 0 {
		t.Fatal("empty cached Ready registered detached namespace", err)
	}
	if _, err := old.WaitForRegistration(t.Context()); err != nil || raw.calls != 2 {
		t.Fatal("explicit retained handle registration", err, raw.calls)
	}
	if len(old.client.storeSnapshot()) != 0 {
		t.Fatal("explicit readiness recached detached handle")
	}
	if result, err := manager.Rename(t.Context(), "subject", "new"); err != nil || result.Disposition != IdentityRenameObserved {
		t.Fatal("explicit namespace readiness did not restore manager", result, err)
	}
}

func TestEvictionRepeatedCoordinateRetainsOneResourceSet(t *testing.T) {
	client, ctx, server := openingReactorClient(t, nil)
	old, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	_ = receiveOpening(t, ctx, server.registered)
	sequence, err := old.EventSequence("custom")
	if err != nil {
		t.Fatal(err)
	}
	old.reactors.mu.Lock()
	run := old.reactors.runs["opening"]
	old.reactors.mu.Unlock()
	for range 3 {
		evictStores(t, client)
		fresh, err := client.EventStore(ctx, "store")
		if err != nil {
			t.Fatal(err)
		}
		fresh.reactors.mu.Lock()
		sameRun := fresh.reactors.runs["opening"] == run && len(fresh.reactors.runs) == 1
		fresh.reactors.mu.Unlock()
		if !sameRun || &fresh.readModelChanges != &old.readModelChanges || len(client.storeOwnerSnapshot()) != 1 {
			t.Fatal("eviction multiplied observer managers or change feeds")
		}
		got, err := fresh.EventSequence("custom")
		if err != nil || got != sequence {
			t.Fatal("eviction duplicated sequence subscriptions", err)
		}
	}
	if err := old.UnregisterReactor(ctx, "opening"); err != nil {
		t.Fatal(err)
	}
	fresh := client.storeSnapshot()[0]
	if _, err := fresh.WaitForRegistration(ctx); err != nil {
		t.Fatal(err)
	}
	fresh.reactors.mu.Lock()
	removed := fresh.reactors.removed["opening"]
	fresh.reactors.mu.Unlock()
	if !removed {
		t.Fatal("new facade forgot unregister tombstone")
	}
	select {
	case <-server.registered:
		t.Fatal("duplicate observer stream")
	default:
	}
}
