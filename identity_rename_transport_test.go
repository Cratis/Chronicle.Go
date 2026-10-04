// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/identities"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/reactors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type identityObserverEvent struct{ Value string }
type identityObserver struct{}

func (*identityObserver) Handle(identityObserverEvent) {}

func TestIdentityRenameNamespaceAckDoesNotStartPendingObserver(t *testing.T) {
	registry := NewRegistry()
	if _, err := RegisterEvent[identityObserverEvent](registry); err != nil {
		t.Fatal(err)
	}
	if err := RegisterReactor[*identityObserver](registry, func() *identityObserver { return &identityObserver{} }, reactors.WithID("identity-observer")); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	s, g := identityStore(t, identityRaw{before: func(context.Context, string, any) { calls.Add(1) }}, WithRegistry(registry))
	if len(s.reactorPlans()) != 1 {
		t.Fatal("missing required observer plan")
	}
	var hooks atomic.Int32
	ready := make(chan struct{})
	s.reactors.runs = map[string]*observerRun{"identity-observer": {generation: g.number, ready: ready, openError: identityHostileError{&hooks}}}
	r, err := s.Identities().Rename(t.Context(), "subject", "new")
	if err == nil || r.Disposition != IdentityRenameNotDispatched || calls.Load() != 0 || hooks.Load() != 0 {
		t.Fatalf("result=%+v error=%v", r, err)
	}
	// Removed observers follow ordinary registration semantics, without restarting.
	s.reactors.removed = map[string]bool{"identity-observer": true}
	failure := (&identityInvocation{store: s, generation: g, root: s.definitions.root, readiness: s.identityReadiness(s.definitions.root), ctx: t.Context()}).check()
	if failure.reason != "" {
		t.Fatal("removed observer changed readiness semantics")
	}
}

func TestIdentityRenameCommandFlightReleasesAndNotifies(t *testing.T) {
	for _, outcome := range []string{"ack", "cancel", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			k := identityHappyKernel()
			conn := identityConnection(t, k)
			s, g := identityStore(t, conn)
			entered, release := make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			g.raw = identityRaw{ClientConnInterface: conn, fail: func(method string) error {
				if method != contracts.Identities_RenameIdentity_FullMethodName {
					return nil
				}
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				if outcome == "panic" {
					panic("private-payload")
				}
				return nil
			}}
			done := make(chan error, 1)
			go func() { _, err := s.Identities().Rename(ctx, "subject", "new"); done <- err }()
			<-entered
			s.client.mu.Lock()
			flight, changed := s.definitions.flight, s.definitions.changed
			s.client.mu.Unlock()
			if !flight {
				t.Fatal("missing command flight")
			}
			// Existing publication wait observes the same command flight.
			waitCtx, waitCancel := context.WithCancel(t.Context())
			wait := make(chan error, 1)
			go func() { wait <- s.waitDefinitionFlight(waitCtx) }()
			if outcome == "cancel" {
				cancel()
			} else {
				close(release)
			}
			err := <-done
			<-changed
			if waitErr := <-wait; waitErr != nil {
				t.Error(waitErr)
			}
			waitCancel()
			if outcome == "ack" && err != nil || outcome != "ack" && !errors.Is(err, ErrIdentityRenameUnknown) {
				t.Fatal(err)
			}
			g.work.Wait()
			s.client.work.Wait()
			s.client.mu.Lock()
			flight, unknown := s.definitions.flight, s.definitions.destructiveUnknown
			s.client.mu.Unlock()
			if flight || unknown {
				t.Fatal("command poisoned definitions")
			}
		})
	}
}

func TestIdentityRenameOccupiedFlightRefusesWithoutTokenOrRPC(t *testing.T) {
	var tokens, raw int
	s, g := identityStore(t, identityRaw{before: func(context.Context, string, any) { raw++ }})
	g.tokens = decisionTokenSource(func(context.Context) (Token, error) { tokens++; return Token{}, nil })
	s.definitions.flight = true
	r, err := s.Identities().Rename(t.Context(), "subject", "new")
	if err == nil || r.Disposition != IdentityRenameNotDispatched || tokens != 0 || raw != 0 {
		t.Fatalf("result=%+v error=%v", r, err)
	}
	s.definitions.flight = false
}

func TestIdentityRenameTokenAndInvalidatorDiscardFailures(t *testing.T) {
	for _, point := range []string{"token_error", "token_panic", "invalidator_panic"} {
		t.Run(point, func(t *testing.T) {
			var hooks, invalidated atomic.Int32
			k := identityHappyKernel()
			conn := identityConnection(t, k)
			s, g := identityStore(t, conn)
			g.tokens = identityInvalidatingTokens{token: func(context.Context) (Token, error) {
				if point == "token_panic" {
					panic(identityHostileError{&hooks})
				}
				if point == "token_error" {
					return Token{}, identityHostileError{&hooks}
				}
				return Token{AccessToken: "synthetic-token"}, nil
			}, invalidate: func() { invalidated.Add(1); panic(identityHostileError{&hooks}) }}
			if point == "invalidator_panic" {
				withDetails, err := status.New(codes.Unauthenticated, "private-payload").WithDetails(wrapperspb.String("private-payload"))
				if err != nil {
					t.Fatal(err)
				}
				g.raw = identityRaw{ClientConnInterface: conn, fail: func(method string) error {
					if method == contracts.Identities_RenameIdentity_FullMethodName {
						return withDetails.Err()
					}
					return nil
				}}
			}
			r, err := s.Identities().Rename(t.Context(), "subject", "new")
			if err == nil || hooks.Load() != 0 {
				t.Fatalf("result=%+v error=%v hooks=%d", r, err, hooks.Load())
			}
			if point == "invalidator_panic" && (r.Disposition != IdentityRenameUnknown || invalidated.Load() != 1) {
				t.Fatal("wrong invalidation/outcome")
			}
			g.work.Wait()
			s.client.work.Wait()
		})
	}
}

func TestIdentityRenameInvalidInputAndProviderFailureCounts(t *testing.T) {
	for _, input := range []string{"", " \t\n", string([]byte{0xff})} {
		for _, subject := range []bool{false, true} {
			var raw, providers int
			s, _ := identityStore(t, identityRaw{before: func(context.Context, string, any) { raw++ }})
			s.client.config.outgoing.Identity = func(context.Context) (identities.Identity, bool, error) {
				providers++
				return identities.Identity{}, false, nil
			}
			subj, name := "subject", identities.Name("new")
			if subject {
				subj = input
			} else {
				name = identities.Name(input)
			}
			_, err := s.Identities().Rename(t.Context(), subj, name)
			if !errors.Is(err, ErrInvalidConfiguration) || raw != 0 || providers != 0 {
				t.Fatal("input reached providers/transport")
			}
		}
	}
	s, _ := identityStore(t, identityRaw{})
	if _, err := s.Identities().Rename(t.Context(), "subject", "new", metadata.CorrelationID{}, metadata.CorrelationID{}); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	var absentContext context.Context // Deliberately exercise nil input validation.
	if _, err := s.Identities().Rename(absentContext, "subject", "new"); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatal(err)
	}
	var hooks atomic.Int32
	s.client.config.outgoing.Identity = func(context.Context) (identities.Identity, bool, error) {
		return identities.Identity{}, false, identityHostileError{&hooks}
	}
	if _, err := s.Identities().Rename(t.Context(), "subject", "new"); err == nil || hooks.Load() != 0 {
		t.Fatal("provider error retained/traversed")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Identities().Rename(ctx, "subject", "new"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestIdentityRenameConcurrentInvocationsKeepIndependentCorrelation(t *testing.T) {
	// Each invocation has its own server/namespace; shared audit providers remain
	// concurrent-safe and no operation state is stored in the manager/client.
	var providers atomic.Int32
	var workers sync.WaitGroup
	for range 2 {
		k := identityHappyKernel()
		s, _ := identityStore(t, identityConnection(t, k))
		s.client.config.outgoing.Correlation = func(context.Context) (metadata.CorrelationID, bool, error) {
			providers.Add(1)
			id, err := metadata.NewCorrelationID()
			return id, true, err
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			result, err := s.Identities().Rename(t.Context(), "subject", "new")
			if err != nil || result.Disposition != IdentityRenameObserved {
				t.Errorf("result=%+v error=%v", result, err)
			}
		}()
	}
	workers.Wait()
	if providers.Load() != 2 {
		t.Fatal("provider invocation count")
	}
}

func TestIdentityRenameTransportBoundsUseRealDecoder(t *testing.T) {
	k := identityHappyKernel()
	k.post.Data[0].Name = string(make([]byte, 2048))
	s, g := identityStore(t, identityConnection(t, k))
	g.transport.callOptions = []grpc.CallOption{grpc.MaxCallRecvMsgSize(256), grpc.MaxCallSendMsgSize(512)}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	r, err := s.Identities().Rename(ctx, "subject", "new")
	if !r.Acknowledged || !errors.Is(err, ErrIdentityRenameUnknown) || k.reads != 2 || k.commands != 1 {
		t.Fatalf("result=%+v error=%v", r, err)
	}
	// Latched cumulative-definition uncertainty is not a blanket identity block.
	k.post = identityQuery(identityRow("subject", "new"))
	k.reads = 0
	s.definitions.destructiveUnknown = true
	r, err = s.Identities().Rename(ctx, "subject", "new")
	if err != nil || r.Disposition != IdentityRenameObserved {
		t.Fatal(r, err)
	}
}
