// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/identities"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/outgoing"
	"github.com/cratis/chronicle.go/internal/registration"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcmetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type identityRaw struct {
	grpc.ClientConnInterface
	before func(context.Context, string, any)
	after  func(context.Context, string)
	fail   func(string) error
	bypass func(string, any) bool
}

func (r identityRaw) Invoke(ctx context.Context, method string, req, reply any, options ...grpc.CallOption) error {
	if r.before != nil {
		r.before(ctx, method, req)
	}
	if r.fail != nil {
		if err := r.fail(method); err != nil {
			return err
		}
	}
	if r.bypass != nil && r.bypass(method, reply) {
		return nil
	}
	if r.ClientConnInterface == nil {
		return nil
	} // Deliberate decoder bypass.
	err := r.ClientConnInterface.Invoke(ctx, method, req, reply, options...)
	if r.after != nil {
		r.after(ctx, method)
	}
	return err
}

type identityHostileError struct{ hooks *atomic.Int32 }

func (e identityHostileError) Error() string { e.hooks.Add(1); return "private-payload" }
func (e identityHostileError) Is(error) bool { e.hooks.Add(1); return false }
func (e identityHostileError) As(any) bool   { e.hooks.Add(1); return false }
func (e identityHostileError) Unwrap() error { e.hooks.Add(1); return nil }
func (e identityHostileError) GRPCStatus() *status.Status {
	e.hooks.Add(1)
	return status.New(codes.Unauthenticated, "private-payload")
}

func TestIdentityRenameIncompleteBarrierNeverJoins(t *testing.T) {
	for _, pending := range []bool{true, false} {
		t.Run(fmt.Sprint(pending), func(t *testing.T) {
			var hooks, rpc atomic.Int32
			s, g := identityStore(t, identityRaw{before: func(context.Context, string, any) { rpc.Add(1) }})
			// Select a new uncached namespace barrier, leaving ordinary registration intact.
			s.namespace = "not-ready"
			key := registrationKey(s.name, s.namespace, s.definitions.root.revision)
			entered, release, joined := make(chan struct{}), make(chan struct{}), make(chan struct{})
			if pending {
				go func() {
					defer close(joined)
					g.registrations.For(key).Run(t.Context(), g.number, registration.Policy{MaxAttempts: 1, AttemptTimeout: time.Minute}, func(error) bool { return false }, func(context.Context) ([]ArtifactRegistration, error) { close(entered); <-release; return nil, nil })
				}()
				<-entered
			} else {
				identityMarkBarrier(g, key, identityHostileError{&hooks})
			}
			r, err := s.Identities().Rename(t.Context(), "subject", "new")
			if r.Disposition != IdentityRenameNotDispatched || err == nil || rpc.Load() != 0 || hooks.Load() != 0 {
				t.Fatalf("result=%+v error=%v rpc=%d hooks=%d", r, err, rpc.Load(), hooks.Load())
			}
			if pending {
				close(release)
				<-joined
				if !g.registrations.For(key).Snapshot().IsSuccess() {
					t.Fatal("ordinary producer changed")
				}
			}
		})
	}
}

func TestIdentityCompleteReadinessChecksObserversAndStages(t *testing.T) {
	s, g := identityStore(t, identityRaw{})
	p := s.identityReadiness(s.definitions.root)
	var hooks atomic.Int32
	for _, observers := range []*storeObservers{&s.reactors, &s.reducers, &s.readModelReactors} {
		for _, state := range []string{"missing", "pending", "failed", "wrong_generation", "ready", "removed"} {
			t.Run(fmt.Sprintf("%p/%s", observers, state), func(t *testing.T) {
				ready := make(chan struct{})
				run := &observerRun{generation: g.number, ready: ready}
				observers.runs = map[string]*observerRun{"required": run}
				observers.removed = nil
				switch state {
				case "missing":
					delete(observers.runs, "required")
				case "failed":
					close(ready)
					run.openError = identityHostileError{&hooks}
				case "wrong_generation":
					close(ready)
					run.generation++
				case "ready":
					close(ready)
				case "removed":
					observers.removed = map[string]bool{"required": true}
				}
				if got := identityObserversReady(observers, g.number, []string{"required"}); got != (state == "ready" || state == "removed") {
					t.Fatalf("readiness=%t", got)
				}
			})
		}
	}
	for _, key := range []string{"external-subscriptions", "seeding"} {
		candidate := p
		if key == "seeding" {
			candidate.seed = key
		} else {
			candidate.external = key
		}
		s.client.mu.Lock()
		failure := s.identityReadyLocked(t.Context(), g, s.definitions.root, candidate, false)
		s.client.mu.Unlock()
		if failure.reason != "registration_not_ready" {
			t.Fatal("namespace ack bypassed pending stage")
		}
		identityMarkBarrier(g, key, identityHostileError{&hooks})
		s.client.mu.Lock()
		failure = s.identityReadyLocked(t.Context(), g, s.definitions.root, candidate, false)
		s.client.mu.Unlock()
		if failure.reason != "registration_not_ready" || hooks.Load() != 0 {
			t.Fatal("failed readiness traversed error")
		}
		identityMarkBarrier(g, key, nil)
		s.client.mu.Lock()
		failure = s.identityReadyLocked(t.Context(), g, s.definitions.root, candidate, false)
		s.client.mu.Unlock()
		if failure.reason != "" {
			t.Fatal("completed stage refused")
		}
	}
}

func TestIdentityRenameGenerationRootAndCancellationRaces(t *testing.T) {
	for _, point := range []string{"token", "pre_read", "command", "post_read"} {
		for _, change := range []string{"root", "generation", "cancel"} {
			t.Run(point+"/"+change, func(t *testing.T) {
				k := identityHappyKernel()
				conn := identityConnection(t, k)
				s, g := identityStore(t, conn)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				mutate := func() {
					s.client.mu.Lock()
					defer s.client.mu.Unlock()
					switch change {
					case "root":
						original := s.definitions.root
						s.definitions.root = &definitionRoot{revision: original.revision + 1, snapshot: original.snapshot}
					case "generation":
						s.client.current = &generation{number: 2}
					case "cancel":
						cancel()
					}
				}
				var tokens int
				if point == "token" {
					g.tokens = decisionTokenSource(func(context.Context) (Token, error) {
						tokens++
						mutate()
						return Token{AccessToken: "synthetic-token"}, nil
					})
				}
				g.raw = identityRaw{ClientConnInterface: conn, after: func(_ context.Context, method string) {
					if point == "pre_read" && method == contracts.Identities_GetIdentities_FullMethodName && k.reads == 1 || point == "command" && method == contracts.Identities_RenameIdentity_FullMethodName || point == "post_read" && method == contracts.Identities_GetIdentities_FullMethodName && k.reads == 2 {
						mutate()
					}
				}}
				r, err := s.Identities().Rename(ctx, "subject", "new")
				ack := point == "command" || point == "post_read"
				if err == nil || r.Acknowledged != ack || (ack && !errors.Is(err, ErrIdentityRenameUnknown)) || (!ack && (r.Disposition != IdentityRenameNotDispatched || k.commands != 0)) {
					t.Fatalf("result=%+v error=%v reads=%d commands=%d", r, err, k.reads, k.commands)
				}
				if point == "token" && (tokens != 1 || k.reads != 0) {
					t.Fatal("token race dispatched")
				}
				if point == "command" && k.reads != 1 {
					t.Fatal("post-read started after changed ack")
				}
			})
		}
	}
}

func TestIdentityRenameHostileFailuresOpaqueAndLeasesReleased(t *testing.T) {
	var hooks atomic.Int32
	ownStatus := status.Error(codes.PermissionDenied, "private-payload")
	typedNil := reflect.Zero(reflect.TypeOf(ownStatus)).Interface().(error)
	for _, tc := range []struct {
		name  string
		err   error
		panic bool
	}{
		{"hostile", identityHostileError{&hooks}, false}, {"typed_nil", typedNil, false}, {"wrapped_status", fmt.Errorf("private-payload: %w", ownStatus), false}, {"status", ownStatus, false}, {"unsupported", status.Error(codes.Unimplemented, "private-payload"), false}, {"invalid_argument", status.Error(codes.InvalidArgument, "private-payload"), false}, {"before_marker", &faults.BeforeDispatch{Cause: identityHostileError{&hooks}}, false}, {"panic", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			k := identityHappyKernel()
			conn := identityConnection(t, k)
			s, g := identityStore(t, conn)
			var commands int
			g.raw = identityRaw{ClientConnInterface: conn, fail: func(method string) error {
				if method != contracts.Identities_RenameIdentity_FullMethodName {
					return nil
				}
				commands++
				s.client.mu.Lock()
				flight := s.definitions.flight
				s.client.mu.Unlock()
				if !flight {
					t.Error("command lacks flight")
				}
				if tc.panic {
					panic(identityHostileError{&hooks})
				}
				return tc.err
			}}
			r, err := s.Identities().Rename(t.Context(), "subject", "new")
			if r.Acknowledged || r.Disposition != IdentityRenameUnknown || !errors.Is(err, ErrIdentityRenameUnknown) || commands != 1 || hooks.Load() != 0 {
				t.Fatalf("result=%+v error=%v commands=%d hooks=%d", r, err, commands, hooks.Load())
			}
			if strings.Contains(fmt.Sprintf("%+v %#v", err, r), "private-payload") {
				t.Fatal("retained payload")
			}
			g.work.Wait()
			s.client.work.Wait()
			s.client.mu.Lock()
			flight, unknown := s.definitions.flight, s.definitions.destructiveUnknown
			s.client.mu.Unlock()
			if flight || unknown {
				t.Fatal("flight leaked or destructive latch set")
			}
		})
	}
}

func TestIdentityRenameDecoderBypassCannotAcknowledge(t *testing.T) {
	k := identityHappyKernel()
	conn := identityConnection(t, k)
	s, g := identityStore(t, conn)
	g.raw = identityRaw{ClientConnInterface: conn, bypass: func(method string, reply any) bool {
		if method != contracts.Identities_RenameIdentity_FullMethodName {
			return false
		}
		reply.(*contracts.CommandResult).IsAuthorized = true
		return true
	}}
	r, err := s.Identities().Rename(t.Context(), "subject", "new")
	if err == nil || r.Acknowledged || r.Disposition != IdentityRenameUnknown || !errors.Is(err, ErrProtocol) || k.commands != 0 || k.reads != 1 {
		t.Fatalf("result=%+v error=%v", r, err)
	}
}

type identityInvalidatingTokens struct {
	token      func(context.Context) (Token, error)
	invalidate func()
}

func (s identityInvalidatingTokens) Token(ctx context.Context) (Token, error) { return s.token(ctx) }
func (s identityInvalidatingTokens) Invalidate()                              { s.invalidate() }

func TestIdentityRenameCallbackCloseReentry(t *testing.T) {
	for _, callback := range []string{"identity", "correlation", "causation", "token", "invalidator"} {
		t.Run(callback, func(t *testing.T) {
			var called atomic.Int32
			k := identityHappyKernel()
			conn := identityConnection(t, k)
			s, g := identityStore(t, conn)
			closeClient := func() {
				called.Add(1)
				if err := s.client.Close(); err != nil {
					t.Error(err)
				}
			}
			switch callback {
			case "identity":
				s.client.config.outgoing.Identity = func(context.Context) (identities.Identity, bool, error) {
					closeClient()
					return identities.Identity{}, false, nil
				}
			case "correlation":
				s.client.config.outgoing.Correlation = func(context.Context) (metadata.CorrelationID, bool, error) {
					closeClient()
					return metadata.CorrelationID{}, false, nil
				}
			case "causation":
				s.client.config.outgoing.Causation = func(context.Context) ([]metadata.Causation, bool, error) { closeClient(); return nil, false, nil }
			case "token":
				g.tokens = decisionTokenSource(func(context.Context) (Token, error) { closeClient(); return Token{AccessToken: "synthetic-token"}, nil })
			case "invalidator":
				g.tokens = identityInvalidatingTokens{token: func(context.Context) (Token, error) { return Token{AccessToken: "synthetic-token"}, nil }, invalidate: closeClient}
				g.raw = identityRaw{ClientConnInterface: conn, fail: func(string) error { return status.Error(codes.Unauthenticated, "private-payload") }}
			}
			r, err := s.Identities().Rename(t.Context(), "subject", "new")
			if err == nil || called.Load() != 1 || k.commands != 0 || r.Acknowledged {
				t.Fatalf("result=%+v error=%v callbacks=%d", r, err, called.Load())
			}
		})
	}
}

func TestIdentityRenameFrozenAuditOnceAndExplicitZero(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			k := identityHappyKernel()
			k.pre.Data[0].Name = "new" // Still one command.
			responseID, _ := metadata.ParseCorrelationID("11223344-5566-7788-99aa-bbccddeeff00")
			k.command.CorrelationId = wire.Guid(responseID)
			s, g := identityStore(t, identityConnection(t, k))
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			deadline, _ := ctx.Deadline()
			selected, _ := metadata.ParseCorrelationID("00112233-4455-6677-8899-aabbccddeeff")
			identityCalls, correlationCalls, causeCalls, rpc := 0, 0, 0, 0
			props := map[string]string{"key": "original"}
			actor := identities.Identity{Subject: "audit-actor", Name: "audit-name"}
			s.client.config.outgoing = outgoing.Config{
				Identity: func(context.Context) (identities.Identity, bool, error) { identityCalls++; return actor, true, nil },
				Correlation: func(context.Context) (metadata.CorrelationID, bool, error) {
					correlationCalls++
					actor.Name = "changed"
					return selected, true, nil
				},
				Causation: func(context.Context) ([]metadata.Causation, bool, error) {
					causeCalls++
					return []metadata.Causation{{Type: "test", Properties: props}}, true, nil
				},
				Enrichers: []events.EventEnricher{func(context.Context, events.TypeRef, *events.EventContent) error {
					t.Error("enricher invoked")
					return nil
				}},
			}
			var frozen metadata.CorrelationID
			g.raw = identityRaw{ClientConnInterface: g.raw, before: func(call context.Context, _ string, _ any) {
				rpc++
				props["key"] = "changed"
				if rpc == 1 {
					frozen = metadata.Correlation(call)
				}
				gotDeadline, ok := call.Deadline()
				md, _ := grpcmetadata.FromOutgoingContext(call)
				if !ok || gotDeadline != deadline || metadata.Correlation(call) != frozen || md.Get("x-correlation-id")[0] != frozen.String() || metadata.Identity(call).Name != "audit-name" || metadata.CausationChain(call)[0].Properties["key"] != "original" {
					t.Error("audit not frozen")
				}
			}}
			var ids []metadata.CorrelationID
			if explicit {
				ids = []metadata.CorrelationID{{}}
			}
			r, err := s.Identities().Rename(ctx, "subject", "new", ids...)
			if err != nil || !r.Acknowledged || rpc != 3 || identityCalls != 1 || causeCalls != 1 || correlationCalls != map[bool]int{false: 1, true: 0}[explicit] || r.RequestCorrelationID != frozen || r.ResponseCorrelationID == nil || *r.ResponseCorrelationID != responseID {
				t.Fatalf("result=%+v error=%v counts=%d/%d/%d/%d", r, err, rpc, identityCalls, correlationCalls, causeCalls)
			}
			if explicit && frozen == selected {
				t.Fatal("zero consulted provider")
			}
		})
	}
}
