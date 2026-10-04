// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"net"
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
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	grpcmetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
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
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	firstID, secondID := metadata.CorrelationID{1}, metadata.CorrelationID{2}
	k := identityHappyKernel()
	k.pre = identityQuery(identityRow("first", "first-new"), identityRow("second", "second-new"))
	k.post = k.pre
	type wireCall struct{ method, correlation, subject string }
	var callsMu sync.Mutex
	var calls []wireCall
	commandEntered, releaseCommand := make(chan struct{}), make(chan struct{})
	var commands atomic.Int32
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.ForceServerCodec(identityServerCodec{k}), grpc.UnaryInterceptor(func(call context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := grpcmetadata.FromIncomingContext(call)
		ids := md.Get("x-correlation-id")
		if len(ids) != 1 || (ids[0] != firstID.String() && ids[0] != secondID.String()) {
			t.Error("wire correlation missing or unexpected")
			return nil, status.Error(codes.InvalidArgument, "invalid test correlation")
		}
		captured := wireCall{method: info.FullMethod, correlation: ids[0]}
		if request, ok := req.(*contracts.RenameIdentityRequest); ok {
			captured.subject = request.Subject
			if request.Subject == "first" && (ids[0] != firstID.String() || request.Name != "first-new") ||
				request.Subject == "second" && (ids[0] != secondID.String() || request.Name != "second-new") {
				t.Error("command correlation or name crossed invocations")
			}
		}
		callsMu.Lock()
		calls = append(calls, captured)
		callsMu.Unlock()
		if info.FullMethod == contracts.Identities_RenameIdentity_FullMethodName && commands.Add(1) == 1 {
			close(commandEntered)
			select {
			case <-releaseCommand:
			case <-call.Done():
				return nil, call.Err()
			}
		}
		return handler(call, req)
	}))
	contracts.RegisterIdentitiesServer(server, k)
	served := make(chan struct{})
	go func() { defer close(served); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close(); <-served })
	conn, err := grpc.NewClient("passthrough:///shared-identity", grpc.WithContextDialer(func(call context.Context, _ string) (net.Conn, error) { return listener.DialContext(call) }), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	s, g := identityStore(t, conn)
	manager, root, generationContext := s.Identities(), s.definitions.root, g.ctx
	var identityCalls, causationCalls, correlationCalls, tokens atomic.Int32
	s.client.config.outgoing.Identity = func(context.Context) (identities.Identity, bool, error) {
		identityCalls.Add(1)
		return identities.Identity{Subject: "audit-actor", Name: "audit-name"}, true, nil
	}
	s.client.config.outgoing.Causation = func(context.Context) ([]metadata.Causation, bool, error) {
		causationCalls.Add(1)
		return []metadata.Causation{{Type: "shared-test", Properties: map[string]string{"key": "frozen"}}}, true, nil
	}
	s.client.config.outgoing.Correlation = func(context.Context) (metadata.CorrelationID, bool, error) {
		correlationCalls.Add(1)
		return metadata.CorrelationID{}, false, nil
	}
	g.tokens = decisionTokenSource(func(context.Context) (Token, error) {
		tokens.Add(1)
		return Token{AccessToken: "synthetic-token"}, nil
	})
	firstRead, secondRead := make(chan struct{}), make(chan struct{})
	releaseFirst, releaseSecond := make(chan struct{}), make(chan struct{})
	var firstReads, secondReads atomic.Int32
	g.raw = identityRaw{ClientConnInterface: conn, before: func(call context.Context, _ string, _ any) {
		chain := metadata.CausationChain(call)
		if metadata.Identity(call).Name != "audit-name" || len(chain) != 1 || chain[0].Properties["key"] != "frozen" {
			t.Error("audit providers were not frozen for the invocation")
		}
	}, after: func(call context.Context, method string) {
		if method != contracts.Identities_GetIdentities_FullMethodName {
			return
		}
		var entered, release chan struct{}
		switch metadata.Correlation(call) {
		case firstID:
			if firstReads.Add(1) == 1 {
				entered, release = firstRead, releaseFirst
			}
		case secondID:
			if secondReads.Add(1) == 1 {
				entered, release = secondRead, releaseSecond
			}
		default:
			t.Error("outgoing correlation crossed invocations")
		}
		if entered != nil {
			close(entered)
			select {
			case <-release:
			case <-call.Done():
			}
		}
	}}
	var firstOnce, secondOnce, commandOnce sync.Once
	unblockFirst := func() { firstOnce.Do(func() { close(releaseFirst) }) }
	unblockSecond := func() { secondOnce.Do(func() { close(releaseSecond) }) }
	unblockCommand := func() { commandOnce.Do(func() { close(releaseCommand) }) }
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); unblockFirst(); unblockSecond(); unblockCommand(); workers.Wait() })
	type outcome struct {
		result IdentityRenameResult
		err    error
	}
	firstDone, secondDone := make(chan outcome, 1), make(chan outcome, 1)
	start := func(subject string, name identities.Name, id metadata.CorrelationID, done chan<- outcome) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			result, err := manager.Rename(ctx, subject, name, id)
			done <- outcome{result, err}
		}()
	}
	start("first", "first-new", firstID, firstDone)
	awaitSignal(t, ctx, firstRead)
	start("second", "second-new", secondID, secondDone)
	awaitSignal(t, ctx, secondRead)
	unblockFirst()
	awaitSignal(t, ctx, commandEntered)
	s.client.mu.Lock()
	flight, changed := s.definitions.flight, s.definitions.changed
	s.client.mu.Unlock()
	if !flight {
		t.Fatal("missing competing command flight")
	}
	unblockSecond()
	var refused outcome
	select {
	case refused = <-secondDone:
	case <-ctx.Done():
		t.Fatal("competing rename waited for the command flight", ctx.Err())
	}
	var failure *IdentityRenameError
	if refused.result.Disposition != IdentityRenameNotDispatched || refused.result.Acknowledged || refused.result.RequestCorrelationID != secondID ||
		!errors.As(refused.err, &failure) || failure.Phase() != "pre_read" || failure.Reason() != "registration_not_ready" || len(failure.Unwrap()) != 0 {
		t.Fatalf("competing result=%+v error=%v", refused.result, refused.err)
	}
	callsMu.Lock()
	admitted := len(calls)
	callsMu.Unlock()
	if admitted != 3 || tokens.Load() != 3 || identityCalls.Load() != 2 || causationCalls.Load() != 2 || correlationCalls.Load() != 0 {
		t.Fatal("refusal started extra RPCs or repeated audit providers")
	}
	unblockCommand()
	var first outcome
	select {
	case first = <-firstDone:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if first.err != nil || !first.result.Acknowledged || first.result.Disposition != IdentityRenameObserved || first.result.RequestCorrelationID != firstID {
		t.Fatalf("original result=%+v error=%v", first.result, first.err)
	}
	awaitSignal(t, ctx, changed)
	later, err := manager.Rename(ctx, "second", "second-new", secondID)
	if err != nil || !later.Acknowledged || later.Disposition != IdentityRenameObserved || later.RequestCorrelationID != secondID {
		t.Fatalf("later result=%+v error=%v", later, err)
	}
	g.work.Wait()
	s.client.work.Wait()
	s.client.mu.Lock()
	stable := s.client.current == g && s.definitions.root == root && g.ctx == generationContext && !s.definitions.flight && !s.definitions.destructiveUnknown
	s.client.mu.Unlock()
	if !stable || len(s.reactors.runs)+len(s.reducers.runs)+len(s.readModelReactors.runs) != 0 {
		t.Fatal("rename changed generation/root or started observers")
	}
	k.mu.Lock()
	reads, mutations := k.reads, k.commands
	k.mu.Unlock()
	if reads != 5 || mutations != 2 || tokens.Load() != 7 || identityCalls.Load() != 3 || causationCalls.Load() != 3 || correlationCalls.Load() != 0 {
		t.Fatal("shared-manager RPC/provider counts")
	}
	want := []wireCall{
		{contracts.Identities_GetIdentities_FullMethodName, firstID.String(), ""},
		{contracts.Identities_GetIdentities_FullMethodName, secondID.String(), ""},
		{contracts.Identities_RenameIdentity_FullMethodName, firstID.String(), "first"},
		{contracts.Identities_GetIdentities_FullMethodName, firstID.String(), ""},
		{contracts.Identities_GetIdentities_FullMethodName, secondID.String(), ""},
		{contracts.Identities_RenameIdentity_FullMethodName, secondID.String(), "second"},
		{contracts.Identities_GetIdentities_FullMethodName, secondID.String(), ""},
	}
	callsMu.Lock()
	defer callsMu.Unlock()
	if len(calls) != len(want) {
		t.Fatalf("wire calls=%v, want %v", calls, want)
	}
	for index := range want {
		if calls[index] != want[index] {
			t.Fatalf("wire call %d=%v, want %v", index, calls[index], want[index])
		}
	}
	if err := s.client.Close(); err != nil {
		t.Fatal(err)
	}
	if conn.GetState() == connectivity.Shutdown {
		t.Fatal("borrowed connection was closed")
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
