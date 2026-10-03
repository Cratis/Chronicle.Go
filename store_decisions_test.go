// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/contracts/clients"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/transactions"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type decisionProfileConn struct {
	grpc.ClientConnInterface
	version, protocol string
	protected         atomic.Int32
}

func (c *decisionProfileConn) Invoke(ctx context.Context, method string, args, reply any, options ...grpc.CallOption) error {
	if _, ok := args.(*sequences.AppendManyForEventSourcesRequest); ok {
		c.protected.Add(1)
	}
	if err := c.ClientConnInterface.Invoke(ctx, method, args, reply, options...); err != nil {
		return err
	}
	if response, ok := reply.(*clients.CompatibilityResponse); ok {
		response.ServerVersion, response.ServerProtocolVersion = c.version, c.protocol
	}
	return nil
}

func TestDecisionCapabilitiesRequireVerifiedVersionAndPreflight(t *testing.T) {
	for _, tc := range []struct {
		name, version, protocol string
		skip, supported         bool
	}{
		{"pinned", "19.29.4", "19.29.4", false, true},
		{"development", "19.29.4-development", "19.29.4", false, true},
		{"old-compatible", "19.28.0", "19.28.0", false, false},
		{"future-compatible", "20.0.0", "19.29.4", false, false},
		{"unknown", "", "19.29.4", false, false},
		{"wrong-protocol", "19.29.4", "19.28.0", false, false},
		{"skip", "19.29.4", "19.29.4", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kernel := &supervisedKernel{}
			var options []ClientOption
			if tc.skip {
				options = append(options, WithSkipCompatibilityCheck())
			}
			client, ctx := supervisionClient(t, kernel, options...)
			raw := &decisionProfileConn{ClientConnInterface: client.config.borrowed, version: tc.version, protocol: tc.protocol}
			client.config.borrowed = raw // Before connection establishment/publication.
			store, err := client.EventStore(ctx, "store")
			if err != nil {
				t.Fatal(err)
			}
			transport := &clientTransport{client: client, store: store}
			lease, err := transport.AcquireDecision(ctx)
			if tc.supported {
				if err != nil {
					t.Fatal(err)
				}
				if lease.Generation == 0 || lease.Check() != nil || lease.Conn == transport {
					t.Fatal("lease is not generation-bound")
				}
				lease.Release()
				return
			}
			if !errors.Is(err, ErrUnsupported) || lease != nil {
				t.Fatalf("capability: %v", err)
			}
			// Even internal synthetic evidence cannot let a protected owner dispatch
			// to an uncharacterized peer. Public callers cannot create this token.
			catalog := transport.DecisionCatalog()
			token := decision.Issue(decision.Evidence{Target: transport.DecisionTarget(events.EventLog), Model: "model", Key: "source", Types: []events.TypeRef{{ID: "changed", Generation: 1}}, Boundary: 5, Catalog: catalog, Epoch: catalog.Epoch.Load(), Generation: 1, Check: func() error { return nil }})
			u, o, err := transactions.Begin(ctx, store.EventLog())
			if err != nil {
				t.Fatal(err)
			}
			if err = u.Enroll(token); err != nil {
				t.Fatal(err)
			}
			if _, err = o.Commit(ctx); !errors.Is(err, ErrUnsupported) || raw.protected.Load() != 0 {
				t.Fatalf("protected dispatch occurred: %v count=%d", err, raw.protected.Load())
			}
		})
	}
}

type decisionTokenSource func(context.Context) (Token, error)

func (f decisionTokenSource) Token(ctx context.Context) (Token, error) { return f(ctx) }

func TestDecisionEpochIsCheckedAfterAuthorizationBeforeDispatch(t *testing.T) {
	kernel := &supervisedKernel{}
	client, ctx := supervisionClient(t, kernel)
	raw := &decisionProfileConn{ClientConnInterface: client.config.borrowed, version: "19.29.4", protocol: "19.29.4"}
	client.config.borrowed = raw
	var invalidate atomic.Bool
	var catalog *decision.Catalog
	client.config.tokenSource = decisionTokenSource(func(context.Context) (Token, error) {
		if invalidate.Swap(false) {
			catalog.Epoch.Add(1)
		}
		return Token{AccessToken: "test"}, nil
	})
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	transport := &clientTransport{client: client, store: store}
	catalog = transport.DecisionCatalog()
	lease, err := transport.AcquireDecision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	token := decision.Issue(decision.Evidence{Target: transport.DecisionTarget(events.EventLog), Model: "model", Key: "source", Types: []events.TypeRef{{ID: "changed", Generation: 1}}, Boundary: 5, Catalog: catalog, Epoch: catalog.Epoch.Load(), Generation: lease.Generation, Check: lease.Check})
	lease.Release()
	u, o, err := transactions.Begin(ctx, store.EventLog())
	if err != nil {
		t.Fatal(err)
	}
	if err = u.Enroll(token); err != nil {
		t.Fatal(err)
	}
	invalidate.Store(true)
	if _, err = o.Commit(ctx); !errors.Is(err, transactions.ErrStaleDecision) || raw.protected.Load() != 0 {
		t.Fatalf("stale dispatch after authorization: %v", err)
	}
}

func TestDecisionLeaseCannotSpliceRPCsAcrossGenerationLoss(t *testing.T) {
	kernel := &supervisedKernel{}
	client, ctx := supervisionClient(t, kernel)
	client.config.borrowed = &decisionProfileConn{ClientConnInterface: client.config.borrowed, version: "19.29.4", protocol: "19.29.4"}
	store, err := client.EventStore(ctx, "store")
	if err != nil {
		t.Fatal(err)
	}
	transport := &clientTransport{client: client, store: store}
	lease, err := transport.AcquireDecision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	kernel.endStream <- status.Error(codes.Unavailable, "restart during decision")
	awaitSignal(t, ctx, lease.Context.Done())
	if lease.Check() == nil {
		t.Fatal("generation loss not visible")
	}
	// A cleanup context detaches request cancellation, never transport identity.
	_, err = clients.NewConnectionServiceClient(lease.Conn).ConnectionKeepAlive(decision.CleanupContext(decision.WithDispatchValidation(lease.Context, func() error { return decision.ErrStale })), &clients.ConnectionKeepAlive{})
	var before *faults.BeforeDispatch
	if !errors.As(err, &before) {
		t.Fatalf("old generation dispatched: %v", err)
	}
	generation := lease.Generation
	lease.Release()
	if err = client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	fresh, err := transport.AcquireDecision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Release()
	if fresh.Generation <= generation || fresh.Conn == lease.Conn {
		t.Fatal("generation was reused")
	}
}
