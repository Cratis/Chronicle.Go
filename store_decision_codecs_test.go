// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cratis/chronicle.go/contracts/compliance"
	projectioncontracts "github.com/cratis/chronicle.go/contracts/projections"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// The wire value selects test-owned behavior without sharing mutable callback
// state between tests or putting a callback in the model schema.
var decisionCodecActions sync.Map

type decisionCodecValue string

func (v decisionCodecValue) ConceptValue() string          { return string(v) }
func (v decisionCodecValue) MarshalJSON() ([]byte, error)  { return json.Marshal(string(v)) }
func (v decisionCodecValue) MarshalText() ([]byte, error)  { return []byte(v), nil }
func (v *decisionCodecValue) UnmarshalText(b []byte) error { *v = decisionCodecValue(b); return nil }
func (v *decisionCodecValue) UnmarshalJSON(b []byte) error {
	var value string
	if err := json.Unmarshal(b, &value); err != nil {
		return err
	}
	if action, ok := decisionCodecActions.Load(value); ok {
		if err := action.(func() error)(); err != nil {
			return err
		}
	}
	*v = decisionCodecValue(value)
	return nil
}

type DecisionCodecChanged struct {
	Name string `json:"name"`
}
type DecisionCodecModel struct {
	ID   string             `json:"id" chronicle:"key"`
	Name decisionCodecValue `json:"name" chronicle:"set(DecisionCodecChanged)"`
}

// The client, registration barrier, counted leases, generation transport and
// shutdown are real. Only decision RPC replies are supplied by this adapter.
type decisionCodecConn struct {
	grpc.ClientConnInterface
	models      []*modelcontracts.ReadModelDefinition
	projections []*projectioncontracts.ProjectionDefinition
	document    string
	cleaned     atomic.Int32
	agreements  atomic.Int32
	released    atomic.Int32
}

func (c *decisionCodecConn) Invoke(ctx context.Context, method string, args, reply any, options ...grpc.CallOption) error {
	var response proto.Message
	switch request := args.(type) {
	case *modelcontracts.RegisterManyRequest:
		c.models = proto.CloneOf(request).ReadModels
		response = &emptypb.Empty{}
	case *projectioncontracts.RegisterRequest:
		c.projections = proto.CloneOf(request).Projections
		response = &emptypb.Empty{}
	case *modelcontracts.GetDefinitionsRequest:
		response = &modelcontracts.GetDefinitionsResponse{ReadModels: c.models}
	case *projectioncontracts.GetAllDefinitionsRequest:
		c.agreements.Add(1)
		response = &projectioncontracts.IEnumerable_ProjectionDefinition{Items: c.projections}
	case *sequences.TailSequenceNumberRequest:
		response = &sequences.QueryResult_EventSequenceTailResponse{IsAuthorized: true, Data: &sequences.EventSequenceTailResponse{SequenceNumber: 5}}
	case *modelcontracts.GetInstanceByKeyRequest:
		response = &modelcontracts.GetInstanceByKeyResponse{ReadModel: c.document, LastHandledEventSequenceNumber: 5}
	case *modelcontracts.DehydrateSessionRequest:
		c.cleaned.Add(1)
		response = &emptypb.Empty{}
	case *compliance.ReleaseRequest:
		c.released.Add(1)
		response = &compliance.ReleaseResponse{Payload: request.Payload}
	default:
		return c.ClientConnInterface.Invoke(ctx, method, args, reply, options...)
	}
	proto.Merge(reply.(proto.Message), response)
	return nil
}

type decisionCodecFailure struct{ message string }

func (e *decisionCodecFailure) Error() string { return e.message }

func TestDecisionCodecsRunAfterCleanupWithoutCountingShutdownWork(t *testing.T) {
	for _, tc := range []struct {
		name      string
		protected bool
		actionAt  int
		action    string
	}{
		{"decode-panic", false, 1, "panic"},
		{"decode-error", false, 1, "error"},
		{"decode-typed-error", false, 1, "typed-error"},
		{"decode-close", false, 1, "close"},
		{"decode-cancel", false, 1, "cancel"},
		{"decode-epoch", false, 1, "epoch"},
		{"decode-generation-loss", false, 1, "generation"},
		{"valid-read", false, 1, "none"},
		{"protected-read-refused", true, 1, "none"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := NewRegistry()
			if _, err := RegisterEvent[DecisionCodecChanged](registry); err != nil {
				t.Fatal(err)
			}
			var options []readmodels.ModelOption
			if tc.protected {
				options = append(options, readmodels.WithPII("name"))
			}
			model, err := RegisterReadModel[DecisionCodecModel](registry, options...)
			if err != nil {
				t.Fatal(err)
			}
			kernel := &supervisedKernel{}
			client, ctx := supervisionClient(t, kernel, WithRegistry(registry))
			const sensitive = "private-model-content-in-codec-failure"
			documentValue := t.Name() + sensitive
			value, err := json.Marshal(documentValue)
			if err != nil {
				t.Fatal(err)
			}
			raw := &decisionCodecConn{ClientConnInterface: &decisionProfileConn{ClientConnInterface: client.config.borrowed, version: "19.32.3", protocol: "19.32.3"}, document: `{"id":"source","name":` + string(value) + `}`}
			client.config.borrowed = raw
			store, err := client.EventStore(ctx, "store")
			if err != nil {
				t.Fatal(err)
			}
			client.mu.Lock()
			generation := client.current
			client.mu.Unlock()
			caller, cancel := context.WithCancel(ctx)
			defer cancel()
			codecCause := errors.New(sensitive)
			if tc.action == "typed-error" {
				codecCause = &decisionCodecFailure{message: sensitive}
			}
			var calls atomic.Int32
			decisionCodecActions.Store(documentValue, func() error {
				if raw.cleaned.Load() != 1 || raw.agreements.Load() != 2 || raw.released.Load() != 0 {
					t.Error("application codec ran before cleanup/release/agreement completed")
				}
				if int(calls.Add(1)) != tc.actionAt {
					return nil
				}
				switch tc.action {
				case "panic":
					panic(documentValue)
				case "error", "typed-error":
					return codecCause
				case "close":
					return client.Close() // Synchronous, not a deadline-based escape.
				case "cancel":
					cancel()
				case "epoch":
					store.decisionCatalog.Epoch.Add(1)
				case "generation":
					kernel.endStream <- status.Error(codes.Unavailable, "lost during codec")
					select {
					case <-generation.ctx.Done():
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			})
			t.Cleanup(func() { decisionCodecActions.Delete(documentValue) })
			var read readmodels.DecisionRead[DecisionCodecModel]
			done := make(chan struct{})
			go func() {
				defer close(done)
				read, err = readmodels.DecisionsFor(store.ReadModels(), model).GetDetached(caller, "source")
			}()
			awaitSignal(t, ctx, done)
			if tc.protected {
				var refused *readmodels.DecisionReadRefused
				if !errors.As(err, &refused) || refused.Reason != readmodels.DecisionProtectedModel || !read.Token.IsZero() || read.Instance.Exists || calls.Load() != 0 || raw.cleaned.Load() != 0 || raw.agreements.Load() != 0 || raw.released.Load() != 0 {
					t.Fatal("classified decision did work or issued evidence", err)
				}
				return
			}
			if int(calls.Load()) < tc.actionAt {
				t.Fatal("codec action was not exercised")
			}
			if tc.action == "none" {
				if err != nil || read.Token.IsZero() || !read.Instance.Exists {
					t.Fatalf("valid read: %+v %v", read, err)
				}
				return
			}
			if tc.action == "panic" || tc.action == "error" || tc.action == "typed-error" {
				if err == nil || !read.Token.IsZero() || read.Instance.Exists || strings.Contains(err.Error(), sensitive) || !errors.Is(err, faults.ErrProtocol) {
					t.Fatalf("unsafe codec failure: %+v %v", read, err)
				}
				var panicked *readmodels.DecisionCodecPanicError
				var typed *decisionCodecFailure
				if errors.As(err, &panicked) != (tc.action == "panic") || errors.Is(err, codecCause) != (tc.action != "panic") || errors.As(err, &typed) != (tc.action == "typed-error") {
					t.Fatalf("lost safe codec failure identity: %v", err)
				}
				return
			}
			want := decision.ErrStale
			if tc.action == "cancel" {
				want = context.Canceled
			}
			if !errors.Is(err, want) || !read.Token.IsZero() || read.Instance.Exists {
				t.Fatalf("invalidated codec read: %+v %v", read, err)
			}
		})
	}
}
