// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	projectioncontracts "github.com/cratis/chronicle.go/contracts/projections"
	modelcontracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/internal/clientoptions"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type subscriptionAdded struct{ Name string }
type subscriptionRemoved struct{}
type subscriptionMarker struct{ Name string }
type subscriptionModel struct {
	ID   string `json:"id" chronicle:"key"`
	Name string `json:"name"`
}
type subscriptionBound struct {
	ID   string `chronicle:"key"`
	Name string `chronicle:"set(subscriptionAdded)"`
}
type subscriptionVariantIdentity struct{}

var rejectedCodecCalls atomic.Uint64
var rejectedErrorHooks atomic.Uint64

type subscriptionSecret string

func (subscriptionSecret) ConceptValue() string { panic("discovery must not invoke payload") }
func (subscriptionSecret) MarshalJSON() ([]byte, error) {
	rejectedCodecCalls.Add(1)
	return nil, subscriptionHostileError{}
}
func (*subscriptionSecret) UnmarshalJSON([]byte) error {
	rejectedCodecCalls.Add(1)
	return subscriptionHostileError{}
}
func (subscriptionSecret) MarshalText() ([]byte, error) {
	rejectedCodecCalls.Add(1)
	return nil, subscriptionHostileError{}
}
func (*subscriptionSecret) UnmarshalText([]byte) error {
	rejectedCodecCalls.Add(1)
	return subscriptionHostileError{}
}

type subscriptionHostileEvent struct{ Secret subscriptionSecret }
type subscriptionHostileError struct{}

func (subscriptionHostileError) Error() string {
	rejectedErrorHooks.Add(1)
	return "dummy-private-diagnostic"
}
func (subscriptionHostileError) Unwrap() error {
	rejectedErrorHooks.Add(1)
	return chronicle.ErrNotRegistered
}
func (subscriptionHostileError) Is(error) bool { rejectedErrorHooks.Add(1); return true }
func (subscriptionHostileError) As(any) bool   { rejectedErrorHooks.Add(1); return true }

type subscriptionConnection struct {
	*substituteTransport
	unaryCalls, streamCalls atomic.Uint64
}

func (c *subscriptionConnection) Invoke(ctx context.Context, method string, args, reply any, options ...grpc.CallOption) error {
	c.unaryCalls.Add(1)
	return c.substituteTransport.Invoke(ctx, method, args, reply, options...)
}
func (c *subscriptionConnection) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, options ...grpc.CallOption) (grpc.ClientStream, error) {
	c.streamCalls.Add(1)
	return c.substituteTransport.NewStream(ctx, desc, method, options...)
}

type subscriptionRegistration struct {
	projectioncontracts.UnimplementedProjectionsServer
}

func (*subscriptionRegistration) Register(context.Context, *projectioncontracts.RegisterRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

type subscriptionModels struct {
	modelcontracts.UnimplementedReadModelsServer
}

func (*subscriptionModels) RegisterMany(context.Context, *modelcontracts.RegisterManyRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

type subscriptionAppendServer struct {
	*substituteKernel
	calls int
	mode  string
}

func (s *subscriptionAppendServer) Append(ctx context.Context, request *sequences.AppendRequest) (*sequences.CommandResult_AppendResponse, error) {
	s.calls++
	switch s.mode {
	case "rejected":
		return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: false, HasConstraintViolations: true, ConstraintViolations: []*sequences.ConstraintViolation{{ConstraintName: "strict-test"}}}}, nil
	case "lost reply":
		return nil, status.Error(codes.Unavailable, "reply unavailable")
	case "unknown":
		return &sequences.CommandResult_AppendResponse{IsAuthorized: true}, nil
	default:
		// Admit the production request without pretending to enforce its
		// concurrency scope. This private witness substitutes acceptance only.
		s.mu.Lock()
		position := s.append(request)
		s.mu.Unlock()
		return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true, CorrelationId: request.CorrelationId, SequenceNumber: position, ConcurrencyCheckPerformed: true}}, nil
	}
}

type subscriptionFixture struct {
	scenario     *ReadModelScenario[subscriptionModel]
	registry     *chronicle.Registry
	client       *chronicle.Client
	connection   *subscriptionConnection
	appendServer *subscriptionAppendServer
	providers    *atomic.Uint64
}

func subscriptionRegistry(t *testing.T, kind string) *chronicle.Registry {
	t.Helper()
	r := chronicle.NewRegistry()
	added, err := chronicle.RegisterEvent[subscriptionAdded](r)
	if err != nil {
		t.Fatal(err)
	}
	removed, err := chronicle.RegisterEvent[subscriptionRemoved](r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[subscriptionMarker](r); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterEvent[subscriptionHostileEvent](r); err != nil {
		t.Fatal(err)
	}
	model, err := chronicle.RegisterReadModel[subscriptionModel](r)
	if err != nil {
		t.Fatal(err)
	}
	options := []projections.Option{projections.RemovedWith(removed)}
	if kind == "variant" {
		options = append(options, projections.VariantOf[subscriptionVariantIdentity](), projections.EntersOn(added))
	}
	if kind == "removed only" {
		options = []projections.Option{projections.RemovedWith(removed)}
	}
	b := projections.NewBuilder("strict-selected", model, options...)
	if kind != "all" && kind != "removed only" {
		var fromOptions []projections.FromOption
		if kind == "custom key" {
			fromOptions = append(fromOptions, projections.UsingConstantKey("catalog"))
		}
		projections.From(b, added, nil, fromOptions...)
	}
	if kind == "all" {
		b = projections.NewBuilder("strict-all", model)
	}
	if kind == "all" || kind == "mixed all" {
		projections.All(b, func(e *projections.EveryBuilder[subscriptionModel]) {
			projections.EveryContext(e, projections.Path[subscriptionModel, string]("id"), "eventSourceId")
		})
	}
	if kind == "every" {
		projections.Every(b, func(e *projections.EveryBuilder[subscriptionModel]) {
			projections.EveryMap(e, projections.Path[subscriptionModel, string]("name"), "Name")
		})
	}
	if kind == "join removal" {
		b = projections.NewBuilder("join-removal", model, projections.RemovedWithJoin(removed))
		projections.From(b, added, nil)
	}
	d, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.AddProjection(d); err != nil {
		t.Fatal(err)
	}
	return r
}

// Same private borrowed connection pattern as projection_naming_test. Supplied
// append envelopes witness SDK admission/effects, never kernel projection execution.
func newSubscriptionFixture(t *testing.T, strict bool, kind string) subscriptionFixture {
	t.Helper()
	r := subscriptionRegistry(t, kind)
	connection := &subscriptionConnection{substituteTransport: substituteConnection()}
	providers := new(atomic.Uint64)
	server := &subscriptionAppendServer{substituteKernel: &substituteKernel{events: map[sequenceKey][]*sequences.AppendedEventResponse{}}}
	sequences.RegisterEventSequencesServer(connection.substituteTransport, server)
	projectioncontracts.RegisterProjectionsServer(connection.substituteTransport, &subscriptionRegistration{})
	modelcontracts.RegisterReadModelsServer(connection.substituteTransport, &subscriptionModels{})
	client, err := chronicle.NewClient(chronicle.WithRegistry(r), clientoptions.Connection[chronicle.ClientOption](connection), chronicle.WithNoAuthentication(),
		chronicle.WithIdentityProvider(func(context.Context) (identities.Identity, bool, error) {
			providers.Add(1)
			return identities.NotSet(), false, nil
		}),
		chronicle.WithCausationProvider(func(context.Context) ([]metadata.Causation, bool, error) { providers.Add(1); return nil, false, nil }),
		chronicle.WithCorrelationProvider(func(context.Context) (metadata.CorrelationID, bool, error) {
			providers.Add(1)
			return metadata.CorrelationID{}, false, nil
		}),
		chronicle.WithEventEnrichers(func(context.Context, events.TypeRef, *events.EventContent) error { providers.Add(1); return nil }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
		if err := connection.Close(); err != nil {
			t.Error(err)
		}
	})
	artifacts, err := client.Artifacts("strict-store")
	if err != nil {
		t.Fatal(err)
	}
	model, ok := artifacts.ReadModels.LookupType(artifacts.Projections[0].Model().GoType())
	if !ok {
		t.Fatal("compiled model absent")
	}
	s := &ReadModelScenario[subscriptionModel]{config: Config{Store: "strict-store", Namespace: "strict-ns"}, client: client, artifacts: artifacts, model: model, projection: artifacts.Projections[0]}
	if strict {
		s.subscription, err = strictProjectionSubscription(s.projection, artifacts.Events)
		if err != nil {
			t.Fatal(err)
		}
	}
	if connection.unaryCalls.Load() != 0 || connection.streamCalls.Load() != 0 || providers.Load() != 0 {
		t.Fatal("compilation performed I/O or invoked providers")
	}
	store, err := client.EventStore(t.Context(), "strict-store", chronicle.WithNamespace("strict-ns"))
	if err != nil {
		t.Fatal(err)
	}
	s.eventScenario = &EventScenario{Client: client, Store: store}
	return subscriptionFixture{s, r, client, connection, server, providers}
}

func TestStrictProjectionGivenRejectsBeforeAnySeedEffect(t *testing.T) {
	f := newSubscriptionFixture(t, true, "ordinary")
	beforeUnary, beforeStreams := f.connection.unaryCalls.Load(), f.connection.streamCalls.Load()
	err := f.scenario.Given(t.Context(), "source", subscriptionMarker{Name: "dummy-payload"})
	if !errors.Is(err, ErrUnsubscribedEventSeeded) {
		t.Fatalf("registered marker = %v; want strict rejection", err)
	}
	if message := err.Error(); !strings.Contains(message, "strict-selected") || !strings.Contains(message, "subscriptionMarker") || strings.Contains(message, "dummy-payload") {
		t.Fatal("strict rejection did not report SDK identities without event content")
	}
	if f.connection.unaryCalls.Load() != beforeUnary || f.connection.streamCalls.Load() != beforeStreams || f.appendServer.calls != 0 || f.providers.Load() != 0 || len(f.scenario.history) != 0 {
		t.Fatal("rejection performed a seed effect")
	}
	rejectedCodecCalls.Store(0)
	rejectedErrorHooks.Store(0)
	if err := f.scenario.Given(t.Context(), "source", subscriptionHostileEvent{Secret: "dummy"}); !errors.Is(err, ErrUnsubscribedEventSeeded) {
		t.Fatalf("hostile seed = %v", err)
	}
	if rejectedCodecCalls.Load() != 0 || rejectedErrorHooks.Load() != 0 || f.providers.Load() != 0 || f.appendServer.calls != 0 {
		t.Fatal("rejected seed invoked codec, error hooks, or outgoing preparation")
	}
}

func TestDefaultProjectionGivenStillAcceptsRegisteredMarker(t *testing.T) {
	f := newSubscriptionFixture(t, false, "ordinary")
	if err := f.scenario.Given(t.Context(), "source", subscriptionMarker{}); err != nil {
		t.Fatal(err)
	}
	if f.appendServer.calls != 1 || len(f.scenario.history) != 1 || f.providers.Load() != 4 {
		t.Fatal("default seeding changed")
	}
}

func TestStrictProjectionGivenPreservesEarlierSeedsAndSyntheticOrder(t *testing.T) {
	f := newSubscriptionFixture(t, true, "ordinary")
	if err := f.scenario.Given(t.Context(), "source", subscriptionAdded{Name: "A"}, subscriptionMarker{}, subscriptionAdded{Name: "not-attempted"}); !errors.Is(err, ErrUnsubscribedEventSeeded) {
		t.Fatal(err)
	}
	if len(f.scenario.history) != 1 || f.scenario.history[0].Content.(*subscriptionAdded).Name != "A" {
		t.Fatal("mixed call lost earlier success or attempted later item")
	}
	if err := f.scenario.Given(t.Context(), "source", subscriptionAdded{Name: "B"}); err != nil {
		t.Fatal(err)
	}
	if len(f.scenario.history) != 2 || f.appendServer.calls != 2 || f.scenario.history[1].Content.(*subscriptionAdded).Name != "B" {
		t.Fatal("accepted history changed")
	}
	for i, event := range f.scenario.history {
		if event.Context.SequenceNumber != events.SequenceNumber(i) {
			t.Fatal("rejection advanced synthetic position")
		}
	}
	if f.scenario.history[0].Context.CorrelationID == f.scenario.history[1].Context.CorrelationID {
		t.Fatal("synthetic correlations were reused")
	}
}

func TestStrictProjectionGivenRetainsExistingAdmissionAndAppendFailures(t *testing.T) {
	f := newSubscriptionFixture(t, true, "ordinary")
	beforeUnary, beforeStreams := f.connection.unaryCalls.Load(), f.connection.streamCalls.Load()
	rejectedCodecCalls.Store(0)
	rejectedErrorHooks.Store(0)
	if err := f.scenario.Given(t.Context(), "source", struct{}{}); !errors.Is(err, chronicle.ErrNotRegistered) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := f.scenario.Given(ctx, "source", subscriptionHostileEvent{}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if f.connection.unaryCalls.Load() != beforeUnary || f.connection.streamCalls.Load() != beforeStreams || f.providers.Load() != 0 || rejectedCodecCalls.Load() != 0 || rejectedErrorHooks.Load() != 0 || len(f.scenario.history) != 0 {
		t.Fatal("unknown/canceled seed performed effects")
	}
	for _, mode := range []string{"rejected", "lost reply", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			f.appendServer.mode = mode
			err := f.scenario.Given(t.Context(), "source", subscriptionAdded{})
			if err == nil {
				t.Fatal("failed append reported success")
			}
			if mode == "rejected" {
				var rejection *eventsequences.ConstraintError
				if !errors.As(err, &rejection) {
					t.Fatal("domain rejection identity lost")
				}
			} else {
				var unknown *eventsequences.OutcomeUnknownError
				if !errors.As(err, &unknown) {
					t.Fatal("unknown append outcome was misclassified")
				}
			}
			if len(f.scenario.history) != 0 {
				t.Fatal("failed append advanced history")
			}
		})
	}
	beforeUnary, beforeStreams = f.connection.unaryCalls.Load(), f.connection.streamCalls.Load()
	beforeProviders := f.providers.Load()
	f.scenario.closed = true
	if err := f.scenario.Given(t.Context(), "source", subscriptionHostileEvent{}); !errors.Is(err, chronicle.ErrClosed) {
		t.Fatal(err)
	}
	if f.connection.unaryCalls.Load() != beforeUnary || f.connection.streamCalls.Load() != beforeStreams || f.providers.Load() != beforeProviders || rejectedCodecCalls.Load() != 0 || rejectedErrorHooks.Load() != 0 {
		t.Fatal("closed fixture performed effects")
	}
}

func TestStrictProjectionMembershipUsesCompiledRootSubscriptions(t *testing.T) {
	for _, kind := range []string{"ordinary", "every", "removed only", "all"} {
		t.Run(kind, func(t *testing.T) {
			f := newSubscriptionFixture(t, true, kind)
			if err := f.scenario.Given(t.Context(), "source", subscriptionRemoved{}); err != nil {
				t.Fatal(err)
			}
			err := f.scenario.Given(t.Context(), "source", subscriptionMarker{})
			if kind == "all" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, ErrUnsubscribedEventSeeded) {
				t.Fatal("Every or another non-subscription broadened membership", err)
			}
		})
	}
	r := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[subscriptionAdded](r); err != nil {
		t.Fatal(err)
	}
	if _, err := chronicle.RegisterReadModel[subscriptionBound](r); err != nil {
		t.Fatal(err)
	}
	client, err := chronicle.NewClient(chronicle.WithRegistry(r))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	a, err := client.Artifacts("bound")
	if err != nil {
		t.Fatal(err)
	}
	selected, err := strictProjectionSubscription(a.Projections[0], a.Events)
	if err != nil || selected.admit("subscriptionAdded") != nil || !errors.Is(selected.admit("subscriptionMarker"), ErrUnsubscribedEventSeeded) {
		t.Fatal("model-bound membership", err)
	}
	// The predicate accepts IDs only. A registered historical descriptor has the
	// same persisted ID, but a different generation and Go name.
	current, err := events.Define[subscriptionAdded](events.WithID("stable"), events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	old, err := events.DefineGeneration[subscriptionMarker](current, 1)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(current.Descriptor(), old.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	m, err := readmodels.Define[subscriptionModel]()
	if err != nil {
		t.Fatal(err)
	}
	b := projections.NewBuilder("id-membership", m)
	projections.From(b, current, nil)
	d, err := b.Build()
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := projections.Compile(d, catalog)
	if err != nil {
		t.Fatal(err)
	}
	selected, err = strictProjectionSubscription(compiled, catalog)
	if err != nil || selected.admit(old.Ref().ID) != nil {
		t.Fatal("historical ID rejected", err)
	}
}

func TestStrictProjectionRefusesUnsupportedProfilesBeforeConnection(t *testing.T) {
	for _, kind := range []string{"variant", "custom key", "mixed all", "join removal"} {
		t.Run(kind, func(t *testing.T) {
			r := subscriptionRegistry(t, kind)
			s, err := OpenReadModelScenario[subscriptionModel](t.Context(), Config{Registry: r, Engine: Kernel, ConnectionString: "chronicle://127.0.0.1:1"}, ReadModelOptions[subscriptionModel]{StrictEventSubscription: true})
			if s != nil || !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatalf("unsupported constructor = %v, %v", s, err)
			}
			connection := &subscriptionConnection{substituteTransport: substituteConnection()}
			client, err := chronicle.NewClient(chronicle.WithRegistry(r), clientoptions.Connection[chronicle.ClientOption](connection), chronicle.WithNoAuthentication())
			if err != nil {
				t.Fatal(err)
			}
			a, err := client.Artifacts("strict-profile")
			if err != nil {
				t.Fatal(err)
			}
			selected, err := strictProjectionSubscription(a.Projections[0], a.Events)
			if selected != nil || !errors.Is(err, chronicle.ErrUnsupported) || connection.unaryCalls.Load() != 0 || connection.streamCalls.Load() != 0 {
				t.Fatal("profile refusal invoked RPC or admitted unsupported definition")
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			if err := connection.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	if selected, err := strictProjectionSubscription(projections.Definition{}, nil); selected != nil || !errors.Is(err, chronicle.ErrNotRegistered) {
		t.Fatal("missing compiler metadata admitted")
	}
}

func TestStrictProjectionInlineSelectionDoesNotMutateOriginalRegistry(t *testing.T) {
	r := subscriptionRegistry(t, "ordinary")
	client, err := chronicle.NewClient(chronicle.WithRegistry(r))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	original, err := client.Artifacts("inline")
	if err != nil {
		t.Fatal(err)
	}
	// Inline replacement uses the exact registered model handle.
	replacementRegistry := chronicle.NewRegistry()
	added, err := chronicle.RegisterEvent[subscriptionAdded](replacementRegistry)
	if err != nil {
		t.Fatal(err)
	}
	marker, err := chronicle.RegisterEvent[subscriptionMarker](replacementRegistry)
	if err != nil {
		t.Fatal(err)
	}
	registeredModel, err := chronicle.RegisterReadModel[subscriptionModel](replacementRegistry)
	if err != nil {
		t.Fatal(err)
	}
	base := projections.NewBuilder("original", registeredModel)
	projections.From(base, added, nil)
	baseDeclaration, err := base.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := replacementRegistry.AddProjection(baseDeclaration); err != nil {
		t.Fatal(err)
	}
	replacement := projections.NewBuilder("replacement", registeredModel)
	projections.From(replacement, marker, nil)
	declaration, err := replacement.Build()
	if err != nil {
		t.Fatal(err)
	}
	detached, err := replacementRegistry.WithProjection(declaration)
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range []struct {
		registry           *chronicle.Registry
		accepted, rejected events.TypeID
	}{{detached, marker.Ref().ID, added.Ref().ID}, {replacementRegistry, added.Ref().ID, marker.Ref().ID}} {
		c, err := chronicle.NewClient(chronicle.WithRegistry(candidate.registry))
		if err != nil {
			t.Fatal(err)
		}
		a, err := c.Artifacts("inline")
		if err != nil {
			t.Fatal(err)
		}
		s, err := strictProjectionSubscription(a.Projections[0], a.Events)
		if err != nil || s.admit(candidate.accepted) != nil || !errors.Is(s.admit(candidate.rejected), ErrUnsubscribedEventSeeded) {
			t.Fatal("inline selection membership", err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if original.Projections[0].Identifier() != "strict-selected" {
		t.Fatal("original frozen registry changed")
	}
}

var _ json.Marshaler = subscriptionSecret("")

type subscriptionChild struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}
type subscriptionComplex struct {
	ID       string              `json:"id" chronicle:"key"`
	Children []subscriptionChild `json:"children"`
	Nested   *subscriptionChild  `json:"nested"`
}
type subscriptionProtected struct {
	ID     string `json:"id" chronicle:"key"`
	Secret string `json:"secret" chronicle:"pii"`
}

func assertStrictUnsupportedBeforeIO[M any](t *testing.T, registry *chronicle.Registry) {
	t.Helper()
	s, err := OpenReadModelScenario[M](t.Context(), Config{Registry: registry, Engine: Kernel, ConnectionString: "chronicle://127.0.0.1:1"}, ReadModelOptions[M]{StrictEventSubscription: true})
	if s != nil || !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("unsupported strict profile = %v, %v", s, err)
	}
	connection := &subscriptionConnection{substituteTransport: substituteConnection()}
	providers := new(atomic.Uint64)
	client, err := chronicle.NewClient(chronicle.WithRegistry(registry), clientoptions.Connection[chronicle.ClientOption](connection), chronicle.WithNoAuthentication(), chronicle.WithCorrelationProvider(func(context.Context) (metadata.CorrelationID, bool, error) {
		providers.Add(1)
		return metadata.CorrelationID{}, false, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	a, err := client.Artifacts("unsupported")
	if err != nil {
		t.Fatal(err)
	}
	selected, err := strictProjectionSubscription(a.Projections[0], a.Events)
	if selected != nil || !errors.Is(err, chronicle.ErrUnsupported) || connection.unaryCalls.Load() != 0 || connection.streamCalls.Load() != 0 || providers.Load() != 0 {
		t.Fatal("unsupported profile produced effects")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStrictProjectionChecksProtectionAcrossSubscribedGenerations(t *testing.T) {
	for _, kind := range []string{"unprotected current only", "protected current", "protected historical"} {
		t.Run(kind, func(t *testing.T) {
			r := chronicle.NewRegistry()
			protection := events.WithProtection(compliance.Property("Name", compliance.Classification{PII: true}))
			options := []events.TypeOption{events.WithID("subscribed-generations"), events.WithGeneration(2)}
			if kind == "protected current" {
				options = append(options, protection)
			}
			current, err := chronicle.RegisterEvent[subscriptionAdded](r, options...)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "protected historical" {
				previous, err := chronicle.RegisterEventGeneration[subscriptionMarker](r, current, 1, protection)
				if err != nil {
					t.Fatal(err)
				}
				if previous.Ref().ID != current.Ref().ID || previous.Ref().Generation != 1 || current.Ref().Generation != 2 {
					t.Fatal("fixture generations do not share the subscribed event ID")
				}
			}
			model, err := chronicle.RegisterReadModel[subscriptionModel](r)
			if err != nil {
				t.Fatal(err)
			}
			builder := projections.NewBuilder("subscribed-generations", model)
			projections.From(builder, current, nil)
			declaration, err := builder.Build()
			if err != nil {
				t.Fatal(err)
			}
			if err := r.AddProjection(declaration); err != nil {
				t.Fatal(err)
			}
			if kind != "unprotected current only" {
				assertStrictUnsupportedBeforeIO[subscriptionModel](t, r)
				return
			}
			// The same current-generation projection is admitted without protection;
			// a generation number alone must not explain either negative case.
			client, err := chronicle.NewClient(chronicle.WithRegistry(r))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := client.Close(); err != nil {
					t.Error(err)
				}
			})
			artifacts, err := client.Artifacts("unprotected")
			if err != nil {
				t.Fatal(err)
			}
			selected, err := strictProjectionSubscription(artifacts.Projections[0], artifacts.Events)
			if err != nil || selected == nil || selected.admit(current.Ref().ID) != nil {
				t.Fatal("unprotected subscribed generation refused", err)
			}
		})
	}
}

func TestStrictProjectionRefusesChildrenNestedJoinAndProtectedModel(t *testing.T) {
	for _, kind := range []string{"children", "nested", "join"} {
		t.Run(kind, func(t *testing.T) {
			r := chronicle.NewRegistry()
			e, err := chronicle.RegisterEvent[subscriptionAdded](r)
			if err != nil {
				t.Fatal(err)
			}
			m, err := chronicle.RegisterReadModel[subscriptionComplex](r)
			if err != nil {
				t.Fatal(err)
			}
			b := projections.NewBuilder("complex", m)
			projections.From(b, e, nil)
			switch kind {
			case "children":
				projections.Children(b, projections.Path[subscriptionComplex, []subscriptionChild]("children"), func(child *projections.Builder[subscriptionChild]) { projections.From(child, e, nil) })
			case "nested":
				projections.Nested(b, projections.Path[subscriptionComplex, *subscriptionChild]("nested"), func(child *projections.Builder[subscriptionChild]) { projections.From(child, e, nil) })
			case "join":
				projections.Join(b, e, projections.Path[subscriptionComplex, string]("id"), nil)
			}
			d, err := b.Build()
			if err != nil {
				t.Fatal(err)
			}
			if err := r.AddProjection(d); err != nil {
				t.Fatal(err)
			}
			assertStrictUnsupportedBeforeIO[subscriptionComplex](t, r)
		})
	}
	t.Run("protected model", func(t *testing.T) {
		r := chronicle.NewRegistry()
		e, err := chronicle.RegisterEvent[subscriptionAdded](r)
		if err != nil {
			t.Fatal(err)
		}
		m, err := chronicle.RegisterReadModel[subscriptionProtected](r)
		if err != nil {
			t.Fatal(err)
		}
		b := projections.NewBuilder("protected", m)
		projections.From(b, e, nil)
		d, err := b.Build()
		if err != nil {
			t.Fatal(err)
		}
		if err := r.AddProjection(d); err != nil {
			t.Fatal(err)
		}
		assertStrictUnsupportedBeforeIO[subscriptionProtected](t, r)
	})
}
