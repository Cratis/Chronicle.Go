// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/seeding"
)

func captureForTest(t *testing.T, options ...ClientOption) *ClientPreparation {
	t.Helper()
	p, err := CaptureClient(options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Client().Close(); err != nil {
			t.Error(err)
		}
	})
	return p
}

func assertPreparationGuards(t *testing.T, client *Client, want error) {
	t.Helper()
	// Nil contexts, blank coordinates and invalid options must not win over the
	// state guard, invoke callbacks, or enter Ready's connection retry loop.
	var invalidContext context.Context // Deliberately nil: state checks precede context use.
	operations := []struct {
		name string
		call func() error
	}{
		{"connect", func() error { return client.Connect(invalidContext) }},
		{"ready", func() error { return client.Ready(invalidContext) }},
		{"store", func() error {
			_, err := client.EventStore(invalidContext, "secret-store", func(*storeConfig) { t.Error("store option ran") })
			return err
		}},
		{"nil store option", func() error { _, err := client.EventStore(invalidContext, "", nil); return err }},
		{"stores", func() error { _, err := client.EventStores(invalidContext); return err }},
		{"catalogs", func() error { _, _, err := client.Catalogs(""); return err }},
		{"artifacts", func() error { _, err := client.Artifacts(""); return err }},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			err := operation.call()
			var state *ClientStateError
			if !errors.Is(err, want) || !errors.As(err, &state) {
				t.Fatal("missing state guard", err)
			}
			for _, text := range []string{fmt.Sprint(err), fmt.Sprintf("%+v", err), fmt.Sprintf("%#v", err)} {
				if strings.Contains(text, "secret") {
					t.Fatal("state error leaked coordinates")
				}
			}
		})
	}
}

func TestCapturedAndFailedClientsGuardEveryEntryPoint(t *testing.T) {
	registry := NewRegistry()
	failure := errors.New("secret preparation failure")
	if err := RegisterSeederFunc(registry, func(*seeding.Builder) error { return failure }); err != nil {
		t.Fatal(err)
	}
	p := captureForTest(t, WithRegistry(registry))
	identity := p.Client()
	if p.Client() != identity {
		t.Fatal("unstable identity")
	}
	assertPreparationGuards(t, p.Client(), ErrNotPrepared)
	if client, err := p.Prepare(t.Context(), nil); client != nil || !errors.Is(err, failure) {
		t.Fatal(client, err)
	}
	assertPreparationGuards(t, p.Client(), ErrNotPrepared)
	if err := p.Client().Close(); err != nil {
		t.Fatal(err)
	}
	assertPreparationGuards(t, p.Client(), ErrClosed)
}

func TestCaptureDefersCallbacksAndFreezesAllSelectedEpochs(t *testing.T) {
	registry := NewRegistry()
	declareEvent[catalogEvent](t, registry)
	calls := 0
	if err := RegisterSeederFunc(registry, func(*seeding.Builder) error { calls++; return nil }); err != nil {
		t.Fatal(err)
	}
	p := captureForTest(t, WithRegistry(registry), WithRegistryForStore("same", registry), WithRegistryForStore("empty", nil))
	if calls != 0 {
		t.Fatal("capture prepared application declarations")
	}
	declareEvent[catalogReplacement](t, registry)
	if err := RegisterSeederFunc(registry, func(*seeding.Builder) error { t.Error("late callback ran"); return nil }); err != nil {
		t.Fatal(err)
	}
	client, err := p.Prepare(t.Context(), nil)
	if err != nil || client != p.Client() {
		t.Fatal(client, err)
	}
	if calls != 1 {
		t.Fatal("shared registry compiled more than once", calls)
	}
	for _, name := range []StoreName{"default", "same", "empty"} {
		catalog, _, err := client.Catalogs(name)
		if err != nil {
			t.Fatal(err)
		}
		want := 1
		if name == "empty" {
			want = 0
		}
		if len(catalog.Descriptors()) != want {
			t.Fatal("captured epoch/replacement lost", name)
		}
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	// Completed calls ignore all arguments and retain the historical identity.
	repeated, err := p.Prepare(nil, (*preparationFactory)(nil)) //nolint:staticcheck // SA1012: completed outcomes ignore invalid arguments.
	if err != nil || repeated != client || calls != 1 {
		t.Fatal(repeated, err, calls)
	}
	if _, err := client.Artifacts("same"); err != nil {
		t.Fatal(err)
	}
	if err := client.Connect(nil); !errors.Is(err, ErrClosed) { //nolint:staticcheck // SA1012: closed guard precedes context access.
		t.Fatal(err)
	}
}

func TestHandledCallbackDenialAndExternalProbeDoNotPoisonPublication(t *testing.T) {
	registry := NewRegistry()
	entered, resume := make(chan struct{}), make(chan struct{})
	var p *ClientPreparation
	if err := RegisterSeederFunc(registry, func(*seeding.Builder) error {
		if err := p.Client().Ready(context.Background()); !errors.Is(err, ErrNotPrepared) {
			return errors.New("missing guard")
		}
		if _, err := p.Prepare(nil, nil); !errors.Is(err, ErrPreparationInProgress) { //nolint:staticcheck // SA1012: reentrant admission precedes argument validation.
			return errors.New("reentrant preparation did not fail promptly")
		}
		close(entered)
		<-resume
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	p = captureForTest(t, WithRegistry(registry))
	result := make(chan error, 1)
	go func() { _, err := p.Prepare(context.Background(), nil); result <- err }()
	<-entered
	assertPreparationGuards(t, p.Client(), ErrNotPrepared)
	if _, err := p.Prepare(t.Context(), nil); !errors.Is(err, ErrPreparationInProgress) {
		t.Error(err)
	}
	close(resume)
	if err := <-result; err != nil {
		t.Fatal("denial poisoned publication", err)
	}
}

func TestCloseBeforePreparationHasNoCallbacks(t *testing.T) {
	registry := NewRegistry()
	if err := RegisterSeederFunc(registry, func(*seeding.Builder) error { t.Error("callback after close"); return nil }); err != nil {
		t.Fatal(err)
	}
	p := captureForTest(t, WithRegistry(registry))
	if err := p.Client().Close(); err != nil {
		t.Fatal(err)
	}
	client, err := p.Prepare(t.Context(), nil)
	if client != nil || !errors.Is(err, ErrClosed) {
		t.Fatal(client, err)
	}
	_, repeated := p.Prepare(nil, nil) //nolint:staticcheck // SA1012: retained failure ignores invalid arguments.
	if repeated != err {
		t.Fatal("failure was not retained")
	}
	assertPreparationGuards(t, p.Client(), ErrClosed)
}

func TestPreparationCloseCancelsButDoesNotJoinCallerOwnedCleanup(t *testing.T) {
	for _, trigger := range []string{"external close", "callback close", "closer close", "caller cancel"} {
		t.Run(trigger, func(t *testing.T) {
			registry := NewRegistry()
			entered, cleanup, finish := make(chan struct{}), make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var p *ClientPreparation
			factory := preparationFactory{open: func(context.Context) (reactors.Scope, error) {
				return &preparationScope{close: func(closeCtx context.Context) error {
					if closeCtx.Err() != nil {
						t.Error("cleanup inherited cancellation")
					}
					if trigger == "closer close" {
						if err := p.Client().Close(); err != nil {
							t.Error(err)
						}
					}
					close(cleanup)
					<-finish
					return nil
				}}, nil
			}}
			if err := RegisterConstraintFactory(registry, []string{"unused"}, func(ctx context.Context) *preparationMetadata {
				close(entered)
				if trigger == "callback close" {
					if err := p.Client().Close(); err != nil {
						t.Error(err)
					}
				}
				if trigger != "closer close" {
					<-ctx.Done()
				}
				return &preparationMetadata{}
			}, func(context.Context, *preparationMetadata) ([]constraints.Definition, error) {
				return nil, errors.New("definition failed")
			}); err != nil {
				t.Fatal(err)
			}
			// Ensure explicit constructor precedence rather than a fake registered service.
			factory.contains = func(typ reflect.Type) bool { return typ != reflect.TypeFor[*preparationMetadata]() }
			p = captureForTest(t, WithRegistry(registry))
			result := make(chan error, 1)
			go func() { _, err := p.Prepare(ctx, factory); result <- err }()
			<-entered
			if trigger == "external close" {
				if err := p.Client().Close(); err != nil {
					t.Fatal(err)
				}
			}
			if trigger == "caller cancel" {
				cancel()
			}
			<-cleanup
			select {
			case <-result:
				t.Fatal("preparation returned before cleanup")
			default:
			}
			if _, err := p.Prepare(nil, nil); !errors.Is(err, ErrPreparationInProgress) { //nolint:staticcheck // SA1012: cleanup retains admission despite invalid arguments.
				t.Error("cleanup released admission early", err)
			}
			// Closing is complete even though preparation is still in its closer.
			if err := p.Client().Close(); err != nil {
				t.Error(err)
			}
			select {
			case <-result:
				t.Error("Close claimed to join preparation")
			default:
			}
			close(finish)
			err := <-result
			if !errors.Is(err, ErrClosed) {
				t.Fatal("closed identity was published", err)
			}
			if trigger != "closer close" && !errors.Is(err, context.Canceled) {
				t.Fatal("lost cancellation", err)
			}
		})
	}
}

type nilPreparationContext struct{ context.Context }

func TestPreparationArgumentsValidateBeforeAdmissionAndCompletedOutcomeWins(t *testing.T) {
	for _, configured := range []bool{false, true} {
		t.Run(fmt.Sprint(configured), func(t *testing.T) {
			var options []ClientOption
			factory := preparationFactory{contains: func(reflect.Type) bool { panic("completed call reevaluated provider") }}
			if configured {
				options = append(options, WithServices(factory))
			}
			p := captureForTest(t, options...)
			for _, call := range []func() (*Client, error){
				func() (*Client, error) { return p.Prepare(nil, nil) }, //nolint:staticcheck // SA1012: nil-context validation is the behavior under test.
				func() (*Client, error) { return p.Prepare(t.Context(), (*preparationFactory)(nil)) },
				func() (*Client, error) { return p.Prepare((*nilPreparationContext)(nil), nil) },
			} {
				if c, err := call(); c != nil || !errors.Is(err, ErrInvalidConfiguration) {
					t.Fatal(c, err)
				}
			}
			if configured {
				if _, err := p.Prepare(t.Context(), factory); !errors.Is(err, ErrInvalidConfiguration) {
					t.Fatal(err)
				}
			}
			client, err := p.Prepare(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if again, err := p.Prepare(nil, factory); err != nil || again != client { //nolint:staticcheck // SA1012: completed outcomes ignore invalid arguments.
				t.Fatal(again, err)
			}
		})
	}
	p := captureForTest(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, failure := p.Prepare(ctx, nil)
	if !errors.Is(failure, context.Canceled) {
		t.Fatal(failure)
	}
	if client, err := p.Prepare(t.Context(), nil); client != nil || err != failure {
		t.Fatal(client, err)
	}
}

func TestPreparationDeadlineWaitsForCooperativeCleanupRatherThanReturningFalseCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		registry := NewRegistry()
		cleanup, finish := make(chan struct{}), make(chan struct{})
		if err := RegisterConstraintFactory(registry, []string{"unused"}, func(ctx context.Context) *preparationArtifact {
			<-ctx.Done()
			return &preparationArtifact{close: func(context.Context) error { close(cleanup); <-finish; return nil }}
		}, func(context.Context, *preparationArtifact) ([]constraints.Definition, error) { return nil, nil }); err != nil {
			t.Fatal(err)
		}
		p := captureForTest(t, WithRegistry(registry))
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel()
		result := make(chan error, 1)
		go func() { _, err := p.Prepare(ctx, nil); result <- err }()
		<-cleanup
		select {
		case <-result:
			t.Fatal("deadline masqueraded as cleanup completion")
		default:
		}
		close(finish)
		if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	})
}

func TestPreparationPreflightsLaterStoreConstructorsBeforeAnyScope(t *testing.T) {
	for _, invalid := range []string{"constructor", "seeder", "metadata"} {
		t.Run(invalid, func(t *testing.T) {
			first, later := NewRegistry(), NewRegistry()
			if err := RegisterConstraintFactory(first, []string{"first"}, func() *preparationMetadata { t.Error("early constructor"); return nil }, func(context.Context, *preparationMetadata) ([]constraints.Definition, error) { return nil, nil }); err != nil {
				t.Fatal(err)
			}
			switch invalid {
			case "constructor":
				if err := RegisterConstraintFactory(later, []string{"later"}, func(*EventStore) *preparationMetadata { return nil }, func(context.Context, *preparationMetadata) ([]constraints.Definition, error) { return nil, nil }); err != nil {
					t.Fatal(err)
				}
			case "seeder":
				if err := RegisterSeederFactory[*clientDependentSeeder](later, func(*EventStore) *clientDependentSeeder { return nil }); err != nil {
					t.Fatal(err)
				}
			case "metadata":
				model, err := RegisterReadModel[catalogModel](NewRegistry())
				if err != nil {
					t.Fatal(err)
				}
				// Directly selected factory metadata references a model in a different epoch.
				later.projectionFactories = append(later.projectionFactories, projectionFactoryDeclaration{id: "missing", model: model.Descriptor()})
			}
			opened := 0
			factory := preparationFactory{contains: func(typ reflect.Type) bool {
				return typ != reflect.TypeFor[*preparationMetadata]() && typ != reflect.TypeFor[*clientDependentSeeder]()
			}, open: func(context.Context) (reactors.Scope, error) { opened++; return nil, errors.New("scope must not open") }}
			p := captureForTest(t, WithRegistryForStore("a", first), WithRegistryForStore("z", later))
			if client, err := p.Prepare(t.Context(), factory); client != nil || !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatal(client, err)
			}
			if opened != 0 {
				t.Fatal("preflight allowed earlier activation")
			}
		})
	}
}

type clientDependentSeeder struct{ client *Client }

func (*clientDependentSeeder) Seed(*seeding.Builder) error { return nil }

func TestPreparationRequiresExactBorrowedClientDependency(t *testing.T) {
	for _, mode := range []string{"default", "exact", "wrong identity", "dynamic exact", "dynamic wrong", "dynamic default"} {
		t.Run(mode, func(t *testing.T) {
			registry := NewRegistry()
			var constructor any = func(client *Client) *clientDependentSeeder { return &clientDependentSeeder{client} }
			if strings.HasPrefix(mode, "dynamic") {
				constructor = func(ctx context.Context, scope reactors.Scope) (*clientDependentSeeder, error) {
					value, err := scope.Resolve(ctx, reflect.TypeFor[*Client]())
					if err != nil {
						return nil, err
					}
					return &clientDependentSeeder{value.(*Client)}, nil
				}
			}
			if err := RegisterSeederFactory[*clientDependentSeeder](registry, constructor); err != nil {
				t.Fatal(err)
			}
			p := captureForTest(t, WithRegistry(registry))
			var scopes reactors.ScopeFactory
			if !strings.Contains(mode, "default") {
				identity := p.Client()
				if strings.Contains(mode, "wrong") {
					identity = captureForTest(t).Client()
				}
				scopes = preparationFactory{contains: func(typ reflect.Type) bool { return typ != reflect.TypeFor[*clientDependentSeeder]() }, open: func(context.Context) (reactors.Scope, error) {
					return &preparationScope{resolve: func(context.Context, reflect.Type) (any, error) { return identity, nil }, close: func(context.Context) error { return nil }}, nil
				}}
			}
			client, err := p.Prepare(t.Context(), scopes)
			if strings.Contains(mode, "exact") {
				if err != nil || client != p.Client() {
					t.Fatal(client, err)
				}
			} else if client != nil || !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatal(client, err)
			}
		})
	}
}

func TestPreparationRejectsSDKArtifactResultsBeforeActivation(t *testing.T) {
	for _, typ := range []reflect.Type{reflect.TypeFor[*Client](), reflect.TypeFor[Client](), reflect.TypeFor[*EventStore](), reflect.TypeFor[*events.Catalog]()} {
		t.Run(typ.String(), func(t *testing.T) {
			registry := NewRegistry()
			registry.constraintFactories = append(registry.constraintFactories, constraintFactoryDeclaration{definitionFactory: definitionFactory{typ: typ}, names: []string{"owned"}})
			p := captureForTest(t, WithRegistry(registry))
			scopes := preparationFactory{open: func(context.Context) (reactors.Scope, error) { t.Error("SDK result activated"); return nil, nil }}
			if client, err := p.Prepare(t.Context(), scopes); client != nil || !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatal(client, err)
			}
		})
	}
}
