// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/reactors"
	di "github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

type selectorBoundaryEvent struct{ Value string }
type selectorBoundaryReactor struct{}

func (*selectorBoundaryReactor) Handle(context.Context, selectorBoundaryEvent) error { return nil }

// Activate uses the production retained reactor plan and its adapted service
// scope, not a direct Fundamentals resolution. No transport is needed on failure.
func TestSelectorErrorsSurviveProductionReactorFactoryAdapter(t *testing.T) {
	ordinary := errors.New("ordinary selector failure")
	for _, mode := range []string{"PrepareClient", "WithServices"} {
		for _, category := range []string{"ordinary", "canceled", "deadline", "panic", "unsafe", "mixed aliases"} {
			t.Run(mode+"/"+category, func(t *testing.T) {
				called, formatted := false, 0
				secret := &secretFailure{&formatted}
				want := ordinary
				selector := func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
					switch category {
					case "canceled":
						return "", "", context.Canceled
					case "deadline":
						return "", "", context.DeadlineExceeded
					case "panic":
						panic(&selectorPanic{})
					case "unsafe":
						return "", "", &selectorOpaqueError{&called}
					case "mixed aliases":
						return "", "", errors.Join(&SelectorError{cause: secret},
							&di.Error{Kind: di.ErrCallbackPanicked, Panic: secret, Cause: secret}, ordinary)
					default:
						return "", "", ordinary
					}
				}
				switch category {
				case "canceled":
					want = context.Canceled
				case "deadline":
					want = context.DeadlineExceeded
				case "panic":
					want = ErrSelectorPanicked
				case "unsafe":
					want = errUnsafeDiagnostic
				case "mixed aliases":
					want = di.ErrCallbackPanicked
				}
				registry := chronicle.NewRegistry()
				if _, err := chronicle.RegisterEvent[selectorBoundaryEvent](registry); err != nil {
					t.Fatal(err)
				}
				if err := chronicle.RegisterReactor[*selectorBoundaryReactor](registry,
					func(ctx context.Context, scope reactors.Scope) (*selectorBoundaryReactor, error) {
						_, err := scope.Resolve(ctx, reflect.TypeFor[*chronicle.EventStore]())
						return nil, err
					}); err != nil {
					t.Fatal(err)
				}
				p, err := chronicle.CaptureClient(chronicle.WithRegistry(registry))
				if err != nil {
					t.Fatal(err)
				}
				var bindings container.Registry
				if err := BindClient(&bindings, p.Client()); err != nil {
					t.Fatal(err)
				}
				if err := BindEventStore(&bindings, selector); err != nil {
					t.Fatal(err)
				}
				provider, err := bindings.Build()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := p.Client().Close(); err != nil {
						t.Error(err)
					}
					if err := provider.Close(context.Background()); err != nil {
						t.Error(err)
					}
				})
				var client *chronicle.Client
				if mode == "PrepareClient" {
					client, err = PrepareClient(t.Context(), p, provider)
				} else {
					// A configured observer client may consume another client's
					// borrowed facade; this avoids a hidden late-assigned binding.
					if _, err := p.Prepare(t.Context(), nil); err != nil {
						t.Fatal(err)
					}
					client, err = chronicle.NewClient(chronicle.WithRegistry(registry), WithServices(provider))
					if err == nil {
						t.Cleanup(func() {
							if err := client.Close(); err != nil {
								t.Error(err)
							}
						})
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				plans, err := client.Artifacts("store")
				if err != nil {
					t.Fatal(err)
				}
				lease, err := plans.Reactors[0].Activate(t.Context())
				if lease != nil {
					if closeErr := lease.Close(t.Context()); closeErr != nil {
						t.Fatal(closeErr)
					}
				}
				var selection *SelectorError
				var diagnostic *di.Error
				if !errors.As(err, &selection) || !errors.As(err, &diagnostic) || !errors.Is(err, want) || called {
					t.Fatal("adapted factory lost safe wrapper/cause or invoked an unsafe hook")
				}
				if errors.Is(err, ordinary) != (category == "ordinary" || category == "mixed aliases") {
					t.Fatal("safe ordinary cause retention changed")
				}
				if errors.Is(err, secret) || formatted != 0 {
					t.Fatal("adapter retained/formatted a panic alias")
				}
			})
		}
	}
}

func TestSanitizerSnapshotsExactSelectorErrorAndPrivateCause(t *testing.T) {
	ordinary := errors.New("ordinary")
	original := &SelectorError{cause: ordinary}
	clean := sanitizeError(original)
	original.cause = &panickingUnwrap{}
	var selection *SelectorError
	if !errors.As(clean, &selection) || selection == original || !errors.Is(clean, ordinary) {
		t.Fatal("selector snapshot retained mutable wrapper or lost cause")
	}
	if clean := sanitizeError(&SelectorError{}); clean == nil {
		t.Fatal("zero selector failure disappeared")
	}
	// External callers can replace an exported value, but cannot mutate private
	// fields. Inspection snapshots synchronously; concurrent caller mutation of
	// an error graph is unsupported, not something Unwrap can safely synchronize.
}

func TestSanitizerSelectorPanicAliasQuarantineAcrossMixedGraphs(t *testing.T) {
	for _, panicFirst := range []bool{false, true} {
		for _, payloadWrapper := range []bool{false, true} {
			t.Run(fmt.Sprintf("panic-first=%t/payload-wrapper=%t", panicFirst, payloadWrapper), func(t *testing.T) {
				formatted := 0
				secret := &secretFailure{&formatted}
				selection := &SelectorError{cause: secret}
				var payload error = secret
				if payloadWrapper {
					payload = selection
				}
				panicNode := &di.Error{Operation: "factory", Kind: di.ErrCallbackPanicked, Panic: payload, Cause: payload}
				ordinary := errors.New("distinct ordinary")
				children := []error{selection, panicNode, ordinary}
				if panicFirst {
					children[0], children[1] = children[1], children[0]
				}
				clean := sanitizeError(errors.Join(children...))
				assertSafeProviderDiagnostics(t, clean)
				var retained *SelectorError
				if !errors.As(clean, &retained) || errors.Is(clean, secret) || errors.Is(clean, selection) || formatted != 0 {
					t.Fatal("selector wrapper bypassed global panic alias quarantine")
				}
				if !errors.Is(clean, ordinary) || !errors.Is(clean, di.ErrCallbackPanicked) {
					t.Fatal("mixed graph lost safe sibling/category")
				}
			})
		}
	}
}

func TestSanitizerSelectorBoundsCyclesAndLookalikes(t *testing.T) {
	cycle := &SelectorError{}
	cycle.cause = cycle
	providerCycle := &di.Error{}
	providerCycle.Cause = &SelectorError{cause: providerCycle}
	deep := error(errors.New("ordinary"))
	for range maxDiagnosticDepth {
		deep = &SelectorError{cause: deep}
	}
	wide := make([]error, maxDiagnosticNodes)
	for i := range wide {
		wide[i] = &SelectorError{}
	}
	calls := 0
	for _, tree := range []error{
		cycle, providerCycle, deep, errors.Join(wide...), (*SelectorError)(nil),
		&struct{ *SelectorError }{&SelectorError{cause: errors.New("ordinary")}},
		&SelectorError{cause: &hostileDiagnostic{calls: &calls}},
		&SelectorError{cause: &onlyAsDiagnostic{&calls}},
		&SelectorError{cause: &onlyIsDiagnostic{&calls}},
		&di.Error{Kind: di.ErrCallbackPanicked, Panic: context.DeadlineExceeded,
			Cause: &SelectorError{cause: context.DeadlineExceeded}},
	} {
		if clean := sanitizeError(tree); clean != errUnsafeDiagnostic || calls != 0 {
			t.Fatal("unsafe selector topology was inspected or partially retained")
		}
	}
	// Root plus the nil private-cause edge must both fit the rooted edge budget.
	for _, budget := range []int{1, 2} {
		walker := diagnosticWalker{remaining: budget, seen: make(map[diagnosticIdentity]*diagnosticNode), active: make(map[diagnosticIdentity]bool)}
		walker.capture(&SelectorError{}, 0)
		if walker.incomplete != (budget == 1) || walker.remaining != 0 {
			t.Fatal("selector edge budget changed")
		}
	}
}
