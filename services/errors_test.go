// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	di "github.com/cratis/fundamentals.go/dependencyinjection"
)

type ordinaryFailure struct{}

func (*ordinaryFailure) Error() string { return "ordinary failure" }

type secretFailure struct{ formatted *int }

func (e *secretFailure) Error() string { (*e.formatted)++; return "secret panic payload" }

type hostileDiagnostic struct {
	child error
	calls *int
}

func (*hostileDiagnostic) Error() string { panic("Error must not run") }
func (e *hostileDiagnostic) Unwrap() error {
	(*e.calls)++
	return e.child
}
func (*hostileDiagnostic) As(any) bool   { panic("As must not run") }
func (*hostileDiagnostic) Is(error) bool { panic("Is must not run") }

type panickingUnwrap struct{}

func (panickingUnwrap) Error() string { panic("Error must not run") }
func (panickingUnwrap) Unwrap() error { panic("secret panic payload") }

type panickingAs struct{}

func (panickingAs) Error() string { panic("Error must not run") }
func (panickingAs) As(any) bool   { panic("As must not run") }

type noncomparableCycle []error

func (e noncomparableCycle) Error() string { panic("Error must not run") }
func (e noncomparableCycle) Unwrap() error { return e }

func TestSanitizerRebuildsProviderPanicTreesWithoutRetainingPayloads(t *testing.T) {
	for _, kind := range []string{"string", "object", "error"} {
		t.Run(kind, func(t *testing.T) {
			formatted := 0
			secret := &secretFailure{&formatted}
			var payload any = "secret panic payload"
			switch kind {
			case "object":
				payload = &struct{ Secret string }{"secret panic payload"}
			case "error":
				payload = secret
			}
			ordinary := &ordinaryFailure{}
			key := di.KeyFor[*ordinaryFailure]()
			original := &di.Error{Operation: "factory", Key: key, Path: []di.Key{key}, Kind: di.ErrFactoryFailed, Panic: payload, Cause: secret}
			// Drop standard wrapper identity and cached text; preserve the
			// independent ordinary sibling and safe provider metadata.
			tree := errors.Join(fmt.Errorf("secret panic payload: %w", original), ordinary)
			clean := sanitizeError(tree)
			assertSafeProviderDiagnostics(t, clean)
			var diagnostic *di.Error
			var cause *ordinaryFailure
			if !errors.As(clean, &diagnostic) || !errors.As(clean, &cause) || cause != ordinary || !errors.Is(clean, ordinary) {
				t.Fatal("lost safe diagnostics/ordinary cause", clean)
			}
			if !errors.Is(clean, di.ErrCallbackPanicked) || !errors.Is(clean, di.ErrFactoryFailed) || errors.Is(clean, original) || errors.Is(clean, secret) {
				t.Fatal("lost category or retained original diagnostic")
			}
			if diagnostic.Operation != "factory" || diagnostic.Key != key || !slices.Equal(diagnostic.Path, []di.Key{key}) {
				t.Fatal("lost safe service metadata")
			}
			original.Path[0] = di.KeyFor[string]()
			if diagnostic.Path[0] != key || formatted != 0 {
				t.Fatal("retained path alias or evaluated application formatting/inspection")
			}
		})
	}
}

func TestSanitizerPreservesOrdinaryProviderCausesAndLifetimeCategories(t *testing.T) {
	ordinary := &ordinaryFailure{}
	original := &di.Error{Operation: "resolve", Kind: di.ErrCaptiveLifetime, Cause: fmt.Errorf("context: %w", ordinary)}
	clean := sanitizeError(original)
	var diagnostic *di.Error
	var cause *ordinaryFailure
	if !errors.Is(clean, ordinary) || !errors.Is(clean, di.ErrCaptiveLifetime) || !errors.As(clean, &cause) || cause != ordinary || !errors.As(clean, &diagnostic) || diagnostic == original {
		t.Fatal("safe error identity or category lost", clean)
	}
}

func TestSanitizerBoundsAndRefusesOpaqueInspection(t *testing.T) {
	cycle := &di.Error{}
	cycle.Cause = cycle
	deep := error(errors.New("unreachable"))
	for range maxDiagnosticDepth + 1 {
		deep = &di.Error{Cause: deep}
	}
	wide := make([]error, maxDiagnosticNodes+1)
	for i := range wide {
		wide[i] = errors.New("ordinary")
	}
	for name, tree := range map[string]error{
		"cycle": cycle, "noncomparable cycle": noncomparableCycle{}, "depth": deep,
		"width": errors.Join(wide...), "panicking unwrap": panickingUnwrap{}, "panicking As": panickingAs{},
	} {
		t.Run(name, func(t *testing.T) {
			ordinary := &ordinaryFailure{}
			clean := sanitizeError(errors.Join(ordinary, tree))
			assertSafeProviderDiagnostics(t, clean)
			if clean != errUnsafeDiagnostic || errors.Is(clean, ordinary) {
				t.Fatal("unsafe graph retained a partial snapshot")
			}
		})
	}
}

func TestSanitizerDiscardsPanicCauseEvenWithoutPayloadAndRejectsUnsafeMetadata(t *testing.T) {
	formatted := 0
	secret := &secretFailure{&formatted}
	for _, diagnostic := range []*di.Error{
		{Operation: "secret panic payload", Kind: di.ErrCallbackPanicked, Cause: secret},
		{Operation: "factory", Kind: secret, Cause: secret, Panic: secret},
		{Operation: "factory", Kind: di.ErrFactoryFailed, Cause: &di.Error{Kind: di.ErrCallbackPanicked, Panic: secret, Cause: secret}},
	} {
		clean := sanitizeError(diagnostic)
		assertSafeProviderDiagnostics(t, clean)
		if !errors.Is(clean, di.ErrCallbackPanicked) || errors.Is(clean, secret) || formatted != 0 {
			t.Fatal("retained or formatted panic cause")
		}
	}
}

func TestSanitizerQuarantinesPanicAliasesAcrossBothSiblingOrders(t *testing.T) {
	for _, panicFirst := range []bool{false, true} {
		for _, wrapped := range []bool{false, true} {
			t.Run(fmt.Sprintf("panic-first=%t/wrapped=%t", panicFirst, wrapped), func(t *testing.T) {
				formatted := 0
				secret := &secretFailure{&formatted}
				ordinary := &ordinaryFailure{}
				var alias error = secret
				if wrapped {
					alias = &di.Error{Kind: di.ErrFactoryFailed, Cause: secret}
				}
				panicNode := &di.Error{Kind: di.ErrCallbackPanicked, Panic: alias, Cause: alias}
				children := []error{alias, panicNode, ordinary}
				if panicFirst {
					children[0], children[1] = children[1], children[0]
				}
				clean := sanitizeError(errors.Join(children...))
				assertSafeProviderDiagnostics(t, clean)
				var leaked *secretFailure
				if errors.Is(clean, secret) || errors.Is(clean, alias) || errors.As(clean, &leaked) || formatted != 0 {
					t.Fatal("panic payload remains reachable through an ordinary sibling")
				}
				if !errors.Is(clean, ordinary) || !errors.Is(clean, di.ErrCallbackPanicked) {
					t.Fatal("distinct ordinary cause or panic category lost")
				}
			})
		}
	}
}

func TestSanitizerEmitsSharedDiagnosticSubtreesOnce(t *testing.T) {
	ordinary := &ordinaryFailure{}
	var tree error = ordinary
	for range 40 {
		tree = errors.Join(tree, tree)
	}
	clean := sanitizeError(tree)
	if !errors.Is(clean, ordinary) || errors.Is(clean, errors.New("absent")) {
		t.Fatal("shared diagnostic identity lost")
	}
	assertSafeProviderDiagnostics(t, clean)
}

type noncomparableFailure []byte

func (noncomparableFailure) Error() string { panic("Error must not run") }

func TestSanitizerDiscardsAmbiguousPanicIdentity(t *testing.T) {
	secret := noncomparableFailure("secret panic payload")
	clean := sanitizeError(errors.Join(secret, &di.Error{Kind: di.ErrCallbackPanicked, Panic: secret}))
	assertSafeProviderDiagnostics(t, clean)
	var leaked noncomparableFailure
	if errors.As(clean, &leaked) || !errors.Is(clean, errUnsafeDiagnostic) {
		t.Fatal("ambiguous panic identity was retained")
	}
}

type diagnosticChildren struct {
	children []error
	calls    *int
}

func (diagnosticChildren) Error() string      { panic("Error must not run") }
func (e *diagnosticChildren) Unwrap() []error { (*e.calls)++; return e.children }

// errors.Join discards nil arguments, but its exposed child slice can contain
// nil slots. Populate it before inspection to exercise the admitted nil budget.
func standardDiagnosticSlots(children ...error) error {
	seeds := slices.Clone(children)
	for i, child := range seeds {
		if child == nil {
			seeds[i] = errUnsafeDiagnostic
		}
	}
	tree := errors.Join(seeds...)
	copy(tree.(interface{ Unwrap() []error }).Unwrap(), children)
	return tree
}

func TestSanitizerDiscoversPanicsThroughOriginalKindEdges(t *testing.T) {
	for _, panicFirst := range []bool{false, true} {
		for _, wrappedKind := range []bool{false, true} {
			t.Run(fmt.Sprintf("panic-first=%t/wrapped-kind=%t", panicFirst, wrappedKind), func(t *testing.T) {
				formatted := 0
				secret := &secretFailure{&formatted}
				ordinary := errors.New("ordinary sibling")
				panicNode := &di.Error{Kind: di.ErrCallbackPanicked, Panic: secret}
				var originalKind error = panicNode
				if wrappedKind {
					originalKind = fmt.Errorf("secret panic payload: %w", panicNode)
				}
				outer := &di.Error{Kind: originalKind, Cause: &di.Error{Kind: di.ErrFactoryFailed}}
				children := []error{secret, outer, ordinary}
				if panicFirst {
					children[0], children[1] = children[1], children[0]
				}
				clean := sanitizeError(errors.Join(children...))
				assertSafeProviderDiagnostics(t, clean)
				var leaked *secretFailure
				if errors.Is(clean, secret) || errors.As(clean, &leaked) || formatted != 0 {
					t.Fatal("original Kind hid a panic payload alias")
				}
				if !errors.Is(clean, ordinary) || !errors.Is(clean, di.ErrCallbackPanicked) || !errors.Is(clean, di.ErrFactoryFailed) {
					t.Fatal("distinct ordinary sibling or safe category lost")
				}
			})
		}
	}
}

func TestSanitizerQuarantinesOriginalPayloadKindAndCauseTopology(t *testing.T) {
	for _, panicFirst := range []bool{false, true} {
		for _, edge := range []string{"Kind", "Cause"} {
			t.Run(fmt.Sprintf("panic-first=%t/edge=%s", panicFirst, edge), func(t *testing.T) {
				formatted := 0
				secret := &secretFailure{&formatted}
				ordinary := errors.New("ordinary sibling")
				payload := &di.Error{}
				if edge == "Kind" {
					payload.Kind = &di.Error{Kind: secret}
				} else {
					payload.Cause = &di.Error{Cause: secret}
				}
				panicNode := &di.Error{Kind: di.ErrFactoryFailed, Panic: payload}
				children := []error{secret, panicNode, ordinary}
				if panicFirst {
					children[0], children[1] = children[1], children[0]
				}
				clean := sanitizeError(errors.Join(children...))
				assertSafeProviderDiagnostics(t, clean)
				var leaked *secretFailure
				if errors.Is(clean, secret) || errors.Is(clean, payload) || errors.As(clean, &leaked) || formatted != 0 {
					t.Fatal("original panic topology leaked a sibling alias")
				}
				if !errors.Is(clean, ordinary) || !errors.Is(clean, di.ErrCallbackPanicked) || !errors.Is(clean, di.ErrFactoryFailed) {
					t.Fatal("distinct ordinary sibling or safe category lost")
				}
			})
		}
	}
}

func TestSanitizerRejectsOriginalKindAndCauseCycles(t *testing.T) {
	for _, edge := range []string{"Kind", "Cause"} {
		t.Run(edge, func(t *testing.T) {
			cycle := &di.Error{Kind: di.ErrFactoryFailed}
			if edge == "Kind" {
				cycle.Kind = cycle
			} else {
				cycle.Cause = cycle
			}
			ordinary := errors.New("ordinary sibling")
			clean := sanitizeError(errors.Join(ordinary, cycle))
			assertSafeProviderDiagnostics(t, clean)
			if clean != errUnsafeDiagnostic || errors.Is(clean, ordinary) {
				t.Fatal("cycle did not discard the entire snapshot")
			}
		})
	}
}

func TestSanitizerChargesNilEdgesAndRefusesWholeOversizedSnapshots(t *testing.T) {
	ordinary := errors.New("ordinary sibling")
	for _, size := range []int{maxDiagnosticNodes - 1, maxDiagnosticNodes, maxDiagnosticNodes * 100} {
		t.Run(fmt.Sprintf("slots=%d", size), func(t *testing.T) {
			children := make([]error, size)
			children[size-1] = ordinary
			clean := sanitizeError(standardDiagnosticSlots(children...))
			assertSafeProviderDiagnostics(t, clean)
			if errors.Is(clean, ordinary) != (size == maxDiagnosticNodes-1) {
				t.Fatal("root and nil edges were not charged exactly")
			}
		})
	}
	storm := standardDiagnosticSlots(make([]error, maxDiagnosticNodes*100)...)
	for _, stormFirst := range []bool{false, true} {
		children := []error{ordinary, storm}
		if stormFirst {
			children[0], children[1] = children[1], children[0]
		}
		if clean := sanitizeError(errors.Join(children...)); clean != errUnsafeDiagnostic {
			t.Fatal("nil-only storm retained a partial ordinary sibling")
		}
	}
}

func TestDiagnosticCaptureAcceptsExactSmallRootedEdgeBudgets(t *testing.T) {
	ordinary := errors.New("ordinary sibling")
	mixed := standardDiagnosticSlots(nil, ordinary, nil)
	nilChild := standardDiagnosticSlots(nil)
	for _, tc := range []struct {
		name   string
		tree   error
		budget int
	}{
		{"mixed nils", mixed, 4},
		{"nil-only list", nilChild, 2},
		{"repeated join edges", errors.Join(ordinary, ordinary), 3},
		{"provider nil fields", &di.Error{}, 3},
		{"provider nil payload topology", &di.Error{Kind: di.ErrFactoryFailed, Panic: nilChild}, 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, budget := range []int{tc.budget - 1, tc.budget} {
				walker := diagnosticWalker{
					remaining: budget, seen: make(map[diagnosticIdentity]*diagnosticNode),
					active: make(map[diagnosticIdentity]bool), quarantined: make(map[uintptr]bool),
				}
				walker.capture(tc.tree, 0)
				if walker.incomplete != (budget < tc.budget) || (!walker.incomplete && walker.remaining != 0) {
					t.Fatalf("budget=%d remaining=%d incomplete=%t", budget, walker.remaining, walker.incomplete)
				}
			}
		})
	}
}

func TestSanitizerChecksStandardEdgeListBudgetBeforeInspectingChildren(t *testing.T) {
	children := make([]error, maxDiagnosticNodes)
	children[0] = &di.Error{}
	walker := diagnosticWalker{
		remaining: maxDiagnosticNodes, seen: make(map[diagnosticIdentity]*diagnosticNode),
		active: make(map[diagnosticIdentity]bool),
	}
	walker.capture(standardDiagnosticSlots(children...), 0)
	if !walker.incomplete || len(walker.seen) != 1 {
		t.Fatal("oversized standard edge list was partially captured")
	}
}

func TestSanitizerAcceptsExactDepthLimit(t *testing.T) {
	for _, depth := range []int{maxDiagnosticDepth - 1, maxDiagnosticDepth} {
		ordinary := errors.New("ordinary sibling")
		tree := ordinary
		for range depth {
			tree = fmt.Errorf("context: %w", tree)
		}
		clean := sanitizeError(tree)
		if errors.Is(clean, ordinary) != (depth < maxDiagnosticDepth) {
			t.Fatal("incorrect depth boundary", depth)
		}
	}
}

func TestSanitizerPreservesDistinctOrdinaryIdentityAfterCategoryOnlyEmission(t *testing.T) {
	first, second := errors.New("ordinary first"), errors.New("ordinary second")
	kind := errors.Join(di.ErrMissing, first)
	clean := sanitizeError(errors.Join(&di.Error{Kind: kind}, first, second))
	assertSafeProviderDiagnostics(t, clean)
	if !errors.Is(clean, first) || !errors.Is(clean, second) || !errors.Is(clean, di.ErrMissing) {
		t.Fatal("category-only rebuild hid distinct safe ordinary identities")
	}
}

func TestSanitizerRefusesOpaqueOriginalKindAndPayloadGraphs(t *testing.T) {
	for _, tree := range []error{
		&di.Error{Kind: panickingUnwrap{}},
		&di.Error{Kind: di.ErrFactoryFailed, Panic: &di.Error{Kind: panickingUnwrap{}}},
	} {
		ordinary := errors.New("ordinary sibling")
		clean := sanitizeError(errors.Join(ordinary, tree))
		assertSafeProviderDiagnostics(t, clean)
		if clean != errUnsafeDiagnostic || errors.Is(clean, ordinary) {
			t.Fatal("opaque inspection retained an unfinished snapshot")
		}
	}
}

type mutatingUnwrap struct {
	target *di.Error
	calls  *int
}

func (*mutatingUnwrap) Error() string { panic("Error must not run") }
func (e *mutatingUnwrap) Unwrap() error {
	(*e.calls)++
	e.target.Panic = nil
	e.target.Kind = nil
	e.target.Cause = nil
	e.target.Operation = "changed"
	e.target.Key = di.KeyFor[string]()
	e.target.Path[0] = di.KeyFor[string]()
	return nil
}

func TestSanitizerNeverInvokesHooksThatMutateProviderDiagnostics(t *testing.T) {
	for _, hookInCause := range []bool{false, true} {
		for _, providerFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("cause-hook=%t/provider-first=%t", hookInCause, providerFirst), func(t *testing.T) {
				formatted, calls := 0, 0
				secret := &secretFailure{&formatted}
				key := di.KeyFor[*ordinaryFailure]()
				provider := &di.Error{Operation: "factory", Key: key, Path: []di.Key{key}, Kind: di.ErrFactoryFailed, Panic: secret}
				hook := &mutatingUnwrap{provider, &calls}
				var sibling error = hook
				if hookInCause {
					provider.Cause = hook
					sibling = secret
				}
				children := []error{sibling, provider, secret, errors.New("distinct ordinary")}
				if providerFirst {
					children[0], children[1] = children[1], children[0]
				}
				clean := sanitizeError(errors.Join(children...))
				assertSafeProviderDiagnostics(t, clean)
				if clean != errUnsafeDiagnostic || errors.Is(clean, secret) || calls != 0 || formatted != 0 {
					t.Fatal("mutating hook ran or exposed a secret alias")
				}
				if provider.Panic != secret || provider.Kind != di.ErrFactoryFailed || provider.Operation != "factory" || provider.Key != key || provider.Path[0] != key {
					t.Fatal("inspection changed original provider fields")
				}
			})
		}
	}
}

type onlyAsDiagnostic struct{ calls *int }

func (*onlyAsDiagnostic) Error() string { panic("Error must not run") }
func (e *onlyAsDiagnostic) As(any) bool { (*e.calls)++; return true }

type onlyIsDiagnostic struct{ calls *int }

func (*onlyIsDiagnostic) Error() string   { panic("Error must not run") }
func (e *onlyIsDiagnostic) Is(error) bool { (*e.calls)++; return true }

// The name matches a standard-library type, but the package identity does not.
type joinError struct{ calls *int }

func (*joinError) Error() string     { panic("Error must not run") }
func (e *joinError) Unwrap() []error { (*e.calls)++; return nil }

type providerDiagnostic = di.Error

func TestSanitizerRefusesOpaqueRootsWithoutCallingInspectionHooksOrLosingFailure(t *testing.T) {
	calls := 0
	for name, tree := range map[string]error{
		"unwrap":                    &hostileDiagnostic{calls: &calls},
		"As-only":                   &onlyAsDiagnostic{&calls},
		"Is-only":                   &onlyIsDiagnostic{&calls},
		"comparable As value":       panickingAs{},
		"standard type lookalike":   &joinError{&calls},
		"promoted provider methods": &struct{ *providerDiagnostic }{&di.Error{Kind: di.ErrFactoryFailed}},
		"small custom nil list":     &diagnosticChildren{[]error{nil}, &calls},
		"custom nil storm":          &diagnosticChildren{make([]error, maxDiagnosticNodes*100), &calls},
	} {
		t.Run(name, func(t *testing.T) {
			calls = 0
			if clean := sanitizeError(tree); clean != errUnsafeDiagnostic || calls != 0 {
				t.Fatal("opaque non-nil failure was lost or inspected", calls)
			}
		})
	}
}

type comparableValueFailure struct{}

func (comparableValueFailure) Error() string { panic("Error must not run") }

func TestSanitizerDiscardsEntireSnapshotForComparableValuePanicIdentity(t *testing.T) {
	for _, payload := range []error{comparableValueFailure{}, panickingAs{}} {
		ordinary := errors.New("distinct ordinary")
		provider := &di.Error{Operation: "factory", Kind: di.ErrFactoryFailed, Panic: payload}
		clean := sanitizeError(errors.Join(ordinary, provider))
		var diagnostic *di.Error
		if clean != errUnsafeDiagnostic || errors.As(clean, &diagnostic) || errors.Is(clean, ordinary) {
			t.Fatal("ambiguous panic retained a partially rebuilt provider diagnostic")
		}
	}
}

func TestSanitizerPreservesBothStandardFmtWrapperForms(t *testing.T) {
	first, second := errors.New("ordinary first"), errors.New("ordinary second")
	for _, tree := range []error{
		fmt.Errorf("secret panic payload: %w", errors.Join(first, second)),
		fmt.Errorf("secret panic payload: %w / %w", first, second),
	} {
		clean := sanitizeError(tree)
		assertSafeProviderDiagnostics(t, clean)
		if !errors.Is(clean, first) || !errors.Is(clean, second) || errors.Is(clean, tree) {
			t.Fatal("standard wrapper retained cached text/identity or lost ordinary leaves")
		}
	}
}

func TestSanitizerKeepsReturnedOrdinarySecretsWithoutKnownPanicAliases(t *testing.T) {
	ordinary := errors.New("ordinary returned secret")
	clean := sanitizeError(&di.Error{Kind: di.ErrFactoryFailed, Cause: ordinary})
	if !errors.Is(clean, ordinary) || !errors.Is(clean, di.ErrFactoryFailed) {
		t.Fatal("ordinary returned causes must remain inspectable")
	}
}

func assertSafeProviderDiagnostics(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("missing diagnostic")
	}
	if strings.Contains(fmt.Sprintf("%v %+v %#v %s", err, err, err, err), "secret panic payload") {
		t.Fatal("diagnostic exposes secret text")
	}
	var diagnostic *di.Error
	if errors.As(err, &diagnostic) && diagnostic.Panic != nil {
		t.Fatal("public errors.As exposes a recovered payload")
	}
	switch tree := err.(type) {
	case *di.Error:
		if tree.Panic != nil {
			t.Fatal("provider diagnostic retains a panic")
		}
		if tree.Cause != nil {
			assertSafeProviderDiagnostics(t, tree.Cause)
		}
		if tree.Kind != nil {
			assertSafeProviderDiagnostics(t, tree.Kind)
		}
	case interface{ Unwrap() []error }:
		for _, child := range tree.Unwrap() {
			assertSafeProviderDiagnostics(t, child)
		}
	case interface{ Unwrap() error }:
		if child := tree.Unwrap(); child != nil {
			assertSafeProviderDiagnostics(t, child)
		}
	}
}
