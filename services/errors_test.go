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
			formatted, calls := 0, 0
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
			// Drop wrapper identity, cached text and inspection hooks; preserve
			// the independent ordinary sibling and safe provider metadata.
			tree := &hostileDiagnostic{child: errors.Join(fmt.Errorf("secret panic payload: %w", original), ordinary), calls: &calls}
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
			if diagnostic.Path[0] != key || formatted != 0 || calls != 1 {
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

func TestSanitizerBoundsAndContainsHostileInspection(t *testing.T) {
	calls := 0
	cycle := &hostileDiagnostic{calls: &calls}
	cycle.child = cycle
	deep := error(errors.New("unreachable"))
	for i := 0; i < maxDiagnosticDepth+1; i++ {
		deep = &hostileDiagnostic{child: deep, calls: &calls}
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
			calls = 0
			ordinary := &ordinaryFailure{}
			clean := sanitizeError(errors.Join(ordinary, tree))
			assertSafeProviderDiagnostics(t, clean)
			if !errors.Is(clean, ordinary) {
				t.Fatal("independent ordinary cause lost")
			}
			if name == "panicking unwrap" {
				if !errors.Is(clean, di.ErrCallbackPanicked) {
					t.Fatal("inspection panic lost")
				}
			} else if !errors.Is(clean, errUnsafeDiagnostic) {
				t.Fatal("unsafe inspection was not diagnosed")
			}
			if calls > maxDiagnosticDepth {
				t.Fatal("unbounded inspection", calls)
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
