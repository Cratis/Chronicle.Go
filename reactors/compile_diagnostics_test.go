// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/reactors"
)

func TestCompilePreservesDeclarationDiagnostics(t *testing.T) {
	badResult, _ := reactors.Define[BadResult](func() BadResult { return BadResult{} })
	badParameter, _ := reactors.Define[BadParameter](func() BadParameter { return BadParameter{} })
	badFactory, _ := reactors.Define[BadResult](func(interface{ Missing() }) BadResult { return BadResult{} })
	nilMiddleware, _ := reactors.Define[*Handlers](func() *Handlers { return nil }, reactors.WithMiddleware(nil))
	wrongMiddleware := func() int { return 0 }
	badMiddleware, _ := reactors.Define[*Handlers](func() *Handlers { return nil }, reactors.WithMiddleware(wrongMiddleware))
	badPolicy, _ := reactors.Define[*Handlers](func() *Handlers { return nil }, reactors.Replay("unknown"))
	catalog, models := catalogs(t)
	for _, tc := range []struct {
		name        string
		declaration reactors.Declaration
		method      string
		parameter   reflect.Type
	}{
		{"result", badResult, "Invalid", reflect.TypeFor[Ordered]()},
		{"parameter", badParameter, "Invalid", reflect.TypeFor[interface{ Missing() }]()},
		{"constructor", badFactory, "constructor", reflect.TypeFor[interface{ Missing() }]()},
		{"nil middleware", nilMiddleware, "middleware", nil},
		{"middleware type", badMiddleware, "middleware", reflect.TypeOf(wrongMiddleware)},
		{"policy", badPolicy, "unknown", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := reactors.Compile(tc.declaration, catalog, models, nil)
			assertCompileDiagnostic(t, err, &reactors.DeclarationError{Reactor: tc.declaration.Identifier(), Method: tc.method, Parameter: tc.parameter})
		})
	}
}

func TestCompilePreservesCallbackDiagnostics(t *testing.T) {
	handler := reactors.On(func(context.Context, Ordered) error { return nil })
	catalog, models := catalogs(t)
	for _, tc := range []struct {
		name      string
		handlers  []reactors.Handler
		method    string
		parameter reflect.Type
	}{
		{"nil", []reactors.Handler{reactors.On[Ordered](nil)}, "callback[0]", nil},
		{"duplicate", []reactors.Handler{handler, handler}, "callback[1]", reflect.TypeFor[Ordered]()},
		{"unregistered", []reactors.Handler{reactors.On(func(context.Context, string) error { return nil })}, "callback[0]", reflect.TypeFor[string]()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			declaration, err := reactors.DefineHandlers("diagnostic", tc.handlers)
			if err != nil {
				t.Fatal(err)
			}
			_, err = reactors.Compile(declaration, catalog, models, nil)
			assertCompileDiagnostic(t, err, &reactors.DeclarationError{Reactor: declaration.Identifier(), Method: tc.method, Parameter: tc.parameter})
		})
	}
}

func assertCompileDiagnostic(t *testing.T, err error, want *reactors.DeclarationError) {
	t.Helper()
	var detail *reactors.DeclarationError
	if !errors.As(err, &detail) || !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("error = %v, want declaration configuration error", err)
	}
	if detail.Reactor != want.Reactor {
		t.Errorf("reactor = %s; want %s", detail.Reactor, want.Reactor)
	}
	if detail.Method != want.Method {
		t.Errorf("method = %s; want %s", detail.Method, want.Method)
	}
	if detail.Parameter != want.Parameter {
		t.Errorf("parameter = %v; want %v", detail.Parameter, want.Parameter)
	}
}
