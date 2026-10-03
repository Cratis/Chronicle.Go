// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services

import (
	"errors"
	"reflect"
	"slices"

	"github.com/cratis/fundamentals.go/dependencyinjection"
)

var errUnsafeDiagnostic = errors.New("chronicle services: provider error diagnostics unavailable")

const (
	maxDiagnosticDepth = 64
	maxDiagnosticNodes = 256
)

// sanitizeError never retains a provider panic or an ancestor that could expose
// it through formatting, As, Is or Unwrap. Only plain error leaves retain identity;
// wrappers are rebuilt, not copied. Known provider metadata is copied explicitly.
func sanitizeError(err error) error {
	walker := diagnosticWalker{remaining: maxDiagnosticNodes, active: make(map[error]bool)}
	return walker.clean(err, 0)
}

type diagnosticWalker struct {
	remaining int
	active    map[error]bool
}

func (w *diagnosticWalker) clean(err error, depth int) (result error) {
	// Inspection can itself invoke application Unwrap code. Recovery only drops
	// the value and constructs a constant diagnostic; it never inspects errors.
	defer func() {
		if recover() != nil {
			result = &dependencyinjection.Error{Operation: "inspect diagnostics", Kind: dependencyinjection.ErrCallbackPanicked}
		}
	}()
	if depth >= maxDiagnosticDepth || w.remaining == 0 {
		return errUnsafeDiagnostic
	}
	w.remaining--
	if err == nil {
		return nil
	}
	value := reflect.ValueOf(err)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return errUnsafeDiagnostic
	}
	if value.Comparable() {
		if w.active[err] {
			return errUnsafeDiagnostic
		}
		w.active[err] = true
		defer delete(w.active, err)
	}
	if diagnostic, ok := err.(*dependencyinjection.Error); ok {
		if diagnostic == nil {
			return errUnsafeDiagnostic
		}
		kind := safeKind(diagnostic.Kind)
		clean := &dependencyinjection.Error{
			Operation: safeOperation(diagnostic.Operation), Key: diagnostic.Key,
			Kind: kind,
		}
		if len(diagnostic.Path) <= maxDiagnosticDepth {
			clean.Path = slices.Clone(diagnostic.Path)
		}
		if diagnostic.Panic != nil || kind == dependencyinjection.ErrCallbackPanicked {
			// Cause/Kind can alias the recovered error too. Do not walk or retain
			// any cause of a panic-bearing node, even an apparently ordinary one.
			clean.Kind = dependencyinjection.ErrCallbackPanicked
			if kind != nil && kind != errUnsafeDiagnostic && kind != dependencyinjection.ErrCallbackPanicked {
				clean.Cause = kind
			}
			return clean
		}
		clean.Cause = w.clean(diagnostic.Cause, depth+1)
		if len(diagnostic.Path) > maxDiagnosticDepth {
			clean.Cause = errors.Join(clean.Cause, errUnsafeDiagnostic)
		}
		return clean
	}
	// Never call application As/Is, nor retain wrappers with cached payload text
	// or hidden diagnostics. Traverse Unwrap under the recovery boundary above.
	switch tree := err.(type) {
	case interface{ Unwrap() []error }:
		children := tree.Unwrap()
		var cleaned []error
		for _, child := range children {
			if w.remaining == 0 {
				cleaned = append(cleaned, errUnsafeDiagnostic)
				break
			}
			cleaned = append(cleaned, w.clean(child, depth+1))
		}
		if joined := errors.Join(cleaned...); joined != nil {
			return joined
		}
		return errUnsafeDiagnostic
	case interface{ Unwrap() error }:
		if child := w.clean(tree.Unwrap(), depth+1); child != nil {
			return child
		}
		return errUnsafeDiagnostic
	case interface{ As(any) bool }, interface{ Is(error) bool }:
		return errUnsafeDiagnostic
	default:
		return err
	}
}

// v0.1 provider categories are stable identities, never application messages.
func safeKind(kind error) error {
	switch kind {
	case nil,
		dependencyinjection.ErrWrongType, dependencyinjection.ErrFactoryFailed,
		dependencyinjection.ErrInvalidRegistration, dependencyinjection.ErrDuplicate,
		dependencyinjection.ErrMissing, dependencyinjection.ErrCycle,
		dependencyinjection.ErrCaptiveLifetime, dependencyinjection.ErrUndeclaredDependency,
		dependencyinjection.ErrClosed, dependencyinjection.ErrInvalidScope,
		dependencyinjection.ErrContextMismatch, dependencyinjection.ErrResolverExpired,
		dependencyinjection.ErrConcurrentFactoryUse, dependencyinjection.ErrNilValue,
		dependencyinjection.ErrCallbackPanicked:
		return kind
	default:
		return errUnsafeDiagnostic
	}
}

func safeOperation(operation string) string {
	switch operation {
	case "key", "binding", "bind", "bind value", "bind function", "bind scope factory",
		"register", "build", "resolve", "factory", "close", "close scope", "close provider",
		"new-scope", "capture context", "check context", "inspect diagnostics":
		return operation
	default:
		return "provider"
	}
}
