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

// sanitizeError snapshots the complete bounded graph before retaining any leaves.
// A panic payload can also be an ordinary sibling (in either order), so sanitizing
// individual branches independently cannot decide which identities are safe.
func sanitizeError(err error) (result error) {
	// Custom Unwrap is the only application hook used during inspection. If it
	// panics, the unfinished graph might hide aliases of any leaf: discard it all.
	defer func() {
		if recover() != nil {
			result = &dependencyinjection.Error{Operation: "inspect diagnostics", Kind: dependencyinjection.ErrCallbackPanicked}
		}
	}()
	walker := diagnosticWalker{
		remaining: maxDiagnosticNodes, seen: make(map[diagnosticIdentity]*diagnosticNode),
		active: make(map[diagnosticIdentity]bool), quarantined: make(map[uintptr]bool),
		quarantinedNodes: make(map[*diagnosticNode]bool), rebuilt: make(map[*diagnosticNode]bool),
	}
	snapshot := walker.capture(err, 0)
	if walker.incomplete {
		// Truncation/cycles could conceal a panic alias. Do not preserve a leaf
		// merely because it happened to occur before the inspection boundary.
		return errUnsafeDiagnostic
	}
	return walker.rebuild(snapshot)
}

type diagnosticIdentity struct {
	typ reflect.Type
	ptr uintptr
}

type diagnosticNode struct {
	identity   diagnosticIdentity
	leaf       error
	diagnostic *dependencyinjection.Error
	children   []*diagnosticNode
}

type diagnosticWalker struct {
	remaining        int
	seen             map[diagnosticIdentity]*diagnosticNode
	active           map[diagnosticIdentity]bool
	quarantined      map[uintptr]bool
	quarantinedNodes map[*diagnosticNode]bool
	rebuilt          map[*diagnosticNode]bool
	incomplete       bool
	ambiguous        bool
}

// referenceIdentity uses address identity, never structural comparison or an
// application's Error/As/Is hooks. Value errors have no safe reference identity.
func referenceIdentity(value any) diagnosticIdentity {
	reflected := reflect.ValueOf(value)
	if reflected.IsValid() && reflected.Kind() == reflect.Pointer && !reflected.IsNil() {
		return diagnosticIdentity{typ: reflected.Type(), ptr: reflected.Pointer()}
	}
	return diagnosticIdentity{}
}

func (w *diagnosticWalker) capture(err error, depth int) *diagnosticNode {
	if err == nil {
		return nil
	}
	if depth >= maxDiagnosticDepth || w.remaining == 0 {
		w.incomplete = true
		return nil
	}
	w.remaining--
	identity := referenceIdentity(err)
	if identity.ptr != 0 {
		if w.active[identity] {
			w.incomplete = true
			return nil
		}
		if existing, ok := w.seen[identity]; ok {
			return existing
		}
		w.active[identity] = true
		defer delete(w.active, identity)
	}
	node := &diagnosticNode{identity: identity}
	if identity.ptr != 0 {
		w.seen[identity] = node
	}
	value := reflect.ValueOf(err)
	if value.Kind() == reflect.Pointer && value.IsNil() {
		return node
	}
	if diagnostic, ok := err.(*dependencyinjection.Error); ok {
		kind := safeKind(diagnostic.Kind)
		node.diagnostic = &dependencyinjection.Error{Operation: safeOperation(diagnostic.Operation), Key: diagnostic.Key, Kind: kind}
		if len(diagnostic.Path) <= maxDiagnosticDepth {
			node.diagnostic.Path = slices.Clone(diagnostic.Path)
		}
		if diagnostic.Panic != nil || kind == dependencyinjection.ErrCallbackPanicked {
			// Quarantine payloads AND original causes across the entire snapshot,
			// including ancestors and ordinary siblings captured earlier or later.
			if payload := referenceIdentity(diagnostic.Panic); payload.ptr != 0 {
				w.quarantined[payload.ptr] = true
			}
			if payload, ok := diagnostic.Panic.(error); ok {
				w.quarantine(w.capture(payload, depth+1))
			}
			w.quarantine(w.capture(diagnostic.Cause, depth+1))
			if kind == errUnsafeDiagnostic {
				w.quarantine(w.capture(diagnostic.Kind, depth+1))
			}
			node.diagnostic.Kind = dependencyinjection.ErrCallbackPanicked
			if kind != nil && kind != errUnsafeDiagnostic && kind != dependencyinjection.ErrCallbackPanicked {
				node.children = []*diagnosticNode{{leaf: kind}}
			}
			return node
		}
		node.children = []*diagnosticNode{w.capture(diagnostic.Cause, depth+1)}
		if len(diagnostic.Path) > maxDiagnosticDepth {
			node.children = append(node.children, &diagnosticNode{leaf: errUnsafeDiagnostic})
		}
		return node
	}
	switch tree := err.(type) {
	case interface{ Unwrap() []error }:
		for _, child := range tree.Unwrap() {
			if w.remaining == 0 {
				w.incomplete = true
				break
			}
			node.children = append(node.children, w.capture(child, depth+1))
		}
	case interface{ Unwrap() error }:
		node.children = []*diagnosticNode{w.capture(tree.Unwrap(), depth+1)}
	case interface{ As(any) bool }, interface{ Is(error) bool }:
		// Never keep a wrapper with hidden inspection hooks.
	default:
		if identity.ptr != 0 {
			node.leaf = err
		}
	}
	return node
}

func (w *diagnosticWalker) quarantine(node *diagnosticNode) {
	if node == nil || w.quarantinedNodes[node] {
		return
	}
	w.quarantinedNodes[node] = true
	if node.identity.ptr == 0 {
		// An error value without reference identity might contain hidden aliases.
		w.ambiguous = true
	} else {
		w.quarantined[node.identity.ptr] = true
	}
	for _, child := range node.children {
		w.quarantine(child)
	}
}

func (w *diagnosticWalker) safeLeaf(err error) error {
	identity := referenceIdentity(err)
	if w.ambiguous || identity.ptr == 0 || w.quarantined[identity.ptr] {
		return errUnsafeDiagnostic
	}
	return err
}

func (w *diagnosticWalker) rebuild(node *diagnosticNode) error {
	if node == nil || w.rebuilt[node] {
		return nil
	}
	// Emit a shared subtree only once. Recreating a DAG would make subsequent
	// errors.Is/As and formatting potentially exponential despite bounded input.
	w.rebuilt[node] = true
	if node.leaf != nil {
		return w.safeLeaf(node.leaf)
	}
	children := make([]error, 0, len(node.children))
	for _, child := range node.children {
		children = append(children, w.rebuild(child))
	}
	cause := errors.Join(children...)
	if node.diagnostic != nil {
		if node.diagnostic.Kind != nil {
			node.diagnostic.Kind = w.safeLeaf(node.diagnostic.Kind)
		}
		node.diagnostic.Cause = cause
		return node.diagnostic
	}
	if cause != nil {
		return cause
	}
	return errUnsafeDiagnostic
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
