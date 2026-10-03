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
	maxDiagnosticNodes = 256 // Rooted edges, including nils and repeated references.
)

// sanitizeError snapshots the complete bounded graph before retaining any leaves.
// A panic payload can also be an ordinary sibling (in either order), so sanitizing
// individual branches independently cannot decide which identities are safe.
func sanitizeError(err error) (result error) {
	// Only known standard-library Unwrap methods are called. Contain any
	// unexpected inspection failure without retaining an unfinished snapshot.
	defer func() {
		if recover() != nil {
			result = &dependencyinjection.Error{Operation: "inspect diagnostics", Kind: dependencyinjection.ErrCallbackPanicked}
		}
	}()
	walker := diagnosticWalker{
		remaining: maxDiagnosticNodes, seen: make(map[diagnosticIdentity]*diagnosticNode),
		active: make(map[diagnosticIdentity]bool), quarantined: make(map[uintptr]bool),
		quarantinedNodes: make(map[*diagnosticNode]bool), rebuilt: make(map[diagnosticEmission]bool),
	}
	snapshot := walker.capture(err, 0)
	if walker.incomplete {
		// Truncation/cycles could conceal a panic alias. Do not preserve a leaf
		// merely because it happened to occur before the inspection boundary.
		return errUnsafeDiagnostic
	}
	for _, payload := range walker.quarantineRoots {
		walker.quarantine(payload)
	}
	if walker.ambiguous {
		return errUnsafeDiagnostic
	}
	return walker.rebuild(snapshot, false)
}

type diagnosticIdentity struct {
	typ reflect.Type
	ptr uintptr
	// Keep pointers alive during inspection so GC cannot recycle their addresses.
	// Only pointer values enter this field, making map-key equality reference-safe.
	reference any
}

type diagnosticNode struct {
	identity    diagnosticIdentity
	leaf        error
	diagnostic  *dependencyinjection.Error
	children    []*diagnosticNode
	kind        *diagnosticNode
	cause       *diagnosticNode
	panicked    bool
	category    error
	pathTooLong bool
}

type diagnosticEmission struct {
	node           *diagnosticNode
	categoriesOnly bool
}

type diagnosticWalker struct {
	// remaining counts rooted edges, including nils and repeated references,
	// not just allocated nodes. The root itself consumes one edge.
	remaining        int
	seen             map[diagnosticIdentity]*diagnosticNode
	active           map[diagnosticIdentity]bool
	quarantined      map[uintptr]bool
	quarantinedNodes map[*diagnosticNode]bool
	quarantineRoots  []*diagnosticNode
	rebuilt          map[diagnosticEmission]bool
	incomplete       bool
	ambiguous        bool
}

// referenceIdentity uses address identity, never structural comparison or an
// application's Error/As/Is hooks. Value errors have no safe reference identity.
func referenceIdentity(value any) diagnosticIdentity {
	reflected := reflect.ValueOf(value)
	if reflected.IsValid() && reflected.Kind() == reflect.Pointer && !reflected.IsNil() {
		return diagnosticIdentity{typ: reflected.Type(), ptr: reflected.Pointer(), reference: value}
	}
	return diagnosticIdentity{}
}

func (w *diagnosticWalker) capture(err error, depth int) *diagnosticNode {
	if w.incomplete || w.remaining == 0 {
		w.incomplete = true
		return nil
	}
	w.remaining--
	if err == nil {
		return nil
	}
	if depth >= maxDiagnosticDepth {
		w.incomplete = true
		return nil
	}
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
		w.incomplete = true
		return nil
	}
	if !value.Comparable() {
		// Unsupported value identities could hide aliases or cycles.
		w.incomplete = true
		return nil
	}
	if diagnostic, ok := err.(*dependencyinjection.Error); ok {
		// Snapshot ALL original fields and copy retained metadata before visiting
		// any child. Never reread the mutable provider diagnostic after descent.
		original := *diagnostic
		node.pathTooLong = len(original.Path) > maxDiagnosticDepth
		if !node.pathTooLong {
			original.Path = slices.Clone(original.Path)
		} else {
			original.Path = nil
		}
		kind := safeKind(original.Kind)
		node.category = kind
		node.diagnostic = &dependencyinjection.Error{
			Operation: safeOperation(original.Operation), Key: original.Key, Path: original.Path, Kind: kind,
		}
		// Fundamentals Error.Unwrap exposes BOTH original Kind and Cause.
		// Capture them even when output category selection rejects the Kind.
		node.kind = w.capture(original.Kind, depth+1)
		node.cause = w.capture(original.Cause, depth+1)
		node.children = []*diagnosticNode{node.kind, node.cause}
		if original.Panic != nil || kind == dependencyinjection.ErrCallbackPanicked {
			node.panicked = true
			// Discover first; quarantine complete original topology before rebuild.
			if payload := referenceIdentity(original.Panic); payload.ptr != 0 {
				w.quarantined[payload.ptr] = true
			}
			if payload, ok := original.Panic.(error); ok {
				captured := w.capture(payload, depth+1)
				node.children = append(node.children, captured)
				w.quarantineRoots = append(w.quarantineRoots, captured)
			}
			w.quarantineRoots = append(w.quarantineRoots, node.cause)
			if kind == errUnsafeDiagnostic {
				w.quarantineRoots = append(w.quarantineRoots, node.kind)
			}
			node.diagnostic.Kind = dependencyinjection.ErrCallbackPanicked
		}
		return node
	}
	switch tree := err.(type) {
	case interface{ Unwrap() []error }:
		if !standardDiagnosticTopology(err) {
			w.incomplete = true
			return nil
		}
		children := tree.Unwrap()
		// Check the entire list before allocating or iterating: even nil slots
		// consume inspection budget, and refusal discards the whole snapshot.
		if len(children) > w.remaining {
			w.incomplete = true
			return nil
		}
		node.children = make([]*diagnosticNode, 0, len(children))
		for _, child := range children {
			captured := w.capture(child, depth+1)
			if w.incomplete {
				return nil
			}
			if captured != nil {
				node.children = append(node.children, captured)
			}
		}
	case interface{ Unwrap() error }:
		if !standardDiagnosticTopology(err) || w.remaining == 0 {
			w.incomplete = true
			return nil
		}
		node.children = []*diagnosticNode{w.capture(tree.Unwrap(), depth+1)}
	case interface{ As(any) bool }, interface{ Is(error) bool }:
		// An opaque hook can conceal aliases or mutate uncaptured siblings.
		// Reject the whole snapshot without invoking or forwarding it.
		w.incomplete = true
		return nil
	default:
		if identity.ptr != 0 {
			node.leaf = err
		}
	}
	return node
}

// standardDiagnosticTopology admits only the concrete join/fmt wrapper types
// whose Unwrap methods return stored fields in supported Go 1.26/1.27. Package
// path plus named pointer type prevents application lookalikes from qualifying;
// new standard-library wrapper forms remain unsupported until explicitly reviewed.
func standardDiagnosticTopology(err error) bool {
	typ := reflect.TypeOf(err)
	if typ.Kind() != reflect.Pointer {
		return false
	}
	typ = typ.Elem()
	switch typ.PkgPath() {
	case "errors":
		return typ.Name() == "joinError"
	case "fmt":
		return typ.Name() == "wrapError" || typ.Name() == "wrapErrors"
	default:
		return false
	}
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

func (w *diagnosticWalker) rebuild(node *diagnosticNode, categoriesOnly bool) error {
	emission := diagnosticEmission{node, categoriesOnly}
	if node == nil || w.rebuilt[emission] {
		return nil
	}
	// Emit a shared subtree once per mode, bounding output even for input DAGs.
	w.rebuilt[emission] = true
	if node.leaf != nil {
		if categoriesOnly {
			return w.safeLeaf(safeKind(node.leaf))
		}
		return w.safeLeaf(node.leaf)
	}
	if node.diagnostic != nil {
		// Never attach an original Kind object or panic Cause to public fields.
		diagnostic := *node.diagnostic
		kind := diagnostic.Kind
		if kind != nil {
			diagnostic.Kind = w.safeLeaf(kind)
		}
		if node.panicked {
			if node.category != nil && node.category != errUnsafeDiagnostic && node.category != dependencyinjection.ErrCallbackPanicked {
				diagnostic.Cause = w.safeLeaf(node.category)
			}
		} else {
			var category error
			if kind == errUnsafeDiagnostic {
				// Unknown kinds may wrap known diagnostics. Expose only rebuilt
				// categories/metadata, not arbitrary ordinary Kind leaves.
				category = w.rebuild(node.kind, true)
			}
			diagnostic.Cause = errors.Join(category, w.rebuild(node.cause, categoriesOnly))
		}
		if !node.panicked && node.pathTooLong {
			diagnostic.Cause = errors.Join(diagnostic.Cause, errUnsafeDiagnostic)
		}
		return &diagnostic
	}
	children := make([]error, 0, len(node.children))
	for _, child := range node.children {
		if rebuilt := w.rebuild(child, categoriesOnly); rebuilt != nil {
			children = append(children, rebuilt)
		}
	}
	if cause := errors.Join(children...); cause != nil {
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
