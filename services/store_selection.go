// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	chronicle "github.com/cratis/chronicle.go"
)

// ErrSelectorPanicked identifies a selector panic. Its payload is discarded.
var ErrSelectorPanicked = errors.New("chronicle services: store selector panicked")

// SelectorError reports a frozen selection failure without formatting coordinates,
// callback errors or panic data. Unwrap exposes only sanitized ordinary causes
// (the same diagnostic grammar as WithServices), standard context cancellation,
// ErrSelectorPanicked, or chronicle.ErrInvalidConfiguration for blank coordinates.
// Retrying selection requires a new scope. Its zero value is a generic failure.
type SelectorError struct{ cause error }

// Error returns fixed, payload-free text.
func (*SelectorError) Error() string { return "chronicle services: store selection failed" }

// Unwrap exposes the sanitized cause, never a recovered panic payload.
func (e *SelectorError) Unwrap() error { return e.cause }

// Format keeps all formatting verbs payload-free, including %#v.
func (e *SelectorError) Format(state fmt.State, _ rune) { _, _ = fmt.Fprint(state, e.Error()) }

// storeSelection owns only immutable coordinates or a safe error after completion.
// Neither the selector nor any context/resolver is retained in the scoped cell.
type storeSelection struct {
	mu        sync.Mutex
	done      chan struct{}
	name      chronicle.StoreName
	namespace chronicle.Namespace
	err       error
}

func (s *storeSelection) selectStore(ctx context.Context, selector StoreSelector) (chronicle.StoreName, chronicle.Namespace, error) {
	s.mu.Lock()
	if s.done != nil {
		done := s.done
		s.mu.Unlock()
		select {
		case <-done:
			return s.name, s.namespace, s.err
		case <-ctx.Done():
			return "", "", ctx.Err()
		}
	}
	s.done = make(chan struct{})
	s.mu.Unlock()

	// The first caller owns synchronous callback execution. No SDK/cell lock is
	// held, and a waiter's cancellation cannot change this caller's outcome.
	name, namespace, err := invokeStoreSelector(ctx, selector)
	s.name, s.namespace, s.err = name, namespace, err
	close(s.done)
	return name, namespace, err
}

func invokeStoreSelector(ctx context.Context, selector StoreSelector) (name chronicle.StoreName, namespace chronicle.Namespace, err error) {
	defer func() {
		if recover() != nil {
			name, namespace, err = "", "", &SelectorError{cause: ErrSelectorPanicked}
		}
	}()
	if err = ctx.Err(); err == nil {
		name, namespace, err = selector(ctx)
		// Owner cancellation is frozen even if the callback ignores it.
		if canceled := ctx.Err(); canceled != nil {
			err = canceled
		}
	}
	if err != nil {
		if err != context.Canceled && err != context.DeadlineExceeded {
			err = sanitizeError(err)
		}
		return "", "", &SelectorError{cause: err}
	}
	if strings.TrimSpace(string(name)) == "" || strings.TrimSpace(string(namespace)) == "" {
		return "", "", &SelectorError{cause: chronicle.ErrInvalidConfiguration}
	}
	// Copy the coordinates; no application object is retained in the cell.
	return chronicle.StoreName(strings.Clone(string(name))), chronicle.Namespace(strings.Clone(string(namespace))), nil
}
