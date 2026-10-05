// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services

import (
	"context"
	"errors"
	"testing"

	"github.com/cratis/fundamentals.go/dependencyinjection"
)

type partialScopeFactory struct {
	scope dependencyinjection.Scope
	err   error
}

func (f partialScopeFactory) NewScope(context.Context) (dependencyinjection.Scope, error) {
	return f.scope, f.err
}

type partialScope struct{ closed int }

func (*partialScope) Resolve(context.Context, dependencyinjection.Key) (any, error) { return nil, nil }
func (s *partialScope) Close(context.Context) error                                 { s.closed++; return nil }

func TestAdapterPreservesPartiallyOpenedScopeForOwnerCleanup(t *testing.T) {
	failure := errors.New("open failed")
	scope := &partialScope{}
	adapted, err := (scopeFactory{partialScopeFactory{scope, failure}}).NewScope(t.Context())
	if !errors.Is(err, failure) || adapted == nil || scope.closed != 0 {
		t.Fatal("partial scope lost or closed by adapter", err)
	}
	if err := adapted.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if scope.closed != 1 {
		t.Fatal("partial scope was not released")
	}
}

func TestAdapterRejectsNilScopeWithoutDiscardingOpenError(t *testing.T) {
	failure := errors.New("open failed")
	for _, scope := range []dependencyinjection.Scope{nil, (*partialScope)(nil)} {
		for _, cause := range []error{nil, failure} {
			adapted, err := (scopeFactory{partialScopeFactory{scope, cause}}).NewScope(t.Context())
			want := cause
			if want == nil {
				want = dependencyinjection.ErrInvalidScope
			}
			if adapted != nil || !errors.Is(err, want) {
				t.Fatal(adapted, err)
			}
		}
	}
}
