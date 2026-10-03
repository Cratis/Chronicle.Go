// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reducers_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/reducers"
)

type ownedFold struct{ trace *[]string }

func (*ownedFold) Fold(Changed, *Total) *Total   { return &Total{} }
func (f *ownedFold) Close(context.Context) error { *f.trace = append(*f.trace, "artifact"); return nil }

type scopes struct {
	trace *[]string
	value *ownedFold
	fail  error
}

func (f *scopes) NewScope(context.Context) (reducers.Scope, error) {
	*f.trace = append(*f.trace, "open")
	return &scope{f}, nil
}
func (f *scopes) Contains(typ reflect.Type) bool {
	return f.value != nil && typ == reflect.TypeFor[*ownedFold]()
}

type scope struct{ factory *scopes }

func (s *scope) Resolve(context.Context, reflect.Type) (any, error) { return s.factory.value, nil }
func (s *scope) Close(ctx context.Context) error {
	if s.factory.value != nil {
		if err := s.factory.value.Close(ctx); err != nil {
			return err
		}
	}
	*s.factory.trace = append(*s.factory.trace, "scope")
	return s.factory.fail
}
func TestReducerRegisteredServiceWinsAndIsDisposedOnlyByScope(t *testing.T) {
	c, m, models := catalog(t)
	var trace []string
	factory := &scopes{trace: &trace, value: &ownedFold{&trace}}
	constructed := false
	d, _ := reducers.Define[*ownedFold](m, func() *ownedFold { constructed = true; return &ownedFold{&trace} })
	p, err := reducers.Compile(d, c, models, factory)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		result := p.Reduce(t.Context(), []reducers.Event{event("Changed", 0, Changed{}), event("Changed", 1, Changed{})}, nil)
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}
	if constructed || !slices.Equal(trace, []string{"open", "artifact", "scope", "open", "artifact", "scope"}) {
		t.Fatal(trace, constructed)
	}
}
func TestReducerActivationFailureReleasesPartialArtifactAndScope(t *testing.T) {
	c, m, models := catalog(t)
	var trace []string
	failure := errors.New("construction failed")
	d, _ := reducers.Define[*ownedFold](m, func() (*ownedFold, error) { return &ownedFold{&trace}, failure })
	p, err := reducers.Compile(d, c, models, &scopes{trace: &trace})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := p.Activate(t.Context())
	var detail *reducers.ActivationError
	if lease != nil || !errors.Is(err, failure) || !errors.As(err, &detail) || !slices.Equal(trace, []string{"open", "artifact", "scope"}) {
		t.Fatal(lease, err, trace)
	}
}
func TestReducerLeaseGuardsUseAfterCloseAndRetainsCleanupFailure(t *testing.T) {
	c, m, models := catalog(t)
	var trace []string
	failure := errors.New("cleanup failed")
	d, _ := reducers.Define[*ownedFold](m, func() *ownedFold { return &ownedFold{&trace} })
	p, err := reducers.Compile(d, c, models, &scopes{trace: &trace, fail: failure})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := p.Activate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := lease.Close(t.Context()); !errors.Is(err, failure) {
			t.Fatal(err)
		}
	}
	if _, err := lease.Invoke(t.Context(), event("Changed", 0, Changed{}), nil); err == nil {
		t.Fatal("invoked closed lease")
	}
	if !slices.Equal(trace, []string{"open", "artifact", "scope"}) {
		t.Fatal(trace)
	}
	result := p.Reduce(t.Context(), []reducers.Event{event("Changed", 0, Changed{})}, nil)
	if result.State != nil || !errors.Is(result.Err, failure) || result.LastSuccessful != events.Unavailable {
		t.Fatal(result)
	}
}
