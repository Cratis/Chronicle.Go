// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reactors_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/reactors"
)

type OwnedReactor struct {
	trace   *[]string
	cleanup error
}

func (r *OwnedReactor) React(Ordered) {}
func (r *OwnedReactor) Close(context.Context) error {
	*r.trace = append(*r.trace, "reactor")
	return r.cleanup
}

type OwnedMiddleware struct{ trace *[]string }

func (*OwnedMiddleware) Before(context.Context, reactors.Invocation) error { return nil }
func (*OwnedMiddleware) After(context.Context, reactors.Invocation) error  { return nil }
func (m *OwnedMiddleware) Close() error                                    { *m.trace = append(*m.trace, "middleware"); return nil }

type ownedScopes struct {
	trace    *[]string
	instance any
}

func (f *ownedScopes) NewScope(context.Context) (reactors.Scope, error) { return &ownedScope{f}, nil }

type ownedScope struct{ factory *ownedScopes }

func (s *ownedScope) Resolve(context.Context, reflect.Type) (any, error) {
	return s.factory.instance, nil
}
func (s *ownedScope) Close(context.Context) error {
	*s.factory.trace = append(*s.factory.trace, "scope")
	return nil
}

type catalogScopes struct{ *ownedScopes }

func (f catalogScopes) Contains(t reflect.Type) bool { return t == reflect.TypeFor[*OwnedReactor]() }

func TestActivationFailureReleasesPartialResourcesAndPreservesCause(t *testing.T) {
	failure := errors.New("activation failed")
	for _, stage := range []string{"reactor", "middleware"} {
		t.Run(stage, func(t *testing.T) {
			var trace []string
			d, err := reactors.Define[*OwnedReactor](func() (*OwnedReactor, error) {
				if stage == "reactor" {
					return &OwnedReactor{trace: &trace}, failure
				}
				return &OwnedReactor{trace: &trace}, nil
			}, reactors.WithMiddleware(func() (*OwnedMiddleware, error) { return &OwnedMiddleware{&trace}, failure }))
			if err != nil {
				t.Fatal(err)
			}
			c, m := catalogs(t)
			p, err := reactors.Compile(d, c, m, &ownedScopes{trace: &trace})
			if err != nil {
				t.Fatal(err)
			}
			lease, err := p.Activate(t.Context())
			var activation *reactors.ActivationError
			if lease != nil || !errors.As(err, &activation) || !errors.Is(err, failure) {
				t.Fatalf("lease %v error %v", lease, err)
			}
			want := []string{"reactor", "scope"}
			if stage == "middleware" {
				want = []string{"middleware", "reactor", "scope"}
			}
			if !slices.Equal(trace, want) {
				t.Fatal(trace)
			}
		})
	}
}
func TestLeaseCleanupIsIdempotentAndGuardsInvocation(t *testing.T) {
	var trace []string
	failure := errors.New("close failed")
	d, _ := reactors.Define[*OwnedReactor](func() *OwnedReactor { return &OwnedReactor{&trace, failure} }, reactors.WithMiddleware(func() *OwnedMiddleware { return &OwnedMiddleware{&trace} }))
	c, m := catalogs(t)
	p, err := reactors.Compile(d, c, m, &ownedScopes{trace: &trace})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := p.Activate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	firstClose, secondClose := lease.Close(t.Context()), lease.Close(t.Context())
	if !errors.Is(firstClose, failure) || !errors.Is(secondClose, failure) {
		t.Fatal("cleanup cause lost")
	}
	if !slices.Equal(trace, []string{"middleware", "reactor", "scope"}) {
		t.Fatal(trace)
	}
	if err = lease.Invoke(t.Context(), &Ordered{}, events.Context{}, &testRuntime{}); err == nil {
		t.Fatal("closed lease invoked")
	}
}
func TestRegisteredArtifactsWinWithoutDoubleDisposal(t *testing.T) {
	var trace []string
	instance := &OwnedReactor{trace: &trace}
	constructed := false
	d, _ := reactors.Define[*OwnedReactor](func() *OwnedReactor { constructed = true; return instance })
	c, m := catalogs(t)
	p, err := reactors.Compile(d, c, m, catalogScopes{&ownedScopes{trace: &trace, instance: instance}})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := p.Activate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if constructed || !slices.Equal(trace, []string{"scope"}) {
		t.Fatalf("constructor %v cleanup %v", constructed, trace)
	}
}
func TestFactoryWithoutCatalogDefersResolutionAndRejectsWrongTypes(t *testing.T) {
	var trace []string
	d, _ := reactors.Define[BadParameter](func() BadParameter { return BadParameter{} })
	c, m := catalogs(t)
	p, err := reactors.Compile(d, c, m, &ownedScopes{trace: &trace, instance: 42})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := p.Activate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err = lease.Invoke(t.Context(), &Ordered{}, events.Context{EventType: events.TypeRef{ID: "Ordered"}}, &testRuntime{}); err == nil {
		t.Fatal("wrong service type accepted")
	}
	if err = lease.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

type Redirect struct{}

func (Redirect) React(Ordered) Other               { return Other{9} }
func (Redirect) GetEventSourceID() events.SourceID { return "redirected" }

type sourceRuntime struct {
	testRuntime
	source events.SourceID
}

func (r *sourceRuntime) Append(ctx context.Context, source events.SourceID, value any) error {
	r.source = source
	return r.testRuntime.Append(ctx, source, value)
}
func TestReturnedEventUsesReactorSourceProvider(t *testing.T) {
	d, _ := reactors.Define[Redirect](func() Redirect { return Redirect{} })
	p := compile(t, d)
	lease, err := p.Activate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	runtime := &sourceRuntime{}
	if err = lease.Invoke(t.Context(), &Ordered{}, events.Context{SourceID: "original", EventType: events.TypeRef{ID: "Ordered"}}, runtime); err != nil {
		t.Fatal(err)
	}
	if err = lease.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if runtime.source != "redirected" {
		t.Fatal(runtime.source)
	}
}
