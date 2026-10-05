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
	"github.com/cratis/chronicle.go/readmodels"
)

type Ordered struct {
	Number int `json:"number"`
}

func (Ordered) Fact() {}

type Other struct {
	Number int `json:"number"`
}

func (Other) Fact() {}

type fact interface{ Fact() }
type Handlers struct{ calls *[]string }

func (h *Handlers) Z(event Ordered) { *h.calls = append(*h.calls, "Z") }
func (h *Handlers) B(_ context.Context, event *Ordered, ec events.Context) {
	*h.calls = append(*h.calls, "B")
}
func (h *Handlers) A(_ context.Context, event Ordered, ec events.Context) {
	*h.calls = append(*h.calls, "A")
}
func (h *Handlers) hidden(event Ordered, ec events.Context, d reactors.Delivery, unused string) {
	*h.calls = append(*h.calls, "hidden")
}
func (*Handlers) Ignore(any) int { return 0 }

type Family struct{ count *int }

func (h *Family) Anything(fact) { *h.count++ }

type BadResult struct{}

func (BadResult) Invalid(Ordered) string { return "" }

type BadParameter struct{}

func (BadParameter) Invalid(Ordered, interface{ Missing() }) {}

type Variadic struct{}

func (Variadic) Invalid(Ordered, ...events.Context) {}

type NoEvents struct{}

func (NoEvents) Ignore(any) {}

type testRuntime struct {
	values    []any
	appendErr error
	model     func(readmodels.Descriptor, readmodels.Key, reflect.Type) (any, error)
}

func (r *testRuntime) Append(_ context.Context, _ events.SourceID, value any) error {
	r.values = append(r.values, value)
	return r.appendErr
}
func (r *testRuntime) ReadModel(_ context.Context, d readmodels.Descriptor, key readmodels.Key, t reflect.Type) (any, error) {
	return r.model(d, key, t)
}
func catalogs(t *testing.T) (*events.Catalog, *readmodels.Catalog) {
	t.Helper()
	a, err := events.Define[Ordered]()
	if err != nil {
		t.Fatal(err)
	}
	b, err := events.Define[Other]()
	if err != nil {
		t.Fatal(err)
	}
	c, err := events.NewCatalog(a.Descriptor(), b.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	m, err := readmodels.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	return c, m
}
func compile(t *testing.T, d reactors.Declaration) *reactors.Plan {
	t.Helper()
	c, m := catalogs(t)
	p, err := reactors.Compile(d, c, m, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func invoke(t *testing.T, p *reactors.Plan, event any, id events.TypeID, r *testRuntime) error {
	t.Helper()
	l, err := p.Activate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	err = l.Invoke(t.Context(), event, events.Context{EventType: events.TypeRef{ID: id, Generation: 1}, SourceID: "source"}, r)
	if closeErr := l.Close(t.Context()); closeErr != nil {
		t.Fatal(closeErr)
	}
	return err
}
func TestDiscoveryPrecedenceAndExportedMethods(t *testing.T) {
	_ = (*Handlers).hidden // It exists, but reflection must not discover it.
	var calls []string
	d, err := reactors.Define[*Handlers](func() *Handlers { return &Handlers{&calls} })
	if err != nil {
		t.Fatal(err)
	}
	p := compile(t, d)
	if len(p.Shadows()) != 2 || p.Shadows()[0].Winner != "A" {
		t.Fatalf("shadows: %+v", p.Shadows())
	}
	if err = invoke(t, p, &Ordered{}, "Ordered", &testRuntime{}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(calls, []string{"A"}) {
		t.Fatal(calls)
	}
	if p.Identifier() != reactors.ID("github.com/cratis/chronicle.go/reactors_test.Handlers") {
		t.Fatal(p.Identifier())
	}
}
func TestRegisteredEventInterfaceFamilies(t *testing.T) {
	count := 0
	d, err := reactors.Define[*Family](func() *Family { return &Family{&count} })
	if err != nil {
		t.Fatal(err)
	}
	p := compile(t, d)
	if len(p.EventTypes()) != 2 {
		t.Fatal(p.EventTypes())
	}
	for _, event := range []struct {
		value any
		id    events.TypeID
	}{{&Ordered{}, "Ordered"}, {&Other{}, "Other"}} {
		if err := invoke(t, p, event.value, event.id, &testRuntime{}); err != nil {
			t.Fatal(err)
		}
	}
	if count != 2 {
		t.Fatal(count)
	}
}
func TestInvalidSignaturesAreTypedAndDoNotActivate(t *testing.T) {
	invoked := false
	goodFactory := func() *Handlers { invoked = true; return nil }
	a, _ := reactors.Define[BadResult](func() BadResult { return BadResult{} })
	b, _ := reactors.Define[BadParameter](func() BadParameter { return BadParameter{} })
	c, _ := reactors.Define[Variadic](func() Variadic { return Variadic{} })
	d, _ := reactors.Define[NoEvents](func() NoEvents { return NoEvents{} })
	e, _ := reactors.Define[*Handlers](func() int { return 1 })
	f, _ := reactors.Define[*Handlers](goodFactory, reactors.WithHandler(reactors.On(func(context.Context, Ordered) error { return nil })))
	catalog, models := catalogs(t)
	for _, declaration := range []reactors.Declaration{a, b, c, d, e, f} {
		_, err := reactors.Compile(declaration, catalog, models, nil)
		var typed *reactors.DeclarationError
		if !errors.As(err, &typed) {
			t.Fatalf("expected declaration error, got %v", err)
		}
	}
	if invoked {
		t.Fatal("constructor ran at compilation")
	}
}
func TestPlansAreIsolatedByCatalog(t *testing.T) {
	d, _ := reactors.Define[BadResult](func() BadResult { return BadResult{} })
	empty, _ := events.NewCatalog()
	models, _ := readmodels.NewCatalog()
	if _, err := reactors.Compile(d, empty, models, nil); err == nil {
		t.Fatal("empty catalog accepted")
	}
	c, _ := catalogs(t)
	if _, err := reactors.Compile(d, c, models, nil); err == nil {
		t.Fatal("bad handler result accepted after another catalog compiled")
	}
}

type ResultHandlers struct {
	err  error
	none bool
}

func (h *ResultHandlers) Return(Ordered) (*Other, error) {
	if h.none {
		return nil, h.err
	}
	return &Other{Number: 2}, h.err
}
func TestReturnedEventAndErrorShapes(t *testing.T) {
	failure := errors.New("failure")
	for _, tc := range []struct {
		name                    string
		handlerErr, errorAppend error
		none                    bool
		appends                 int
		want                    error
	}{{"event", nil, nil, false, 1, nil}, {"nil", nil, nil, true, 0, nil}, {"handler error", failure, nil, false, 0, failure}, {"append rejection", nil, failure, false, 1, failure}} {
		t.Run(tc.name, func(t *testing.T) {
			d, _ := reactors.Define[*ResultHandlers](func() *ResultHandlers { return &ResultHandlers{tc.handlerErr, tc.none} })
			r := &testRuntime{appendErr: tc.errorAppend}
			err := invoke(t, compile(t, d), &Ordered{}, "Ordered", r)
			if !errors.Is(err, tc.want) || len(r.values) != tc.appends {
				t.Fatalf("error %v appends %d", err, len(r.values))
			}
		})
	}
}
func TestTypedCallbacksShareValidationAndEffects(t *testing.T) {
	d, err := reactors.DefineHandler("plain", func(context.Context, Other) error { return nil }, reactors.WithHandler(reactors.Returning(func(_ context.Context, e Ordered) (Other, error) { return Other{e.Number + 1}, nil })))
	if err != nil {
		t.Fatal(err)
	}
	r := &testRuntime{}
	if err = invoke(t, compile(t, d), &Ordered{3}, "Ordered", r); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.values, []any{Other{4}}) {
		t.Fatal(r.values)
	}
}
func TestDeliveryIdentityMatchesCSharp(t *testing.T) {
	d := reactors.Delivery{Reactor: "reactor", Store: "store", Namespace: "tenant", Sequence: events.EventLog, Partition: "source", SequenceNumber: 42}
	if d.ID() != "reactor#store#tenant#event-log#source#42" {
		t.Fatal(d.ID())
	}
}
