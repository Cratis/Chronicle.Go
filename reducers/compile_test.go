// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package reducers_test

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/metadata"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

type Changed struct{ Amount int }

func (Changed) IsChange() {}

type Removed struct{}
type Unknown struct{}
type Family interface{ IsChange() }
type Total struct {
	ID     string
	Amount int
}
type Other struct{ Amount int }
type Ledger struct{}

func (*Ledger) Change(ctx context.Context, e Changed, current *Total, ec events.Context) (*Total, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	amount := e.Amount
	if current != nil {
		amount += current.Amount
	}
	return &Total{ID: string(ec.SourceID), Amount: amount}, nil
}
func (*Ledger) Remove(Removed, *Total) *Total { return nil }

type Competing struct{}

func (Competing) Z(Changed, *Total) *Total                { return &Total{Amount: 99} }
func (Competing) B(Family, *Total, events.Context) *Total { return &Total{Amount: 2} }
func (Competing) A(Family, *Total, events.Context) *Total { return &Total{Amount: 1} }

type ExtraService struct{}

func (ExtraService) Fold(Changed, *Total, string) Total { return Total{} }

type WrongModel struct{}

func (WrongModel) Fold(Changed, *Other) Total { return Total{} }

type WrongReturn struct{}

func (WrongReturn) Fold(Changed, *Total) error { return nil }

type UnknownEvent struct{}

func (UnknownEvent) Fold(Unknown, *Total) Total { return Total{} }

type Variadic struct{}

func (Variadic) Fold(Changed, *Total, ...events.Context) Total { return Total{} }

type ValueCurrent struct{}

func (ValueCurrent) Fold(e Changed, current Total) Total { current.Amount += e.Amount; return current }

func catalog(t *testing.T) (*events.Catalog, readmodels.Model[Total], *readmodels.Catalog) {
	t.Helper()
	a, err := events.Define[Changed](events.WithGeneration(2))
	if err != nil {
		t.Fatal(err)
	}
	b, err := events.Define[Removed]()
	if err != nil {
		t.Fatal(err)
	}
	c, err := events.NewCatalog(a.Descriptor(), b.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	m, err := readmodels.Define[Total]()
	if err != nil {
		t.Fatal(err)
	}
	models, err := readmodels.NewCatalog(m.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	return c, m, models
}
func event(id events.TypeID, n events.SequenceNumber, content any) reducers.Event {
	return reducers.Event{Content: content, Context: events.Context{EventType: events.TypeRef{ID: id, Generation: 2}, SourceID: "account", SequenceNumber: n}}
}
func TestExplicitAndDiscoveredFoldsSharePlanAndResults(t *testing.T) {
	c, m, models := catalog(t)
	// Supply the full callback shape on both paths, so the semantic signature is identical.
	d, err := reducers.Define[*Single](m, func() *Single { return &Single{} }, reducers.WithVersion("v1"))
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := reducers.DefineHandlers(m, "explicit", []reducers.Handler{reducers.On(func(ctx context.Context, e Changed, current *Total, ec events.Context) (*Total, error) {
		return (&Single{}).Change(ctx, e, current, ec)
	})}, reducers.WithVersion("v1"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := reducers.Compile(d, c, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	q, err := reducers.Compile(explicit, c, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(p.EventTypes(), q.EventTypes()) || p.Fingerprint() != q.Fingerprint() {
		t.Fatal("explicit/discovered plans differ")
	}
	batch := []reducers.Event{event("Changed", 0, Changed{2}), event("Changed", 1, &Changed{3})}
	for _, plan := range []*reducers.Plan{p, q} {
		result := plan.Reduce(t.Context(), batch, nil)
		if result.Err != nil || !reflect.DeepEqual(result.State, &Total{ID: "account", Amount: 5}) || result.LastSuccessful != 1 {
			t.Fatalf("%+v", result)
		}
	}
	changed, _ := reducers.Define[*Single](m, nil, reducers.WithVersion("v2"))
	v2, err := reducers.Compile(changed, c, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	if p.Fingerprint() == v2.Fingerprint() {
		t.Fatal("version omitted from fingerprint")
	}
}

type Single struct{}

func (*Single) Change(ctx context.Context, e Changed, current *Total, ec events.Context) (*Total, error) {
	return (&Ledger{}).Change(ctx, e, current, ec)
}

func TestDiscoveryPrecedenceFamiliesAndCatalogIsolation(t *testing.T) {
	c, m, models := catalog(t)
	d, _ := reducers.Define[Competing](m, nil)
	p, err := reducers.Compile(d, c, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Shadows()) != 2 || p.Shadows()[0].Winner != "A" {
		t.Fatal(p.Shadows())
	}
	result := p.Reduce(t.Context(), []reducers.Event{event("Changed", 0, &Changed{})}, nil)
	if result.Err != nil || result.State.(*Total).Amount != 1 {
		t.Fatal(result)
	}
	empty, _ := events.NewCatalog()
	if _, err := reducers.Compile(d, empty, models, nil); err == nil {
		t.Fatal("global dispatch cache leaked")
	}
}
func TestInvalidFoldSignaturesAreTypedStartupErrors(t *testing.T) {
	c, m, models := catalog(t)
	a, _ := reducers.Define[ExtraService](m, nil)
	b, _ := reducers.Define[WrongModel](m, nil)
	d, _ := reducers.Define[WrongReturn](m, nil)
	e, _ := reducers.Define[UnknownEvent](m, nil)
	f, _ := reducers.Define[Variadic](m, nil)
	g, _ := reducers.Define[*Single](m, func() int { return 1 })
	duplicate, _ := reducers.Define[*Single](m, nil, reducers.WithHandler(reducers.On((&Single{}).Change)))
	wrong, _ := reducers.DefineHandlers(m, "wrong", []reducers.Handler{reducers.On(func(context.Context, Changed, *Other, events.Context) (*Other, error) { return nil, nil })})
	for i, declaration := range []reducers.Declaration{a, b, d, e, f, g, duplicate, wrong} {
		_, err := reducers.Compile(declaration, c, models, nil)
		var detail *reducers.DeclarationError
		if !errors.Is(err, chronicle.ErrInvalidConfiguration) || !errors.As(err, &detail) {
			t.Fatalf("case %d: %v", i, err)
		}
	}
}
func TestNilDeleteRecreateAndPresentZero(t *testing.T) {
	c, m, models := catalog(t)
	d, _ := reducers.Define[*Ledger](m, nil)
	p, err := reducers.Compile(d, c, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := p.Reduce(t.Context(), []reducers.Event{event("Changed", 0, Changed{7}), event("Removed", 1, Removed{}), event("Changed", 2, Changed{})}, nil)
	if result.Err != nil || !reflect.DeepEqual(result.State, &Total{ID: "account"}) || result.LastSuccessful != 2 {
		t.Fatal(result)
	}
	result = p.Reduce(t.Context(), []reducers.Event{event("Removed", 3, Removed{})}, result.State)
	if result.Err != nil || result.State != nil || result.LastSuccessful != 3 {
		t.Fatal(result)
	}
}
func TestValueCurrentUsesZeroForAbsence(t *testing.T) {
	c, m, models := catalog(t)
	d, _ := reducers.Define[ValueCurrent](m, nil)
	p, err := reducers.Compile(d, c, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := p.Reduce(t.Context(), []reducers.Event{event("Changed", 0, Changed{4})}, nil)
	if result.Err != nil || result.State.(*Total).Amount != 4 {
		t.Fatal(result)
	}
}
func TestFirstFailureStopsAndDiscardsMutatedPartialState(t *testing.T) {
	c, m, models := catalog(t)
	calls := 0
	failure := errors.New("bad amount")
	d, _ := reducers.DefineHandlers(m, "fail", []reducers.Handler{reducers.On(func(_ context.Context, e Changed, current *Total, _ events.Context) (*Total, error) {
		calls++
		if current == nil {
			current = &Total{}
		}
		current.Amount += e.Amount
		if calls == 2 {
			return current, failure
		}
		return current, nil
	})})
	p, err := reducers.Compile(d, c, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := p.Reduce(t.Context(), []reducers.Event{event("Changed", 0, Changed{1}), event("Changed", 1, Changed{2}), event("Changed", 2, Changed{3})}, nil)
	if !errors.Is(result.Err, failure) || result.State != nil || result.LastSuccessful != 0 || calls != 2 {
		t.Fatal(result, calls)
	}
}
func TestIdentityIsSystemForConstructorsAndFolds(t *testing.T) {
	c, m, models := catalog(t)
	check := func(ctx context.Context) {
		identity := metadata.Identity(ctx)
		if !reflect.DeepEqual(identity, identities.System()) {
			t.Fatalf("identity %+v", identity)
		}
	}
	d, _ := reducers.Define[*Single](m, func(ctx context.Context) *Single { check(ctx); return &Single{} })
	p, err := reducers.Compile(d, c, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	result := p.Reduce(metadata.WithIdentity(t.Context(), identities.Identity{Subject: "human"}), []reducers.Event{event("Changed", 0, Changed{1})}, nil)
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	callback, _ := reducers.DefineHandlers(m, "identity", []reducers.Handler{reducers.On(func(ctx context.Context, _ Changed, _ *Total, _ events.Context) (*Total, error) {
		check(ctx)
		return nil, nil
	})})
	p, err = reducers.Compile(callback, c, models, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result := p.Reduce(metadata.WithIdentity(t.Context(), identities.Identity{Subject: "human"}), []reducers.Event{event("Changed", 0, Changed{})}, nil); result.Err != nil {
		t.Fatal(result.Err)
	}
}
