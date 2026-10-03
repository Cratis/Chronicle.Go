// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

type recordingRegistrar struct {
	bindings []dependencyinjection.Binding
	attempts int
	failAt   int
	failure  error
}

func (r *recordingRegistrar) Register(b dependencyinjection.Binding) error {
	r.attempts++
	if r.attempts == r.failAt {
		return r.failure
	}
	r.bindings = append(r.bindings, b)
	return nil
}

type catalogRegistrar struct {
	recordingRegistrar
	present map[dependencyinjection.Key]bool
}

func (r *catalogRegistrar) Contains(key dependencyinjection.Key) bool {
	if r.present[key] {
		return true
	}
	for _, b := range r.bindings {
		if b.Key() == key {
			return true
		}
	}
	return false
}
func staticStore(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
	return "store", "namespace", nil
}

func TestBindingDescriptorsUseExactKeysBorrowedFacadesAndResourceFreeOwnedCell(t *testing.T) {
	p, err := chronicle.CaptureClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Client().Close() }()
	r := &recordingRegistrar{}
	if err := BindClient(r, p.Client()); err != nil {
		t.Fatal(err)
	}
	if err := BindEventStore(r, staticStore); err != nil {
		t.Fatal(err)
	}
	for _, bind := range []func(dependencyinjection.Registrar) error{BindEventLog, BindEventTypes, BindReadModels, BindCompliance} {
		if err := bind(r); err != nil {
			t.Fatal(err)
		}
	}
	keys := []dependencyinjection.Key{dependencyinjection.KeyFor[*chronicle.Client](), dependencyinjection.KeyFor[*storeSelection](), dependencyinjection.KeyFor[*chronicle.EventStore](), dependencyinjection.KeyFor[*eventsequences.Sequence](), dependencyinjection.KeyFor[*events.Catalog](), dependencyinjection.KeyFor[*readmodels.Service](), dependencyinjection.KeyFor[*compliance.Manager]()}
	if len(r.bindings) != len(keys) {
		t.Fatal("unexpected bindings", len(r.bindings))
	}
	for i, b := range r.bindings {
		lifetime, ownership := dependencyinjection.Scoped, dependencyinjection.Borrowed
		if i == 0 {
			lifetime = dependencyinjection.Singleton
		}
		if i == 1 {
			ownership = dependencyinjection.Owned
		}
		if b.Key() != keys[i] || b.Lifetime() != lifetime || b.Ownership() != ownership {
			t.Fatalf("descriptor %d: %v %v %v", i, b.Key(), b.Lifetime(), b.Ownership())
		}
		deps := b.Dependencies()
		switch {
		case i < 2:
			if len(deps) != 0 {
				t.Fatal("hidden dependency")
			}
		case i == 2:
			if len(deps) != 2 || deps[0] != keys[1] || deps[1] != keys[0] {
				t.Fatal("selection must be cached before client resolution")
			}
		default:
			if len(deps) != 1 || deps[0] != keys[2] {
				t.Fatal("facade does not depend on exact store")
			}
		}
	}
	got, err := r.bindings[0].Construct(context.Background(), nil)
	if err != nil || got != p.Client() {
		t.Fatal("borrowed identity changed", err)
	}
}

func TestBindEventStorePreflightsWholeBatchAndDependencies(t *testing.T) {
	clientKey, cellKey, storeKey := dependencyinjection.KeyFor[*chronicle.Client](), dependencyinjection.KeyFor[*storeSelection](), dependencyinjection.KeyFor[*chronicle.EventStore]()
	for _, duplicate := range []dependencyinjection.Key{cellKey, storeKey} {
		r := &catalogRegistrar{present: map[dependencyinjection.Key]bool{clientKey: true, duplicate: true}}
		if err := BindEventStore(r, staticStore); !errors.Is(err, dependencyinjection.ErrDuplicate) {
			t.Fatal(err)
		}
		if r.attempts != 0 {
			t.Fatal("preflight left a partial batch")
		}
	}
	r := &catalogRegistrar{}
	if err := BindEventStore(r, staticStore); !errors.Is(err, dependencyinjection.ErrMissing) {
		t.Fatal(err)
	}
	if r.attempts != 0 {
		t.Fatal("missing client left a partial batch")
	}
	for _, bind := range []func(dependencyinjection.Registrar) error{BindEventLog, BindEventTypes, BindReadModels, BindCompliance} {
		if err := bind(r); !errors.Is(err, dependencyinjection.ErrMissing) {
			t.Fatal(err)
		}
	}
	if r.attempts != 0 {
		t.Fatal("missing store registered facades")
	}
}

func TestBindingRegistrationFailuresAreReturnedWithoutRollback(t *testing.T) {
	failure := errors.New("registrar failure")
	for _, failAt := range []int{1, 2} {
		r := &recordingRegistrar{failAt: failAt, failure: failure}
		if err := BindEventStore(r, staticStore); err != failure {
			t.Fatal("registration error changed", err)
		}
		if len(r.bindings) != failAt-1 || r.attempts != failAt {
			t.Fatal("partial registration was hidden")
		}
		if failAt == 2 {
			if r.bindings[0].Key() != dependencyinjection.KeyFor[*storeSelection]() {
				t.Fatal("wrong partial binding")
			}
		}
	}
	// A Catalog exposes the leftover private cell and prevents a second attempt
	// from silently replacing it. Generic Registrars do not promise rollback.
	r := &catalogRegistrar{recordingRegistrar: recordingRegistrar{failAt: 2, failure: failure}, present: map[dependencyinjection.Key]bool{dependencyinjection.KeyFor[*chronicle.Client](): true}}
	if err := BindEventStore(r, staticStore); err != failure {
		t.Fatal(err)
	}
	if err := BindEventStore(r, staticStore); !errors.Is(err, dependencyinjection.ErrDuplicate) {
		t.Fatal(err)
	}
	if r.attempts != 2 {
		t.Fatal("partial batch was retried")
	}
}

func TestBindingsRejectNilAndDuplicatesWithoutInvokingCallbacks(t *testing.T) {
	p, err := chronicle.CaptureClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Client().Close() }()
	var typedNil *container.Registry
	for _, r := range []dependencyinjection.Registrar{nil, typedNil} {
		if err := BindClient(r, p.Client()); !errors.Is(err, dependencyinjection.ErrInvalidRegistration) {
			t.Fatal(err)
		}
		if err := BindEventStore(r, staticStore); !errors.Is(err, dependencyinjection.ErrInvalidRegistration) {
			t.Fatal(err)
		}
		for _, bind := range []func(dependencyinjection.Registrar) error{BindEventLog, BindEventTypes, BindReadModels, BindCompliance} {
			if err := bind(r); !errors.Is(err, dependencyinjection.ErrInvalidRegistration) {
				t.Fatal(err)
			}
		}
	}
	var r container.Registry
	if err := BindClient(&r, nil); !errors.Is(err, dependencyinjection.ErrNilValue) {
		t.Fatal(err)
	}
	if err := BindEventStore(&r, nil); !errors.Is(err, dependencyinjection.ErrNilValue) {
		t.Fatal(err)
	}
	if err := BindClient(&r, p.Client()); err != nil {
		t.Fatal(err)
	}
	if err := BindClient(&r, p.Client()); !errors.Is(err, dependencyinjection.ErrDuplicate) {
		t.Fatal(err)
	}
	selector := func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
		t.Fatal("selector ran during registration/build")
		return "", "", nil
	}
	if err := BindEventStore(&r, selector); err != nil {
		t.Fatal(err)
	}
	if err := BindEventStore(&r, selector); !errors.Is(err, dependencyinjection.ErrDuplicate) {
		t.Fatal(err)
	}
	for _, bind := range []func(dependencyinjection.Registrar) error{BindEventLog, BindEventTypes, BindReadModels, BindCompliance} {
		if err := bind(&r); err != nil {
			t.Fatal(err)
		}
		if err := bind(&r); !errors.Is(err, dependencyinjection.ErrDuplicate) {
			t.Fatal(err)
		}
	}
	provider, err := r.Build()
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Prepare(context.Background(), nil); err != nil {
		t.Fatal("provider owned borrowed root", err)
	}
}
