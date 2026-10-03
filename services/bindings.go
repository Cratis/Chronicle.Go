// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services

import (
	"context"
	"reflect"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/compliance"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/fundamentals.go/dependencyinjection"
)

// StoreSelector reads trusted host metadata, not message payloads, to select a
// store and nonblank namespace. Selection is not authorization. It must not
// resolve services (including through a captured/global scope), call the SDK, or
// retain ctx. Hidden resolution can self-wait in a provider's in-flight cache;
// only declared dependency cycles can be rejected at provider build.
//
// The first resolving caller runs selection synchronously with its context.
// Give that caller a bounded operation context and join it before scope closure.
// Cancellation is cooperative; no background callback is started. Other callers
// may cancel their waits without canceling this invocation. Coordinates or error
// (including owner cancellation) freeze once per scope. A new scope is required
// to reselect. Selectors can run concurrently in different scopes.
type StoreSelector func(context.Context) (chronicle.StoreName, chronicle.Namespace, error)

// BindClient binds the exact client as a borrowed Singleton *chronicle.Client.
// Supply a CaptureClient identity or a normally constructed client, never a zero
// value or copy. No lifecycle operation is invoked. The application must close
// Chronicle and join preparation/application work before closing the provider.
func BindClient(r dependencyinjection.Registrar, client *chronicle.Client) error {
	key := dependencyinjection.KeyFor[*chronicle.Client]()
	if client == nil {
		return bindingError(key, dependencyinjection.ErrInvalidRegistration, dependencyinjection.ErrNilValue)
	}
	if err := preflightBindings(r, []dependencyinjection.Key{key}, nil); err != nil {
		return err
	}
	return dependencyinjection.BindValue(r, client)
}

// BindEventStore binds a borrowed Scoped *chronicle.EventStore and a private,
// scope-owned selection cell with no resources. BindClient must precede it when
// r exposes Catalog. The cell is successfully cached before selection runs, so
// failed store registration can retry with the same frozen coordinates.
//
// Resolution calls Client.EventStore, which performs connection/registration I/O
// only after preparation. There is no Ready probe: an unprepared or closed client
// returns its ordinary guard error, but the application selector may run first.
//
// Every helper preflights its added keys and required dependencies when Catalog
// is available. Registration is single-owner, not transactional: any Register
// error is returned unchanged. If the second registration fails, the private
// cell remains registered; discard the registrar rather than reuse that partial
// graph. Helpers never replace bindings or roll back arbitrary Registrars.
func BindEventStore(r dependencyinjection.Registrar, selector StoreSelector) error {
	key := dependencyinjection.KeyFor[*chronicle.EventStore]()
	if selector == nil {
		return bindingError(key, dependencyinjection.ErrInvalidRegistration, dependencyinjection.ErrNilValue)
	}
	keys := []dependencyinjection.Key{dependencyinjection.KeyFor[*storeSelection](), key}
	if err := preflightBindings(r, keys, []dependencyinjection.Key{dependencyinjection.KeyFor[*chronicle.Client]()}); err != nil {
		return err
	}
	if err := dependencyinjection.Bind(r, dependencyinjection.Scoped,
		func(context.Context, dependencyinjection.Resolver) (*storeSelection, error) {
			return &storeSelection{}, nil
		}); err != nil {
		return err
	}
	return dependencyinjection.BindBorrowedFunc2(r, dependencyinjection.Scoped,
		func(ctx context.Context, selection *storeSelection, client *chronicle.Client) (*chronicle.EventStore, error) {
			name, namespace, err := selection.selectStore(ctx, selector)
			if err != nil {
				return nil, err
			}
			return client.EventStore(ctx, name, chronicle.WithNamespace(namespace))
		})
}

// BindEventLog binds a borrowed Scoped *eventsequences.Sequence, forwarding the
// selected store's exact EventLog instance. BindEventStore first; per-helper
// preflight and partial-registration semantics are described on BindEventStore.
func BindEventLog(r dependencyinjection.Registrar) error {
	return bindFacade(r, func(store *chronicle.EventStore) *eventsequences.Sequence { return store.EventLog() })
}

// BindEventTypes binds the selected store's exact *events.Catalog as borrowed
// Scoped. BindEventStore first. It has the preflight semantics of BindEventLog.
func BindEventTypes(r dependencyinjection.Registrar) error {
	return bindFacade(r, func(store *chronicle.EventStore) *events.Catalog { return store.EventTypes() })
}

// BindReadModels binds the selected store's exact *readmodels.Service as borrowed
// Scoped. BindEventStore first. It has the preflight semantics of BindEventLog.
func BindReadModels(r dependencyinjection.Registrar) error {
	return bindFacade(r, func(store *chronicle.EventStore) *readmodels.Service { return store.ReadModels() })
}

// BindCompliance binds the selected store's exact *compliance.Manager as borrowed
// Scoped. BindEventStore first. It has the preflight semantics of BindEventLog.
func BindCompliance(r dependencyinjection.Registrar) error {
	return bindFacade(r, func(store *chronicle.EventStore) *compliance.Manager { return store.Compliance() })
}

func bindFacade[T any](r dependencyinjection.Registrar, get func(*chronicle.EventStore) T) error {
	if err := preflightBindings(r, []dependencyinjection.Key{dependencyinjection.KeyFor[T]()},
		[]dependencyinjection.Key{dependencyinjection.KeyFor[*chronicle.EventStore]()}); err != nil {
		return err
	}
	return dependencyinjection.BindBorrowedFunc1(r, dependencyinjection.Scoped,
		func(_ context.Context, store *chronicle.EventStore) (T, error) { return get(store), nil })
}

func preflightBindings(r dependencyinjection.Registrar, added, required []dependencyinjection.Key) error {
	if r == nil || nilRegistrar(r) {
		return bindingError(added[0], dependencyinjection.ErrInvalidRegistration, nil)
	}
	if catalog, ok := r.(dependencyinjection.Catalog); ok {
		for _, key := range added {
			if catalog.Contains(key) {
				return bindingError(key, dependencyinjection.ErrDuplicate, nil)
			}
		}
		for _, key := range required {
			if !catalog.Contains(key) {
				return bindingError(key, dependencyinjection.ErrMissing, nil)
			}
		}
	}
	return nil
}

func nilRegistrar(r dependencyinjection.Registrar) bool {
	value := reflect.ValueOf(r)
	switch value.Kind() {
	case reflect.Pointer, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan, reflect.Interface:
		return value.IsNil()
	}
	return false
}

func bindingError(key dependencyinjection.Key, kind, cause error) error {
	return &dependencyinjection.Error{Operation: "bind", Key: key, Kind: kind, Cause: cause}
}
