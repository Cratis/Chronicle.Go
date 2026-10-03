// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

type selectorOpaqueError struct{ called *bool }

func (*selectorOpaqueError) Error() string   { panic("never format selector errors") }
func (e *selectorOpaqueError) Unwrap() error { *e.called = true; panic("never inspect opaque hooks") }

type selectorPanic struct{}

func (*selectorPanic) Error() string { panic("never format panic data") }

func TestSelectorFailureFreezesSafeErrorAndRequiresNewScope(t *testing.T) {
	ordinary := errors.New("private ordinary message")
	payload := &selectorPanic{}
	inspected := false
	cases := []struct {
		name     string
		selector StoreSelector
		cause    error
		retained error
	}{
		{"ordinary", func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
			return "private-store", "private-namespace", fmt.Errorf("private wrapper: %w", ordinary)
		}, ordinary, ordinary},
		{"panic", func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) { panic(payload) }, ErrSelectorPanicked, nil},
		{"nil panic", func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) { panic(nil) }, ErrSelectorPanicked, nil},
		{"opaque", func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
			return "", "", &selectorOpaqueError{&inspected}
		}, errUnsafeDiagnostic, nil},
		{"blank store", func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
			return " \t", "private-namespace", nil
		}, chronicle.ErrInvalidConfiguration, nil},
		{"blank namespace", func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
			return "private-store", "\n", nil
		}, chronicle.ErrInvalidConfiguration, nil},
		{"cancellation", func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
			return "", "", context.Canceled
		}, context.Canceled, nil},
		{"deadline", func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
			return "", "", context.DeadlineExceeded
		}, context.DeadlineExceeded, nil},
		{"panic aliases", func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
			return "", "", errors.Join(ordinary, &dependencyinjection.Error{Kind: dependencyinjection.ErrCallbackPanicked, Panic: ordinary, Cause: ordinary})
		}, dependencyinjection.ErrCallbackPanicked, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := chronicle.CaptureClient()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = p.Client().Close() }()
			var r container.Registry
			if err := BindClient(&r, p.Client()); err != nil {
				t.Fatal(err)
			}
			calls := 0
			if err := BindEventStore(&r, func(ctx context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
				calls++
				return tc.selector(ctx)
			}); err != nil {
				t.Fatal(err)
			}
			provider, err := r.Build()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = provider.Close(context.Background()) }()
			for scopeIndex := range 2 {
				scope, err := provider.NewScope(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				var frozen *SelectorError
				for range 2 {
					_, err := dependencyinjection.Resolve[*chronicle.EventStore](t.Context(), scope)
					var selection *SelectorError
					if !errors.As(err, &selection) || !errors.Is(err, tc.cause) {
						t.Fatalf("selection error = %v", err)
					}
					if frozen != nil && selection != frozen {
						t.Fatal("selector failure was not retained")
					}
					frozen = selection
					if errors.Is(err, payload) {
						t.Fatal("panic payload retained")
					}
					if tc.retained == nil && errors.Is(err, ordinary) {
						t.Fatal("panic alias retained")
					}
					for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
						if strings.Contains(fmt.Sprintf(format, selection), "private") {
							t.Fatal("sensitive selection diagnostic")
						}
					}
				}
				if calls != scopeIndex+1 {
					t.Fatalf("selector calls = %d", calls)
				}
				if err := scope.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	if inspected {
		t.Fatal("opaque error inspected")
	}
}

func TestSelectionCellWaiterCancellationDoesNotOwnOutcome(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cell := &storeSelection{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		started, release, completed := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var calls atomic.Int32
		selector := func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
			calls.Add(1)
			close(started)
			<-release
			return "store", "namespace", nil
		}
		go func() {
			defer close(completed)
			if _, _, err := cell.selectStore(context.Background(), selector); err != nil {
				t.Error(err)
			}
		}()
		<-started
		waited := make(chan error, 1)
		go func() { _, _, err := cell.selectStore(ctx, selector); waited <- err }()
		synctest.Wait()
		cancel()
		if err := <-waited; !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		close(release)
		<-completed
		if name, namespace, err := cell.selectStore(context.Background(), selector); err != nil || name != "store" || namespace != "namespace" {
			t.Fatal("waiter changed outcome", err)
		}
		if calls.Load() != 1 {
			t.Fatal("selector reran")
		}
	})
}

func TestSelectorOwnerCancellationIsFrozenDespiteCallbackSuccess(t *testing.T) {
	p, err := chronicle.CaptureClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Client().Close() }()
	var r container.Registry
	if err := BindClient(&r, p.Client()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	calls := 0
	if err := BindEventStore(&r, func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
		calls++
		cancel()
		return "store", "namespace", nil
	}); err != nil {
		t.Fatal(err)
	}
	provider, err := r.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = provider.Close(context.Background()) }()
	scope, err := provider.NewScope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dependencyinjection.Resolve[*chronicle.EventStore](ctx, scope); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := dependencyinjection.Resolve[*chronicle.EventStore](t.Context(), scope); !errors.Is(err, context.Canceled) {
			t.Fatal("live caller retried frozen cancellation", err)
		}
	}
	if calls != 1 {
		t.Fatal("owner cancellation reselected")
	}
	// A new scope makes a new selection attempt.
	other, err := provider.NewScope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dependencyinjection.Resolve[*chronicle.EventStore](t.Context(), other); !errors.Is(err, chronicle.ErrNotPrepared) {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatal("new scope did not reselect")
	}
}

func TestSelectionCellIsCachedBeforeClientFactoryFailureOrNil(t *testing.T) {
	p, err := chronicle.CaptureClient()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = p.Client().Close() }()
	var r container.Registry
	attempts, selections := 0, 0
	failure := errors.New("client factory failure")
	if err := dependencyinjection.BindBorrowed(&r, dependencyinjection.Singleton, func(context.Context, dependencyinjection.Resolver) (*chronicle.Client, error) {
		attempts++
		switch attempts {
		case 1:
			return nil, failure
		case 2:
			return nil, nil
		default:
			return p.Client(), nil
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := BindEventStore(&r, func(context.Context) (chronicle.StoreName, chronicle.Namespace, error) {
		selections++
		return "store", "namespace", nil
	}); err != nil {
		t.Fatal(err)
	}
	provider, err := r.Build()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = provider.Close(context.Background()) }()
	scope, err := provider.NewScope(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var cell *storeSelection
	for _, expected := range []error{failure, dependencyinjection.ErrNilValue, chronicle.ErrNotPrepared, chronicle.ErrNotPrepared} {
		if _, err := dependencyinjection.Resolve[*chronicle.EventStore](t.Context(), scope); !errors.Is(err, expected) {
			t.Fatal(err)
		}
		got, err := dependencyinjection.Resolve[*storeSelection](t.Context(), scope)
		if err != nil {
			t.Fatal(err)
		}
		if cell != nil && got != cell {
			t.Fatal("dependency failure discarded cell")
		}
		cell = got
	}
	if selections != 1 || attempts != 3 {
		t.Fatal("unexpected attempts", selections, attempts)
	}
}
