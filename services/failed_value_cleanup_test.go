// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package services

import (
	"context"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/projections"
	di "github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

var errConstructionFailed = errors.New("construction failed")

type failedValueCleanup struct{ constructed, closed int }

// bind registers a scoped factory that fails with a partial value whose cleanup
// returns cleanupErr, and closes the built provider when the test ends.
func (f *failedValueCleanup) bind(t *testing.T, cleanupErr error) di.Provider {
	t.Helper()
	var bindings container.Registry
	if err := di.Bind(&bindings, di.Scoped, func(context.Context, di.Resolver) (*preparedArtifact, error) {
		f.constructed++
		return &preparedArtifact{close: func() error { f.closed++; return cleanupErr }}, errConstructionFailed
	}); err != nil {
		t.Fatal(err)
	}
	provider, err := bindings.Build()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := provider.Close(context.Background()); err != nil {
			t.Error("provider retained a failed scoped value", err)
		}
	})
	return provider
}

func registerFailingPreparation(t *testing.T) *chronicle.Registry {
	t.Helper()
	registry := chronicle.NewRegistry()
	model, err := chronicle.RegisterReadModel[preparedModel](registry)
	if err != nil {
		t.Fatal(err)
	}
	if err := chronicle.RegisterProjectionFactory(registry, "prepared", model.Descriptor(), func() *preparedArtifact {
		t.Fatal("registered provider service did not take precedence")
		return nil
	}, func(context.Context, *preparedArtifact) (projections.Declaration, error) {
		t.Fatal("define ran after failed construction")
		return projections.Declaration{}, nil
	}); err != nil {
		t.Fatal(err)
	}
	return registry
}

func keepsBothFailures(err, cleanupErr error) bool {
	return errors.Is(err, errConstructionFailed) && errors.Is(err, cleanupErr) && !errors.Is(err, errUnsafeDiagnostic)
}

// Fundamentals.Go v0.3.2 wraps failed-value cleanup errors so cleanup
// cancellation never triggers a construction retry. Sanitization must still
// see through that wrapper instead of discarding the whole diagnostic.
func TestDefaultProviderPreparationKeepsFailedValueCleanupErrorsWithoutRetry(t *testing.T) {
	for name, cleanupErr := range map[string]error{"ordinary": &ordinaryFailure{}, "canceled": context.Canceled} {
		t.Run(name, func(t *testing.T) {
			cleanup := &failedValueCleanup{}
			provider := cleanup.bind(t, cleanupErr)
			client, err := chronicle.NewClient(chronicle.WithRegistry(registerFailingPreparation(t)), WithServices(provider))
			if client != nil {
				t.Fatal("failed preparation published a client")
			}
			if !keepsBothFailures(err, cleanupErr) {
				t.Fatal("construction or cleanup failure was hidden", err)
			}
			assertSafeProviderDiagnostics(t, err)
			if cleanup.constructed != 1 || cleanup.closed != 1 {
				t.Fatalf("constructed=%d closed=%d", cleanup.constructed, cleanup.closed)
			}
		})
	}
}
