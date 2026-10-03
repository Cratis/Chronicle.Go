// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"context"
	"errors"
	"slices"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/seeding"
	"github.com/cratis/chronicle.go/services"
	"github.com/cratis/fundamentals.go/dependencyinjection"
	"github.com/cratis/fundamentals.go/dependencyinjection/container"
)

type seedDependency struct {
	order    *[]string
	closeErr error
}

func (d *seedDependency) Close() error {
	*d.order = append(*d.order, "close scope dependency")
	return d.closeErr
}

type scopedSeeder struct {
	dependency *seedDependency
	fail       bool
}

func (s *scopedSeeder) Seed(*seeding.Builder) error {
	*s.dependency.order = append(*s.dependency.order, "seed")
	if s.fail {
		panic("seed panic")
	}
	return nil
}
func (s *scopedSeeder) Close() error {
	*s.dependency.order = append(*s.dependency.order, "close seeder")
	return nil
}

func TestSeedersUseFundamentalsPreparationScopeAndJoinCleanupFailures(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			var order []string
			cleanupFailure := errors.New("cleanup failed")
			var bindings container.Registry
			if err := dependencyinjection.Bind[*seedDependency](&bindings, dependencyinjection.Scoped, func(context.Context, dependencyinjection.Resolver) (*seedDependency, error) {
				order = append(order, "resolve dependency")
				d := &seedDependency{order: &order}
				if fail {
					d.closeErr = cleanupFailure
				}
				return d, nil
			}); err != nil {
				t.Fatal(err)
			}
			provider, err := bindings.Build()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = provider.Close(context.Background()) }()
			registry := chronicle.NewRegistry()
			if err := chronicle.RegisterSeederFactory[*scopedSeeder](registry, func(ctx context.Context, dependency *seedDependency) *scopedSeeder {
				if ctx.Err() != nil {
					t.Error(ctx.Err())
				}
				order = append(order, "construct")
				return &scopedSeeder{dependency, fail}
			}); err != nil {
				t.Fatal(err)
			}
			client, err := chronicle.NewClientContext(t.Context(), chronicle.WithRegistry(registry), services.WithServices(provider))
			if fail {
				if client != nil || !errors.Is(err, cleanupFailure) || !errors.Is(err, chronicle.ErrInvalidConfiguration) {
					t.Fatal(client, err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if err := client.Close(); err != nil {
					t.Fatal(err)
				}
			}
			want := []string{"resolve dependency", "construct", "seed", "close seeder", "close scope dependency"}
			if !slices.Equal(order, want) {
				t.Fatal(order)
			}
		})
	}
}
