// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"reflect"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/seeding"
)

type seederDeclaration struct {
	instance seeding.Seeder
	typ      reflect.Type
	factory  any
}

// RegisterSeeder borrows a seeder for definition preparation. Seed runs once per
// selected registry snapshot at NewClient, never at registration or reconnect.
// The caller owns the instance; Chronicle never closes it. Registration order is
// preserved. Duplicate concrete seeder types are rejected (use RegisterSeederFunc
// for independent callbacks). Adding seeders cannot mutate an existing client.
func RegisterSeeder(registry *Registry, seeder seeding.Seeder) error {
	if nilValue(seeder) {
		return fmt.Errorf("%w: nil seeder", ErrInvalidConfiguration)
	}
	return registerSeeder(registry, seederDeclaration{instance: seeder, typ: reflect.TypeOf(seeder)})
}

// RegisterSeederFunc registers a plain function. Closures are independent
// declarations, even when their Go function type is identical. Captures are
// borrowed and may be accessed concurrently by separate NewClient calls.
func RegisterSeederFunc(registry *Registry, seed func(*seeding.Builder) error) error {
	if seed == nil {
		return fmt.Errorf("%w: nil seeder function", ErrInvalidConfiguration)
	}
	return registerSeeder(registry, seederDeclaration{instance: seeding.Func(seed)})
}

// RegisterSeederFactory declares scoped construction of S during preparation.
// A constructor returns S or (S,error) and may accept context.Context followed by
// dependencies (or the operation Scope). Nil selects service/default activation.
// The default path uses plain Go construction, with no DI container. WithServices
// opts into a resolver; registered-service precedence and cleanup match observers.
// Each seeder gets one scope: open, construct, Seed, close before NewClient returns.
func RegisterSeederFactory[S seeding.Seeder](registry *Registry, factory any) error {
	return registerSeeder(registry, seederDeclaration{typ: reflect.TypeFor[S](), factory: factory})
}

func registerSeeder(registry *Registry, declaration seederDeclaration) error {
	if registry == nil {
		return fmt.Errorf("%w: nil seeder registry", ErrInvalidConfiguration)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, existing := range registry.seeders {
		if declaration.typ != nil && existing.typ == declaration.typ {
			return fmt.Errorf("%w: duplicate seeder type %s", ErrInvalidConfiguration, declaration.typ)
		}
	}
	registry.seeders = append(registry.seeders, declaration)
	return nil
}

func prepareSeeders(ctx context.Context, catalog *events.Catalog, declarations []seederDeclaration, plans registryFactoryPlans) (seeding.Definition, error) {
	return seeding.Prepare(catalog, seeding.Func(func(builder *seeding.Builder) error {
		for i, declaration := range declarations {
			if err := ctx.Err(); err != nil {
				return err
			}
			if declaration.instance != nil {
				if err := artifacts.Protect("seeder", "define", func() error { return declaration.instance.Seed(builder) }); err != nil {
					return err
				}
			} else if err := prepareScopedSeeder(ctx, builder, plans, plans.seeders[i]); err != nil {
				return err
			}
		}
		return ctx.Err()
	}))
}

func prepareScopedSeeder(ctx context.Context, builder *seeding.Builder, plans registryFactoryPlans, constructor artifacts.Constructor) error {
	return artifacts.Prepare(ctx, "seeder", plans.services, constructor, plans.check, func(value any) error {
		return value.(seeding.Seeder).Seed(builder)
	})
}
