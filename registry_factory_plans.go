// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"reflect"

	"github.com/cratis/chronicle.go/internal/artifacts"
)

// Plans are compiled once across all selected registries before activation. Keep
// the original factory for compilation: defaultFactory's concrete identity controls
// explicit-constructor precedence. Only temporary scope resolution is decorated.
type registryFactoryPlans struct {
	projections, constraints, migrations, seeders []artifacts.Constructor
	services                                      artifacts.ScopeFactory
	check                                         func(reflect.Type) error
}

func preflightDefinitionFactories(d *registryDeclarations, services artifacts.ScopeFactory, client *Client) (plans registryFactoryPlans, err error) {
	if services == nil {
		services = artifacts.DefaultScopeFactory()
	}
	plans.check = func(typ reflect.Type) error {
		if typ == reflect.TypeFor[*Client]() {
			if client == nil || artifacts.IsDefaultScopeFactory(services) {
				return invalidFactory("client dependency requires an explicit borrowed captured identity")
			}
			return artifacts.ValidateService(services, typ)
		}
		return definitionDependency(typ)
	}
	plans.services = identityScopeFactory{services, client}
	err = artifacts.Protect("registry", "constructors", func() error {
		compile := func(family string, declaration definitionFactory) (artifacts.Constructor, error) {
			return compileDefinitionFactory(family, declaration, services, plans.check)
		}
		for _, d := range d.projectionFactories {
			plan, err := compile("projection", d.definitionFactory)
			if err != nil {
				return err
			}
			plans.projections = append(plans.projections, plan)
		}
		for _, d := range d.constraintFactories {
			plan, err := compile("constraint", d.definitionFactory)
			if err != nil {
				return err
			}
			plans.constraints = append(plans.constraints, plan)
		}
		for _, d := range d.migrationFactories {
			plan, err := compile("migration", d.definitionFactory)
			if err != nil {
				return err
			}
			plans.migrations = append(plans.migrations, plan)
		}
		plans.seeders = make([]artifacts.Constructor, len(d.seeders))
		for i, d := range d.seeders {
			if d.instance != nil {
				continue
			}
			plan, err := compile("seeder", definitionFactory{d.typ, d.factory})
			if err != nil {
				return err
			}
			plans.seeders[i] = plan
		}
		return nil
	})
	return plans, err
}

// Provider-internal graphs remain the provider/application's responsibility. Every
// *Client resolved directly by Chronicle must be the explicitly borrowed identity.
type identityScopeFactory struct {
	artifacts.ScopeFactory
	client *Client
}

func (f identityScopeFactory) NewScope(ctx context.Context) (artifacts.Scope, error) {
	scope, err := f.ScopeFactory.NewScope(ctx)
	if nilValue(scope) {
		return scope, err
	}
	return identityScope{scope, f.client}, err
}

type identityScope struct {
	artifacts.Scope
	client *Client
}

// UnderlyingScope preserves optional adapter access to the borrowed delivery
// scope; it is never used to unwrap the expiring preparation resolver.
func (s identityScope) UnderlyingScope() artifacts.Scope { return s.Scope }

func (s identityScope) Resolve(ctx context.Context, typ reflect.Type) (any, error) {
	if typ == reflect.TypeFor[Client]() {
		return nil, invalidFactory("client values cannot be resolved")
	}
	value, err := s.Scope.Resolve(ctx, typ)
	if err == nil && typ == reflect.TypeFor[*Client]() && (s.client == nil || value != s.client) {
		return nil, invalidFactory("client dependency is not the borrowed captured identity")
	}
	return value, err
}

// Retain the same identity check for runtime direct resolutions, preserving the
// provider's optional Catalog and the default factory's constructor precedence.
func clientIdentityServices(services artifacts.ScopeFactory, client *Client) artifacts.ScopeFactory {
	if artifacts.IsDefaultScopeFactory(services) {
		return services
	}
	wrapped := identityScopeFactory{services, client}
	if catalog, ok := services.(artifacts.Catalog); ok {
		return identityCatalogFactory{wrapped, catalog}
	}
	return wrapped
}

type identityCatalogFactory struct {
	identityScopeFactory
	artifacts.Catalog
}

// Artifact results are owned, including partial constructor failures. Permitting
// a borrowed client parameter must never permit returning that client as an artifact.
func definitionResult(typ reflect.Type) error {
	if err := definitionDependency(typ); err != nil {
		return err
	}
	base := typ
	if base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	if (base.PkgPath() == "github.com/cratis/chronicle.go/events" && base.Name() == "Catalog") ||
		(base.PkgPath() == "github.com/cratis/chronicle.go/readmodels" && (base.Name() == "Service" || base.Name() == "Catalog")) ||
		(base.PkgPath() == "github.com/cratis/chronicle.go/compliance" && base.Name() == "Manager") {
		return invalidFactory("SDK facades cannot be preparation artifacts")
	}
	return nil
}
