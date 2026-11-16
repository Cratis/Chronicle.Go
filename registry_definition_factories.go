// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/cratis/chronicle.go/constraints"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

// PreparationError reports a failed preparation boundary. Its text (including
// formatted values) never contains application error or panic payloads. Unwrap
// preserves ordinary causes for errors.Is/As. Recovered panic values are discarded.
// Failed preparation publishes no client, but cannot roll back application effects.
type PreparationError = artifacts.PreparationError

type definitionFactory struct {
	typ     reflect.Type
	factory any
}
type projectionFactoryDeclaration struct {
	definitionFactory
	id     string
	model  readmodels.Descriptor
	define func(context.Context, any) (projections.Declaration, error)
}
type constraintFactoryDeclaration struct {
	definitionFactory
	names  []string
	define func(context.Context, any) ([]constraints.Definition, error)
}
type migrationFactoryDeclaration struct {
	definitionFactory
	upgrade, previous events.Descriptor
	define            func(context.Context, any) (events.MigrationDeclaration, error)
}

// RegisterProjectionFactory declares a config-only projection producer. id and
// model are fixed before activation; define must return exactly that identity and
// registered model descriptor. Existing fluent/model-bound builders are unchanged.
// The callback runs once per captured registry per NewClient, never on reconnect.
// A factory returns P or (P,error), optionally taking context.Context followed by
// dependencies. Nil selects registered-service/default activation; a registered
// service takes precedence over an explicit constructor when Catalog is available.
// No container is needed for a plain constructor. Constructor results are owned;
// resolved services and the provider are borrowed. The temporary scope is closed
// after define, including on error. Callbacks must be synchronous and must not
// retain the borrowed resolver or resolve this client's store/sequence facades.
// CaptureClient permits *Client only through an explicit borrowed binding of its
// exact identity; it remains unprepared during callbacks. Client values and SDK
// facade artifact results are prohibited. Provider-internal graphs and hidden
// closure dependencies remain the application's responsibility.
func RegisterProjectionFactory[P any](registry *Registry, id string, model readmodels.Descriptor, factory any, define func(context.Context, P) (projections.Declaration, error)) error {
	if registry == nil || define == nil {
		return invalidFactory("projection metadata and callback required")
	}
	if strings.TrimSpace(id) == "" || model.GoType() == nil {
		return invalidFactory("projection metadata and callback required")
	}
	declaration := projectionFactoryDeclaration{definitionFactory: definitionFactory{reflect.TypeFor[P](), factory}, id: id, model: model,
		define: func(ctx context.Context, value any) (projections.Declaration, error) { return define(ctx, value.(P)) }}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if projectionIdentityTaken(registry, id, model) {
		return invalidFactory("duplicate projection identity or model")
	}
	registry.projectionFactories = append(registry.projectionFactories, declaration)
	return nil
}

// RegisterConstraintFactory declares all output names before activation. define
// must return exactly one definition per name (in any order), using events from
// this registry. Names are copied; duplicate/conflicting declarations fail atomically.
// Factory/ownership semantics match RegisterProjectionFactory. Only static
// WithMessage templates are allowed: WithMessageProvider would retain a potentially
// scoped collaborator beyond preparation and is rejected in this first slice.
func RegisterConstraintFactory[C any](registry *Registry, names []string, factory any, define func(context.Context, C) ([]constraints.Definition, error)) error {
	if registry == nil || define == nil {
		return invalidFactory("constraint names and callback required")
	}
	if len(names) == 0 {
		return invalidFactory("constraint names and callback required")
	}
	owned := slices.Clone(names)
	if err := validateConstraintFactoryNames(owned); err != nil {
		return err
	}
	declaration := constraintFactoryDeclaration{definitionFactory: definitionFactory{reflect.TypeFor[C](), factory}, names: owned,
		define: func(ctx context.Context, value any) ([]constraints.Definition, error) { return define(ctx, value.(C)) }}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if slices.ContainsFunc(owned, func(name string) bool { return constraintNameTaken(registry, name) }) {
		return invalidFactory("duplicate constraint name")
	}
	registry.constraintFactories = append(registry.constraintFactories, declaration)
	return nil
}

// RegisterEventMigrationFactory declares one adjacent generation edge before
// activation. define uses events.DefineMigration with typed endpoint handles and
// returns frozen authoring metadata, never a runtime migration callback. Endpoints
// must be the exact registered declarations and outputs must match both endpoints.
// Factory/ownership semantics match RegisterProjectionFactory.
func RegisterEventMigrationFactory[M any](registry *Registry, upgrade, previous events.Descriptor, factory any, define func(context.Context, M) (events.MigrationDeclaration, error)) error {
	if registry == nil || define == nil {
		return invalidFactory("migration endpoints and callback required")
	}
	if upgrade.GoType() == nil || previous.GoType() == nil {
		return invalidFactory("migration endpoints and callback required")
	}
	if err := validateMigrationEdge(upgrade, previous); err != nil {
		return err
	}
	declaration := migrationFactoryDeclaration{definitionFactory: definitionFactory{reflect.TypeFor[M](), factory}, upgrade: upgrade, previous: previous,
		define: func(ctx context.Context, value any) (events.MigrationDeclaration, error) {
			return define(ctx, value.(M))
		}}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if migrationEdgeTaken(registry, previous.Ref()) {
		return invalidFactory("duplicate migration edge")
	}
	registry.migrationFactories = append(registry.migrationFactories, declaration)
	return nil
}

func invalidFactory(message string) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfiguration, message)
}

func validateConstraintFactoryNames(names []string) error {
	seen := map[string]bool{}
	for _, name := range names {
		if strings.TrimSpace(name) == "" || seen[name] {
			return invalidFactory("nonblank distinct constraint names required")
		}
		seen[name] = true
	}
	return nil
}

func projectionIdentityTaken(r *Registry, id string, model readmodels.Descriptor) bool {
	return slices.ContainsFunc(r.projections, func(existing projections.Declaration) bool {
		return existing.Identifier() == id || existing.Model().GoType() == model.GoType()
	}) || slices.ContainsFunc(r.projectionFactories, func(existing projectionFactoryDeclaration) bool {
		return existing.id == id || existing.model.GoType() == model.GoType()
	})
}
func constraintNameTaken(r *Registry, name string) bool {
	return slices.ContainsFunc(r.constraints, func(existing constraints.Definition) bool {
		return existing.Name() == name
	}) || slices.ContainsFunc(r.constraintFactories, func(existing constraintFactoryDeclaration) bool {
		return slices.Contains(existing.names, name)
	})
}
func migrationEdgeTaken(r *Registry, previous events.TypeRef) bool {
	return slices.ContainsFunc(r.migrations, func(existing events.MigrationDeclaration) bool {
		return existing.Previous().Ref() == previous
	}) || slices.ContainsFunc(r.migrationFactories, func(existing migrationFactoryDeclaration) bool {
		return existing.previous.Ref() == previous
	})
}
func validateMigrationEdge(upgrade, previous events.Descriptor) error {
	from, to := previous.Ref(), upgrade.Ref()
	if !adjacentMigrationEdge(from, to) {
		return invalidFactory("migration requires same ID and adjacent increasing generations")
	}
	return nil
}

func adjacentMigrationEdge(from, to events.TypeRef) bool {
	return from.ID == to.ID && from.Generation != 0 && to.Generation > from.Generation && to.Generation-from.Generation == 1
}

// Preparation constructors may consume configuration, not facades of their own
// not-yet-prepared client. Dynamic resolver requests use the same guard. Hidden
// closure dependencies remain an application contract, not discoverable DI edges.
func definitionDependency(typ reflect.Type) error {
	base := typ
	if base.Kind() == reflect.Pointer {
		base = base.Elem()
	}
	if unavailableDefinitionDependency(base) {
		return invalidFactory("client/store/sequence dependencies are unavailable during definition preparation")
	}
	return nil
}

func unavailableDefinitionDependency(base reflect.Type) bool {
	switch base.PkgPath() {
	case "github.com/cratis/chronicle.go":
		return base.Name() == "Client" || base.Name() == "EventStore"
	case "github.com/cratis/chronicle.go/eventsequences":
		return base.Name() == "Sequence"
	default:
		return false
	}
}

func compileDefinitionFactory(family string, declaration definitionFactory, services artifacts.ScopeFactory, check func(reflect.Type) error) (constructor artifacts.Constructor, err error) {
	err = artifacts.Protect(family, "validate", func() error {
		if err := definitionResult(declaration.typ); err != nil {
			return err
		}
		var compileErr error
		constructor, compileErr = artifacts.CompileConstructor(declaration.typ, declaration.factory, services)
		if compileErr != nil {
			return compileErr
		}
		constructor = constructor.WithResultValidation(definitionResult)
		return constructor.ValidateDependencies(check)
	})
	return constructor, err
}

// registryFactoryOutput contains only accepted authoring values, not the
// constructors, definition callbacks or temporary scopes that produced them.
// Projection graph compilation and constraint composition still follow this step.
type registryFactoryOutput struct {
	projections []projections.Declaration
	constraints []constraints.Definition
	migrations  []events.MigrationDeclaration
}

func prepareDefinitionFactories(ctx context.Context, captured *registryDeclarations, plans registryFactoryPlans) (*registryFactoryOutput, error) {
	result := registryFactoryOutput{
		projections: slices.Clone(captured.projections),
		constraints: slices.Clone(captured.constraints),
		migrations:  slices.Clone(captured.migrations),
	}
	if err := result.prepareProjections(ctx, captured.projectionFactories, plans); err != nil {
		return nil, err
	}
	if err := result.prepareConstraints(ctx, captured, plans); err != nil {
		return nil, err
	}
	if err := result.prepareMigrations(ctx, captured.migrationFactories, plans); err != nil {
		return nil, err
	}
	return &result, nil
}

func (result *registryFactoryOutput) prepareProjections(ctx context.Context, declarations []projectionFactoryDeclaration, plans registryFactoryPlans) error {
	for i, declaration := range declarations {
		err := artifacts.Prepare(ctx, "projection", plans.services, plans.projections[i], plans.check, func(value any) error {
			return result.defineProjection(ctx, declaration, value)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (result *registryFactoryOutput) defineProjection(ctx context.Context, declaration projectionFactoryDeclaration, value any) error {
	output, err := declaration.define(ctx, value)
	if err != nil {
		return err
	}
	if !declaration.matches(output) {
		return invalidFactory("projection output does not match declared identity")
	}
	result.projections = append(result.projections, output)
	return nil
}

func (declaration projectionFactoryDeclaration) matches(output projections.Declaration) bool {
	return output.Identifier() == declaration.id && output.Model() == declaration.model && !output.IsGlobal()
}

func (result *registryFactoryOutput) prepareConstraints(ctx context.Context, captured *registryDeclarations, plans registryFactoryPlans) error {
	for i, declaration := range captured.constraintFactories {
		err := artifacts.Prepare(ctx, "constraint", plans.services, plans.constraints[i], plans.check, func(value any) error {
			return result.defineConstraints(ctx, declaration, value, captured.descriptors)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (result *registryFactoryOutput) defineConstraints(ctx context.Context, declaration constraintFactoryDeclaration, value any, descriptors []events.Descriptor) error {
	output, err := declaration.define(ctx, value)
	if err != nil {
		return err
	}
	if err := declaration.validateOutput(output, descriptors); err != nil {
		return err
	}
	result.constraints = append(result.constraints, output...)
	return nil
}

func (declaration constraintFactoryDeclaration) validateOutput(output []constraints.Definition, descriptors []events.Descriptor) error {
	seen := map[string]bool{}
	for _, definition := range output {
		if err := declaration.validateDefinition(definition, seen, descriptors); err != nil {
			return err
		}
	}
	if len(seen) != len(declaration.names) {
		return invalidFactory("constraint output identities do not match metadata")
	}
	return nil
}

func (declaration constraintFactoryDeclaration) validateDefinition(definition constraints.Definition, seen map[string]bool, descriptors []events.Descriptor) error {
	if !declaration.accepts(definition, seen) {
		return invalidFactory("constraint output identity or callback lifetime mismatch")
	}
	seen[definition.Name()] = true
	return validateFactoryConstraintEvents(definition, descriptors)
}

func (declaration constraintFactoryDeclaration) accepts(definition constraints.Definition, seen map[string]bool) bool {
	return slices.Contains(declaration.names, definition.Name()) && !seen[definition.Name()] && !definition.HasMessageProvider()
}

func validateFactoryConstraintEvents(definition constraints.Definition, descriptors []events.Descriptor) error {
	for _, event := range append(definition.EventTypes(), definition.RemovalTypes()...) {
		if !containsConstraintEvent(descriptors, event) {
			return invalidFactory("constraint references unregistered event")
		}
	}
	return nil
}

func (result *registryFactoryOutput) prepareMigrations(ctx context.Context, declarations []migrationFactoryDeclaration, plans registryFactoryPlans) error {
	for i, declaration := range declarations {
		err := artifacts.Prepare(ctx, "migration", plans.services, plans.migrations[i], plans.check, func(value any) error {
			return result.defineMigration(ctx, declaration, value)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (result *registryFactoryOutput) defineMigration(ctx context.Context, declaration migrationFactoryDeclaration, value any) error {
	output, err := declaration.define(ctx, value)
	if err != nil {
		return err
	}
	if !output.Upgrade().SameDeclaration(declaration.upgrade) || !output.Previous().SameDeclaration(declaration.previous) {
		return invalidFactory("migration output does not match declared endpoints")
	}
	result.migrations = append(result.migrations, output)
	return nil
}
