// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/artifacts"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

// ProjectionRegistration separates retained local publication from registration
// acknowledgement. Published remains true after cancellation or remote failure.
// Use WaitForRegistration to retry; submitting the declaration again conflicts.
type ProjectionRegistration struct {
	Published bool                // Published means the complete local desired snapshot includes the addition.
	Outcome   RegistrationOutcome // Outcome is the real registration pass, not materialization or attachment evidence.
}

// RegisterProjection adds one new ordinary projection and its new read model.
// It reuses the existing declaration/builders and exact known event generations.
// Replacements, variants, globals, protected or relationship/derived shapes and
// other runtime artifact families are unsupported. Existing readers keep their
// original snapshots; new ReadModels calls select the new root. Publication
// invalidates every previously issued/enrolled decision guard in this store.
//
// Cancellation before publication changes nothing. After publication the desired
// addition survives failure and reconnect. ErrDestructiveRegistrationUnknown
// refuses future additions but does not withdraw ordinary acknowledged readiness.
// It can accompany Published=true and a successful Outcome after a cumulative
// attempt became uncertain and a retry acknowledged the same root.
func (s *EventStore) RegisterProjection(ctx context.Context, declaration projections.Declaration) (result ProjectionRegistration, err error) {
	if s == nil || s.storeOwner == nil || s.client == nil || nilValue(ctx) {
		return result, ErrInvalidConfiguration
	}
	if err = ctx.Err(); err != nil {
		return result, err
	}
	if declaration.IsGlobal() || declaration.IsVariant() {
		return result, fmt.Errorf("%w: runtime variants and globals", ErrUnsupported)
	}
	if declaration.Model().GoType() == nil || declaration.Identifier() == "" {
		return result, ErrInvalidConfiguration
	}
	if err = s.waitDefinitionFlight(ctx); err != nil {
		return result, err
	}
	base := s.definitionRoot()
	if err = validateProjectionAddition(base.snapshot, declaration.Identifier(), declaration.Model()); err != nil {
		return result, err
	}
	var delta projections.Definition
	err = artifacts.Protect("projection", "runtime preparation", func() error {
		var prepareErr error
		delta, prepareErr = s.prepareProjectionAddition(declaration)
		return prepareErr
	})
	if err != nil {
		return result, err
	}
	for {
		if err = s.waitDefinitionFlight(ctx); err != nil {
			return result, err
		}
		base = s.definitionRoot()
		if err = validateProjectionAddition(base.snapshot, delta.Identifier(), delta.Model()); err != nil {
			return result, err
		}
		snapshot := base.snapshot
		snapshot.models, err = readmodels.NewCatalog(append(base.snapshot.models.Descriptors(), delta.Model())...)
		if err != nil {
			return result, err
		}
		snapshot.projections = append(slices.Clone(base.snapshot.projections), delta)
		root := newDefinitionRoot(snapshot, s.definitions.clock, base.revision+1)
		root.previous, root.delta = base.revision, delta
		// Prepare namespace readers outside the publication lock. If acquisition
		// adds a namespace, repeat this callback-free composition, not preparation.
		readers := make(map[*storeOwner]*readmodels.Service)
		for {
			for _, store := range s.client.storeOwnerSnapshot() {
				if store.name != s.name || readers[store] != nil {
					continue
				}
				readers[store], err = (&EventStore{storeOwner: store}).readersFor(root)
				if err != nil {
					return result, err
				}
			}
			s.client.mu.Lock()
			switch {
			case s.client.closed:
				err = ErrClosed
			case ctx.Err() != nil:
				err = ctx.Err()
			case s.definitions.destructiveUnknown:
				err = ErrDestructiveRegistrationUnknown
			}
			if err != nil {
				s.client.mu.Unlock()
				return result, err
			}
			if s.definitions.root != base || s.definitions.flight {
				s.client.mu.Unlock()
				break
			}
			complete := true
			for _, store := range s.client.storeOwners {
				if store.name == s.name && readers[store] == nil {
					complete = false
					break
				}
			}
			if !complete {
				s.client.mu.Unlock()
				continue
			}
			// Fixed expected epochs make even old readers unable to mint current
			// evidence. Invalidation precedes visibility of every new reader/root.
			s.definitions.clock.Store(root.revision)
			s.definitions.root = root
			for store, service := range readers {
				store.latestReaders, store.readerRoot = service, root
			}
			s.definitions.notifyLocked()
			s.client.mu.Unlock()
			result.Published = true
			result.Outcome, err = s.WaitForRegistration(ctx)
			s.client.mu.Lock()
			unknown := s.definitions.destructiveUnknown
			s.client.mu.Unlock()
			if unknown {
				err = errors.Join(err, ErrDestructiveRegistrationUnknown)
			}
			return result, err
		}
	}
}

func (s *EventStore) prepareProjectionAddition(declaration projections.Declaration) (projections.Definition, error) {
	if err := ordinaryModel(declaration.Model()); err != nil {
		return projections.Definition{}, err
	}
	output := s.client.registryOutput
	if selected := s.client.storeRegistryOutputs[s.name]; selected != nil {
		output = selected
	}
	definition, err := projections.Compile(declaration, output.authoring.events)
	if err != nil {
		return projections.Definition{}, err
	}
	wire := definition.KernelDefinition()
	if definition.IsPassive() || wire.SubscribesToAllEvents || len(wire.Children)+len(wire.Nested)+len(wire.Join)+len(wire.RemovedWithJoin)+len(wire.FromEvery) != 0 || wire.FromEventProperty != nil {
		return projections.Definition{}, fmt.Errorf("%w: runtime registration requires a basic active projection", ErrUnsupported)
	}
	for _, from := range wire.From {
		if err := ordinaryEvent(output.authoring.events, events.TypeRef{ID: events.TypeID(from.Key.Id), Generation: events.Generation(from.Key.Generation)}); err != nil {
			return projections.Definition{}, err
		}
	}
	for _, removal := range wire.RemovedWith {
		if err := ordinaryEvent(output.authoring.events, events.TypeRef{ID: events.TypeID(removal.Key.Id), Generation: events.Generation(removal.Key.Generation)}); err != nil {
			return projections.Definition{}, err
		}
	}
	model, err := definition.Model().WithNamingPolicy(s.client.config.naming)
	if err != nil {
		return projections.Definition{}, err
	}
	model, err = model.WithDefaultSinkType(s.client.config.defaultSinkType)
	if err != nil {
		return projections.Definition{}, err
	}
	definition, err = definition.Rebind(model, output.authoring.events, output.frozen.events)
	if err != nil {
		return projections.Definition{}, err
	}
	definition, err = definition.ForStore(string(s.name))
	if err != nil {
		return projections.Definition{}, err
	}
	if source, _ := definition.ExternalSubscription(); source != "" && source != string(s.name) {
		return projections.Definition{}, fmt.Errorf("%w: runtime external subscriptions", ErrUnsupported)
	}
	return definition, nil
}

func ordinaryModel(model readmodels.Descriptor) error {
	roots, err := serialization.ProtectionRoots(model.Schema())
	if err != nil {
		return err
	}
	if len(roots) != 0 {
		return fmt.Errorf("%w: runtime protected model", ErrUnsupported)
	}
	for _, field := range model.Fields() {
		if field.Collection || !field.Scalar.IsPrimitive() {
			return fmt.Errorf("%w: runtime non-scalar model", ErrUnsupported)
		}
	}
	return nil
}

func ordinaryEvent(catalog *events.Catalog, ref events.TypeRef) error {
	event, ok := catalog.LookupRef(ref)
	if !ok {
		return ErrNotRegistered
	}
	roots, err := serialization.ProtectionRoots(event.Schema())
	if err != nil {
		return err
	}
	if len(roots) != 0 {
		return fmt.Errorf("%w: runtime protected input", ErrUnsupported)
	}
	for _, field := range event.Fields() {
		if field.Collection || !field.Scalar.IsPrimitive() {
			return fmt.Errorf("%w: runtime non-scalar input", ErrUnsupported)
		}
	}
	return nil
}

func validateProjectionAddition(snapshot registrySnapshot, id string, model readmodels.Descriptor) error {
	for _, existing := range snapshot.models.Descriptors() {
		_, producer := existing.Observer()
		if existing.Identifier() == model.Identifier() || existing.GoType() == model.GoType() || existing.ContainerName() == model.ContainerName() || producer == id {
			return fmt.Errorf("%w: runtime model identity, container or producer already registered", ErrInvalidConfiguration)
		}
	}
	for _, existing := range snapshot.projections {
		if existing.Identifier() == id {
			return fmt.Errorf("%w: projection already registered", ErrInvalidConfiguration)
		}
	}
	for _, existing := range snapshot.reactors {
		if string(existing.Identifier()) == id {
			return fmt.Errorf("%w: observer identity already registered", ErrInvalidConfiguration)
		}
	}
	for _, existing := range snapshot.reducers {
		if string(existing.Identifier()) == id {
			return fmt.Errorf("%w: observer identity already registered", ErrInvalidConfiguration)
		}
	}
	for _, existing := range snapshot.readModelReactors {
		if string(existing.Identifier()) == id {
			return fmt.Errorf("%w: observer identity already registered", ErrInvalidConfiguration)
		}
	}
	return nil
}

func (s *EventStore) readersFor(root *definitionRoot) (*readmodels.Service, error) {
	validator, err := projectionReplayValidatorFor(root.snapshot)
	if err != nil {
		return nil, err
	}
	// Reducer plans cannot change in this workflow. Closures retain the original
	// handle's compiled plans, never a new producer selected by model identity.
	return readmodels.New(s.name, s.namespace, root.snapshot.models, &clientTransport{client: s.client, store: s, decisionSnapshot: root.decisions}, readmodels.WithReleasedPassiveReader(s.readPassiveReducer), readmodels.WithReducerCollectionReader(s.readReducerCollection), readmodels.WithProjectionReplayValidator(validator), readmodels.WithSnapshotEventCatalog(root.snapshot.events), readmodels.WithReductionChanges(&s.readModelChanges))
}
