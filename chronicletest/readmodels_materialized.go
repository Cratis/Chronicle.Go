// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/observation"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

type materializedSeed struct {
	ref      events.TypeRef
	position events.SequenceNumber
}

func materializedProjectionMembership(definition projections.Definition, catalog *events.Catalog) (*projectionSubscription, error) {
	if definition.IsPassive() || definition.Model().Sink().Type == readmodels.NoSink {
		return nil, fmt.Errorf("%w: materialized scenarios require an active projection with a sink", chronicle.ErrUnsupported)
	}
	return projectionMembership(definition, catalog)
}

func (s *ReadModelScenario[M]) materializedDocuments(ctx context.Context) ([]json.RawMessage, error) {
	if s.materializationErr != nil {
		return nil, s.materializationErr
	}
	if len(s.history) == 0 {
		return []json.RawMessage{}, nil
	}
	if err := s.awaitMaterialization(ctx); err != nil {
		return nil, err
	}
	collection, err := s.eventScenario.Store.ReadModels().GetAll(ctx, s.model.Identifier(), nil)
	if err != nil {
		return nil, err
	}
	documents := make([]json.RawMessage, 0, len(collection.Instances))
	tail := s.materializedSeeds[len(s.materializedSeeds)-1].position
	for _, instance := range collection.Instances {
		if instance.LastHandled != nil && *instance.LastHandled > tail {
			return nil, chronicle.ErrProtocol
		}
		documents = append(documents, instance.Value)
	}
	return documents, nil
}

func (s *ReadModelScenario[M]) awaitMaterialization(ctx context.Context) error {
	refs := make([]events.TypeRef, len(s.materializedSeeds))
	positions := make([]events.SequenceNumber, len(s.materializedSeeds))
	var required events.SequenceNumber
	for i, seed := range s.materializedSeeds {
		refs[i], positions[i] = seed.ref, seed.position
		if _, ok := s.membership.ids[seed.ref.ID]; ok {
			required = max(required, seed.position)
		}
	}
	completion, err := observation.NewCompletion(s.config.Store, s.config.Namespace, s.projection.EventSequence(), refs, positions)
	if err != nil {
		return err
	}
	observers := s.eventScenario.Store.Observers()
	id := observation.ID(s.projection.Identifier())
	tail := positions[len(positions)-1]
	outstanding := errors.New("projection observer completion not established")
	for {
		result, err := observers.WaitForCompletion(ctx, completion, 0)
		if err != nil {
			return errors.Join(ErrMaterializationIncomplete, outstanding, err)
		}
		if len(result.FailedPartitions()) != 0 {
			return errors.Join(ErrMaterializationIncomplete, errors.New("completion reports failed partitions"))
		}
		observer, err := observers.Get(ctx, id, s.projection.EventSequence())
		if err != nil {
			return errors.Join(ErrMaterializationIncomplete, err)
		}
		switch {
		case observer == nil:
			outstanding = errors.New("projection observer missing")
		case observer.LastHandled() != events.Unavailable && observer.LastHandled() > tail:
			return fmt.Errorf("%w: projection observer handled beyond scenario tail", chronicle.ErrProtocol)
		case observer.LastHandled() == events.Unavailable || observer.LastHandled() < required:
			outstanding = errors.New("projection observer has not handled subscribed seed tail")
		default:
			failed, err := observers.FailedPartitions(ctx, id)
			if err != nil {
				return errors.Join(ErrMaterializationIncomplete, err)
			}
			if len(failed) != 0 {
				return errors.Join(ErrMaterializationIncomplete, errors.New("projection observer has failed partitions"))
			}
			if result.IsSuccess() {
				return nil
			}
			outstanding = fmt.Errorf("completion outstanding: observers=%d, timed out=%t", len(result.OutstandingObservers()), result.TimedOut())
		}
		// Retry processing evidence only. Never poll or synthesize sink content.
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ErrMaterializationIncomplete, outstanding, ctx.Err())
		case <-timer.C:
		}
	}
}
