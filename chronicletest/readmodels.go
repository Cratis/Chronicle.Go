// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/observerruntime"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/reducers"
)

// ReadModelOptions configures a scenario before production registry compilation.
// Projection replaces the discovered producer (including a reducer). Initial
// supplies reducer state, copied through production serialization at construction;
// initial projection state is unsupported and fails explicitly.
type ReadModelOptions[M any] struct {
	// Projection replaces the registered producer for M in this fixture only.
	Projection *projections.Declaration
	// Initial is a serialized snapshot used at the start of each source's fold.
	Initial *M
}

// ReadModelScenario folds reducers in-process or replays projections on the real
// kernel. It never implements projection evaluation in Go. Result reads replay
// collected events; handler code should be deterministic. One fold scope is opened
// per source; unlike C#'s multi-source single fold, sources cannot corrupt each other.
type ReadModelScenario[M any] struct {
	config        Config
	client        *chronicle.Client
	eventScenario *EventScenario
	artifacts     chronicle.Artifacts
	model         readmodels.Descriptor
	reducer       *reducers.Plan
	projection    projections.Definition
	history       []reducers.Event
	initial       json.RawMessage
	seeds         *seededModels
	closed        bool
}

// OpenReadModelScenario selects the registered producer for M. At most one options
// value is accepted. Inline projection precedence is applied to a detached registry.
// Projections require Kernel even with no seeded events, refusing false green tests.
func OpenReadModelScenario[M any](ctx context.Context, config Config, options ...ReadModelOptions[M]) (*ReadModelScenario[M], error) {
	if len(options) > 1 {
		return nil, chronicle.ErrInvalidConfiguration
	}
	var option ReadModelOptions[M]
	if len(options) == 1 {
		option = options[0]
	}
	if option.Projection != nil {
		if option.Projection.Model().GoType() != reflect.TypeFor[M]() {
			return nil, chronicle.ErrInvalidConfiguration
		}
		var err error
		config.Registry, err = config.Registry.WithProjection(*option.Projection)
		if err != nil {
			return nil, err
		}
	}
	config, clientOptions, err := configuration(config)
	if err != nil {
		return nil, err
	}
	client, err := chronicle.NewClient(clientOptions...)
	if err != nil {
		return nil, err
	}
	s := &ReadModelScenario[M]{config: config, client: client}
	fail := func(err error) (*ReadModelScenario[M], error) { return nil, errors.Join(err, s.Close()) }
	s.artifacts, err = client.Artifacts(config.Store)
	if err != nil {
		return fail(err)
	}
	var ok bool
	s.model, ok = s.artifacts.ReadModels.LookupType(reflect.TypeFor[M]())
	if !ok {
		return fail(chronicle.ErrNotRegistered)
	}
	s.seeds, err = newSeededModels(config, s.artifacts.ReadModels)
	if err != nil {
		return fail(err)
	}
	for _, plan := range s.artifacts.Reducers {
		if plan.Model().GoType() == reflect.TypeFor[M]() {
			s.reducer = plan
		}
	}
	for _, definition := range s.artifacts.Projections {
		if definition.Model().GoType() == reflect.TypeFor[M]() {
			s.projection = definition
		}
	}
	if s.reducer != nil {
		if option.Initial != nil {
			s.initial, err = s.model.Marshal(option.Initial)
			if err != nil {
				return fail(err)
			}
		}
		return s, nil
	}
	if s.projection.Identifier() == "" {
		return fail(fmt.Errorf("%w: no read-model producer", chronicle.ErrNotRegistered))
	}
	if config.Engine != Kernel {
		return fail(fmt.Errorf("%w: projections require the real kernel", ErrFidelityUnavailable))
	}
	if option.Initial != nil {
		return fail(fmt.Errorf("%w: projection initial state", chronicle.ErrUnsupported))
	}
	if len(s.artifacts.Reactors) != 0 || len(s.artifacts.Reducers) != 0 {
		return fail(fmt.Errorf("%w: projection scenario registry contains other Go observers; isolate the registry or use EventScenario for live lifecycle", chronicle.ErrUnsupported))
	}
	if config.ConnectionString == "" {
		return fail(ErrKernelUnavailable)
	}
	store, err := client.EventStore(ctx, config.Store, chronicle.WithNamespace(config.Namespace))
	if err != nil {
		return fail(err)
	}
	s.eventScenario = &EventScenario{Client: client, Store: store, fidelity: kernelFidelity()}
	return s, nil
}

// NewReadModelScenario registers testing.T cleanup and skips only a missing kernel endpoint.
func NewReadModelScenario[M any](t *testing.T, config Config, options ...ReadModelOptions[M]) *ReadModelScenario[M] {
	t.Helper()
	s, err := OpenReadModelScenario[M](t.Context(), config, options...)
	if err != nil {
		constructionFailure(t, err)
	}
	cleanup(t, s.Close)
	return s
}

// Fidelity reports local fold substitutions or kernel replay boundaries. Projection
// replay does not establish sink persistence or observer catch-up even on a kernel.
func (s *ReadModelScenario[M]) Fidelity() Fidelity {
	if s.reducer != nil {
		return localFidelity()
	}
	f := kernelFidelity()
	f.substituted = append(f.substituted, ObserverLifecycle, ReadModelStorage, DurableStorage)
	return f
}

// Given snapshots events for local folding, or appends them through the production
// client to the projection's selected sequence. Unsubscribed events are ignored by
// folds and filtered by the kernel for projections. Failures retain earlier seeds.
func (s *ReadModelScenario[M]) Given(ctx context.Context, source events.SourceID, values ...any) error {
	if s.closed {
		return chronicle.ErrClosed
	}
	if strings.TrimSpace(string(source)) == "" {
		return chronicle.ErrInvalidConfiguration
	}
	for _, value := range values {
		if err := ctx.Err(); err != nil {
			return err
		}
		descriptor, ok := s.artifacts.Events.Lookup(value)
		if !ok {
			return chronicle.ErrNotRegistered
		}
		data, err := descriptor.Marshal(value)
		if err != nil {
			return err
		}
		sequence := s.model.EventSequence()
		ec := synthetic(s.config, sequence, descriptor, source, events.SequenceNumber(len(s.history)))
		ec, content, err := observerruntime.DecodeContent(descriptor, ec, data, nil)
		if err != nil {
			return err
		}
		if s.eventScenario != nil {
			handle, err := s.eventScenario.Store.EventSequence(s.projection.EventSequence())
			if err != nil {
				return err
			}
			result, err := handle.Append(ctx, source, value)
			if err != nil {
				return err
			}
			if err = result.Err(); err != nil {
				return err
			}
		}
		s.history = append(s.history, reducers.Event{Content: content, Context: ec})
	}
	return nil
}

// Instances returns owned values keyed by the actual projected key. Kernel replay
// includes all roots, not just seeded source IDs, so joins/custom keys remain valid.
// A missing/duplicate key in the kernel response fails rather than guessing.
func (s *ReadModelScenario[M]) Instances(ctx context.Context) (map[readmodels.Key]M, error) {
	if s.closed {
		return nil, chronicle.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.reducer != nil {
		return s.reduce(ctx)
	}
	documents, err := s.eventScenario.Store.ReadModels().ReplayProjection(ctx, s.model.Identifier(), uint64(len(s.history)))
	if err != nil {
		return nil, err
	}
	result := map[readmodels.Key]M{}
	for _, document := range documents {
		key, err := modelKey(document, s.model.KeyProperty())
		if err != nil {
			return nil, err
		}
		if _, duplicate := result[key]; duplicate {
			return nil, chronicle.ErrProtocol
		}
		var value M
		if err = json.Unmarshal(document, &value); err != nil {
			return nil, err
		}
		result[key] = value
	}
	return result, nil
}
func modelKey(document json.RawMessage, property string) (readmodels.Key, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(document, &object); err != nil {
		return "", err
	}
	for _, name := range []string{property, "_id", "id", "Id", "ID"} {
		if name == "" {
			continue
		}
		raw, ok := object[name]
		if !ok {
			continue
		}
		var key string
		if err := json.Unmarshal(raw, &key); err != nil || key == "" {
			return "", fmt.Errorf("%w: read model key must be a nonempty string", chronicle.ErrProtocol)
		}
		return readmodels.Key(key), nil
	}
	return "", fmt.Errorf("%w: keyed access requires a projected ID field", ErrFidelityUnavailable)
}
func (s *ReadModelScenario[M]) reduce(ctx context.Context) (map[readmodels.Key]M, error) {
	batches := map[readmodels.Key][]reducers.Event{}
	var keys []readmodels.Key
	for _, event := range s.history {
		key := readmodels.Key(event.Context.SourceID)
		if _, ok := batches[key]; !ok {
			keys = append(keys, key)
			batches[key] = nil
		}
		if _, ok := s.reducer.Descriptor(event.Context.EventType.ID); ok {
			batches[key] = append(batches[key], event)
		}
	}
	result := map[readmodels.Key]M{}
	for _, key := range keys {
		batch := batches[key]
		var initial *M
		if s.initial != nil {
			initial = new(M)
			if err := json.Unmarshal(s.initial, initial); err != nil {
				return nil, err
			}
		}
		if len(batch) == 0 {
			if initial != nil {
				result[key] = *initial
			}
			continue
		}
		operation := reducers.WithBatch(ctx, reducers.Batch{Reducer: s.reducer.Identifier(), Store: s.config.Store, Namespace: s.config.Namespace, Sequence: s.reducer.EventSequence()})
		folded := s.reducer.Reduce(operation, batch, initial)
		if folded.Err != nil {
			return nil, folded.Err
		}
		if folded.State != nil {
			data, err := s.model.Marshal(folded.State)
			if err != nil {
				return nil, err
			}
			var value M
			if err = json.Unmarshal(data, &value); err != nil {
				return nil, err
			}
			result[key] = value
		}
	}
	return result, nil
}

// Instance returns absence or exactly one result; multiple materialized roots
// return ErrAmbiguousInstance, never the first event source's result.
func (s *ReadModelScenario[M]) Instance(ctx context.Context) (readmodels.Instance[M], error) {
	if s.closed {
		return readmodels.Instance[M]{}, chronicle.ErrClosed
	}
	if s.eventScenario != nil {
		documents, err := s.eventScenario.Store.ReadModels().ReplayProjection(ctx, s.model.Identifier(), uint64(len(s.history)))
		if err != nil {
			return readmodels.Instance[M]{}, err
		}
		if len(documents) > 1 {
			return readmodels.Instance[M]{}, ErrAmbiguousInstance
		}
		if len(documents) == 0 {
			return readmodels.Instance[M]{}, nil
		}
		var value M
		if err = json.Unmarshal(documents[0], &value); err != nil {
			return readmodels.Instance[M]{}, err
		}
		return readmodels.Instance[M]{Exists: true, Value: value}, nil
	}
	values, err := s.Instances(ctx)
	if err != nil {
		return readmodels.Instance[M]{}, err
	}
	if len(values) > 1 {
		return readmodels.Instance[M]{}, ErrAmbiguousInstance
	}
	for _, value := range values {
		return readmodels.Instance[M]{Exists: true, Value: value}, nil
	}
	return readmodels.Instance[M]{}, nil
}

// InstanceFor selects a projected key (not necessarily an event source ID).
func (s *ReadModelScenario[M]) InstanceFor(ctx context.Context, key readmodels.Key) (readmodels.Instance[M], error) {
	values, err := s.Instances(ctx)
	if err != nil {
		return readmodels.Instance[M]{}, err
	}
	value, ok := values[key]
	return readmodels.Instance[M]{Value: value, Exists: ok}, nil
}

// SeedReadModel snapshots a registered dependency independently of projected/folded
// state. It is exposed through ReadModels, not used as the scenario's initial state.
func (s *ReadModelScenario[M]) SeedReadModel(key readmodels.Key, value any) error {
	if s.closed {
		return chronicle.ErrClosed
	}
	return s.seeds.seed(key, value)
}

// ReadModels returns the production reader over explicitly seeded dependencies.
// It does not read or persist the scenario's projected/folded result.
func (s *ReadModelScenario[M]) ReadModels() *readmodels.Service { return s.seeds.service }

// Close releases local/remote clients once without deleting kernel state.
func (s *ReadModelScenario[M]) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.seeds != nil {
		s.seeds.closed = true
	}
	if s.eventScenario != nil {
		return s.eventScenario.Close()
	}
	return s.client.Close()
}
