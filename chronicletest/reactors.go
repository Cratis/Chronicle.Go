// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/observerruntime"
	"github.com/cratis/chronicle.go/reactors"
	"github.com/cratis/chronicle.go/readmodels"
)

// RecordingReactorSideEffectHandlers flattens finite collections and targeted
// event/batch wrappers. It does not execute commands or append events and does not
// prove production acceptance. Payload objects are borrowed; Produced copies only
// the outer slice. Serial use only; the zero value is ready to use.
type RecordingReactorSideEffectHandlers struct{ produced []any }

// RecordEffect implements reactors.EffectRecorder. Declared return types still
// undergo production compilation; custom commands must have a registered handler.
func (r *RecordingReactorSideEffectHandlers) RecordEffect(ctx context.Context, _ reactors.SideEffectContext, value any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	flattened, err := flatten(value, 0)
	if err != nil {
		return err
	}
	r.produced = append(r.produced, flattened...)
	return nil
}
func flatten(value any, depth int) ([]any, error) {
	if depth > 64 {
		return nil, chronicle.ErrInvalidConfiguration
	}
	if value == nil {
		return nil, nil
	}
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil, nil
		}
		v = v.Elem()
	}
	switch item := v.Interface().(type) {
	case eventsequences.Entry:
		return flatten(item.Event, depth+1)
	case eventsequences.EventsWithConcurrencyScopes:
		var result []any
		for _, entry := range item.Events {
			values, err := flatten(entry.Event, depth+1)
			if err != nil {
				return nil, err
			}
			result = append(result, values...)
		}
		return result, nil
	}
	if v.Kind() == reflect.Slice || v.Kind() == reflect.Array {
		var result []any
		for i := 0; i < v.Len(); i++ {
			values, err := flatten(v.Index(i).Interface(), depth+1)
			if err != nil {
				return nil, err
			}
			result = append(result, values...)
		}
		return result, nil
	}
	return []any{value}, nil
}

// Produced returns a detached slice of borrowed effect payloads in return order.
func (r *RecordingReactorSideEffectHandlers) Produced() []any {
	return append([]any(nil), r.produced...)
}

// ReactorScenario activates registered R and invokes its real production plan.
// Each Given call is a batch scope (PerEvent registrations open one per event).
// Sequence numbers increase across calls, including failed delivered events.
// No kernel observer, retry, checkpoint or quarantine is simulated.
type ReactorScenario[R any] struct {
	config    Config
	client    *chronicle.Client
	artifacts chronicle.Artifacts
	plan      *reactors.Plan
	recorder  RecordingReactorSideEffectHandlers
	seeds     *seededModels
	next      events.SequenceNumber
	closed    bool
}

// OpenReactorScenario freezes the normal production registry with its naming,
// services, middleware and declared side-effect extension types. R must be a
// registered artifact; no scenario-only method discovery exists. Context is used
// for cancellation, but construction performs no I/O and opens no service scope.
func OpenReactorScenario[R any](ctx context.Context, config Config) (*ReactorScenario[R], error) {
	return openReactorScenario[R](ctx, config, "")
}

// OpenReactorScenarioForID selects a registered reactor by ID, including explicit
// callback declarations without an artifact type. It shares the production plan
// pipeline and recording behavior with OpenReactorScenario.
func OpenReactorScenarioForID(ctx context.Context, config Config, id reactors.ID) (*ReactorScenario[any], error) {
	if id == "" {
		return nil, chronicle.ErrInvalidConfiguration
	}
	return openReactorScenario[any](ctx, config, id)
}

func openReactorScenario[R any](ctx context.Context, config Config, id reactors.ID) (*ReactorScenario[R], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	config, options, err := configuration(config)
	if err != nil {
		return nil, err
	}
	client, err := chronicle.NewClient(options...)
	if err != nil {
		return nil, err
	}
	s := &ReactorScenario[R]{config: config, client: client}
	fail := func(err error) (*ReactorScenario[R], error) { return nil, errors.Join(err, s.Close()) }
	s.artifacts, err = client.Artifacts(config.Store)
	if err != nil {
		return fail(err)
	}
	for _, plan := range s.artifacts.Reactors {
		if (id != "" && plan.Identifier() == id) || (id == "" && plan.GoType() == reflect.TypeFor[R]()) {
			s.plan = plan
		}
	}
	if s.plan == nil {
		return fail(chronicle.ErrNotRegistered)
	}
	s.seeds, err = newSeededModels(config, s.artifacts.ReadModels)
	if err != nil {
		return fail(err)
	}
	return s, nil
}

// NewReactorScenario constructs a testing.T-owned fixture with automatic cleanup.
func NewReactorScenario[R any](t *testing.T, config Config) *ReactorScenario[R] {
	t.Helper()
	s, err := OpenReactorScenario[R](t.Context(), config)
	if err != nil {
		constructionFailure(t, err)
	}
	cleanup(t, s.Close)
	return s
}

// Fidelity reports synthetic delivery, seeded storage and recording substitutions.
func (s *ReactorScenario[R]) Fidelity() Fidelity { return localFidelity() }

// SeedReadModel snapshots a registered model for handler argument materialization.
// Replacing a seed affects subsequent Given calls, never a retained handler scope.
func (s *ReactorScenario[R]) SeedReadModel(key readmodels.Key, value any) error {
	if s.closed {
		return chronicle.ErrClosed
	}
	return s.seeds.seed(key, value)
}

// ReadModels returns the production reader backed by scenario-local seeded JSON.
// Its lifetime ends at Close. It never queries a kernel or claims sink fidelity.
func (s *ReactorScenario[R]) ReadModels() *readmodels.Service { return s.seeds.service }

// Produced returns recorded handler results, not accepted/persisted events.
func (s *ReactorScenario[R]) Produced() []any { return s.recorder.Produced() }

// Given invokes handlers in order. Returned errors/panics/cleanup failures surface
// immediately; earlier external handler actions and recordings are not rolled back.
func (s *ReactorScenario[R]) Given(ctx context.Context, source events.SourceID, values ...any) (err error) {
	if s.closed {
		return chronicle.ErrClosed
	}
	if strings.TrimSpace(string(source)) == "" {
		return chronicle.ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx = reactors.WithBatch(ctx, reactors.Batch{Reactor: s.plan.Identifier(), Store: s.config.Store, Namespace: s.config.Namespace, Sequence: s.plan.EventSequence()})
	var lease *reactors.Lease
	defer func() {
		if lease != nil {
			err = errors.Join(err, closeLease(ctx, lease.Close))
		}
	}()
	for _, value := range values {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.next >= events.Unavailable-2 {
			return chronicle.ErrProtocol
		}
		descriptor, ok := s.artifacts.Events.Lookup(value)
		if !ok {
			return chronicle.ErrNotRegistered
		}
		data, err := descriptor.Marshal(value)
		if err != nil {
			return err
		}
		ec := synthetic(s.config, s.plan.EventSequence(), descriptor, source, s.next)
		ec, content, err := observerruntime.DecodeContent(descriptor, ec, data, nil)
		if err != nil {
			return err
		}
		deliveryCtx := observerruntime.ReactorInvocationContext(ctx, s.plan.Identifier(), ec)
		if lease == nil {
			activationCtx := ctx
			if s.plan.PerEvent() {
				activationCtx = deliveryCtx
			}
			lease, err = s.plan.Activate(activationCtx)
			if err != nil {
				return err
			}
		}
		s.next++
		if err = lease.Invoke(deliveryCtx, content, ec, s); err != nil {
			return err
		}
		if s.plan.PerEvent() {
			err = closeLease(ctx, lease.Close)
			lease = nil
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// ReadModel implements the production invoker's dependency boundary.
func (s *ReactorScenario[R]) ReadModel(ctx context.Context, _ readmodels.Descriptor, key readmodels.Key, typ reflect.Type) (any, error) {
	return s.seeds.service.GetValue(ctx, typ, key)
}

// Append refuses direct runtime writes. Returned effects go through RecordEffect.
func (s *ReactorScenario[R]) Append(context.Context, events.SourceID, any) error {
	return ErrFidelityUnavailable
}

// RecordEffect replaces effect execution with explicit recording.
func (s *ReactorScenario[R]) RecordEffect(ctx context.Context, ec reactors.SideEffectContext, value any) error {
	return s.recorder.RecordEffect(ctx, ec, value)
}

// Close releases the offline client. No live observers were created.
func (s *ReactorScenario[R]) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	if s.seeds != nil {
		s.seeds.closed = true
	}
	return s.client.Close()
}
