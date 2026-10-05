// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/clientoptions"
)

// EventScenario owns a production client and optionally a private substitute
// transport. Client and Store are borrowed until Close. No remote data is deleted.
type EventScenario struct {
	// Client is the scenario-owned production client; do not replace it.
	Client *chronicle.Client
	// Store is the borrowed isolated store handle, valid until Close.
	Store          *chronicle.EventStore
	fidelity       Fidelity
	closeTransport func() error
	closed         bool
}

// OpenEventScenario connects and registers through the production client. It
// never silently falls back to the substitute when the kernel is unavailable.
// The substitute rejects constraint registrations and unsupported RPCs.
func OpenEventScenario(ctx context.Context, config Config) (*EventScenario, error) {
	config, options, err := configuration(config)
	if err != nil {
		return nil, err
	}
	result := &EventScenario{fidelity: kernelFidelity()}
	if config.Engine == Kernel {
		if config.ConnectionString == "" {
			return nil, ErrKernelUnavailable
		}
	} else {
		conn := substituteConnection()
		result.closeTransport = conn.Close
		result.fidelity = localFidelity()
		options = append(options, clientoptions.Connection[chronicle.ClientOption](conn), chronicle.WithNoAuthentication(), chronicle.WithDefaultConcurrencyStrategy(uncheckedScenario{}))
	}
	result.Client, err = chronicle.NewClient(options...)
	if err == nil && config.Engine == Substitute {
		var artifacts chronicle.Artifacts
		artifacts, err = result.Client.Artifacts(config.Store)
		if err == nil && (len(artifacts.Constraints) != 0 || len(artifacts.Projections) != 0 || len(artifacts.Reactors) != 0 || len(artifacts.Reducers) != 0 || len(artifacts.ReadModels.Descriptors()) != 0) {
			err = fmt.Errorf("%w: substitute event scenarios accept events only; constraints, read models and observers need a kernel", ErrFidelityUnavailable)
		}
	}
	if err == nil {
		result.Store, err = result.Client.EventStore(ctx, config.Store, chronicle.WithNamespace(config.Namespace))
	}
	if err != nil {
		return nil, errors.Join(err, result.Close())
	}
	return result, nil
}

// NewEventScenario is the testing.TB-oriented constructor with automatic cleanup.
// Only an omitted kernel endpoint skips; a configured but broken server fails.
func NewEventScenario(t testing.TB, config Config) *EventScenario {
	t.Helper()
	scenario, err := OpenEventScenario(t.Context(), config)
	if err != nil {
		constructionFailure(t, err)
	}
	cleanup(t, scenario.Close)
	return scenario
}

// Fidelity describes what this engine cannot establish.
func (s *EventScenario) Fidelity() Fidelity { return s.fidelity }

// EventLog returns the production sequence handle; append results retain normal
// transport-versus-domain rejection semantics and notifications.
func (s *EventScenario) EventLog() *eventsequences.Sequence { return s.Store.EventLog() }

// Given appends seed events through the production append path in order. On
// failure earlier events remain; no retry or rollback is invented.
func (s *EventScenario) Given(ctx context.Context, source events.SourceID, values ...any) error {
	if s.closed {
		return chronicle.ErrClosed
	}
	for _, value := range values {
		result, err := s.EventLog().Append(ctx, source, value)
		if err != nil {
			return err
		}
		if err = result.Err(); err != nil {
			return err
		}
	}
	return ctx.Err()
}

type uncheckedScenario struct{}

func (uncheckedScenario) GetScope(ctx context.Context, _ *eventsequences.Sequence, _ eventsequences.ScopeFilter) (eventsequences.Scope, error) {
	return eventsequences.Scope{Expectation: eventsequences.NoCheck()}, ctx.Err()
}

// Close joins the client and substitute transport once. Subsequent Close is a no-op.
// Call only after operations have finished, as for all scenario mutation methods.
func (s *EventScenario) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	if s.Client != nil {
		err = s.Client.Close()
	}
	if s.closeTransport != nil {
		err = errors.Join(err, s.closeTransport())
	}
	return err
}
