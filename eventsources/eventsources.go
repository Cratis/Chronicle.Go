// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package eventsources registers named event source definitions that appends can
// reference through eventsequences.WithEventSource. Registration is store-wide
// (definitions are not namespace-scoped) and is an upsert by name: supplied
// names are added or replaced and absent names survive. CLR attribute discovery,
// reading registrations and automatic definition-driven concurrency
// configuration are not ported; Observe is unsupported.
package eventsources

import (
	"context"
	"fmt"
	"strings"

	contracts "github.com/cratis/chronicle.go/contracts/eventsources"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/administration"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
)

// Name names a registered event source definition.
type Name = events.SourceName

// OutcomeUnknownError reports ambiguous dispatched administration mutations.
type OutcomeUnknownError = administration.OutcomeUnknownError

// ConcurrencyDimensions is a flags value selecting the append coordinates that
// participate in optimistic concurrency checks.
type ConcurrencyDimensions int32

const (
	// NoDimensions selects no dimension.
	NoDimensions ConcurrencyDimensions = 0
	// EventSourceID selects the event source identifier.
	EventSourceID ConcurrencyDimensions = 1
	// EventSourceType selects the event source type.
	EventSourceType ConcurrencyDimensions = 2
	// EventStreamType selects the event stream type.
	EventStreamType ConcurrencyDimensions = 4
	// EventStreamID selects the event stream identifier.
	EventStreamID ConcurrencyDimensions = 8

	allDimensions = EventSourceID | EventSourceType | EventStreamType | EventStreamID
)

// Stream declares an event stream owned by a source.
type Stream struct {
	// Name is the stream type name, matched ordinally; it must not be blank.
	Name string
	// Description is optional documentation.
	Description string
	// Concurrency selects the stream's concurrency dimensions.
	Concurrency ConcurrencyDimensions
}

// Definition declares one client-owned event source.
type Definition struct {
	// Name is the source name, matched ordinally; it must not be blank.
	Name Name
	// Description is optional documentation.
	Description string
	// Concurrency selects the source's concurrency dimensions. The SDK does not
	// infer concurrency from registrations when appending.
	Concurrency ConcurrencyDimensions
	// Streams are sent in the given order.
	Streams []Stream
}

// Service is an immutable, concurrent-safe store handle. It borrows its
// connection, starts no goroutines and never retries mutations.
type Service struct {
	store  metadata.StoreName
	client contracts.EventSourcesClient
}

// New constructs a low-level service without I/O. Prefer EventStore.EventSources
// for registration/lifecycle barriers. The caller owns the connection.
func New(store metadata.StoreName, conn grpc.ClientConnInterface) (*Service, error) {
	if strings.TrimSpace(string(store)) == "" || conn == nil {
		return nil, invalid("store and connection required")
	}
	return &Service{store, contracts.NewEventSourcesClient(conn)}, nil
}

// Store returns this service's immutable store coordinate.
func (s *Service) Store() metadata.StoreName { return s.store }

// Register adds or replaces each supplied definition by name with Client
// ownership. It never removes registrations that are absent from the call, and
// an empty call sends nothing. A success reply is not evidence of the effect
// when the error is OutcomeUnknownError. Input is validated and copied before any RPC.
func (s *Service) Register(ctx context.Context, definitions ...Definition) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(definitions) == 0 {
		return nil
	}
	sources, err := wireDefinitions(definitions)
	if err != nil {
		return err
	}
	response, err := s.client.RegisterEventSources(ctx, &contracts.RegisterEventSourcesRequest{EventStore: string(s.store), Sources: sources})
	if err == nil {
		err = wire.CheckEnvelope(response)
	}
	return administration.MutationError("register event sources", err)
}

// Observe is unsupported in this increment: no stream is opened.
func (s *Service) Observe(context.Context, func([]Definition)) error {
	return fmt.Errorf("%w: observing event sources", faults.ErrUnsupported)
}

func wireDefinitions(definitions []Definition) ([]*contracts.EventSourceDefinition, error) {
	result := make([]*contracts.EventSourceDefinition, 0, len(definitions))
	seen := make(map[Name]bool, len(definitions))
	for _, definition := range definitions {
		if err := validateName(string(definition.Name), "event source"); err != nil {
			return nil, err
		}
		if seen[definition.Name] {
			return nil, invalid("duplicate event source name")
		}
		seen[definition.Name] = true
		if !definition.Concurrency.valid() {
			return nil, invalid("unknown concurrency dimensions")
		}
		streams, err := wireStreams(definition.Streams)
		if err != nil {
			return nil, err
		}
		result = append(result, &contracts.EventSourceDefinition{Name: string(definition.Name), Description: definition.Description, Owner: contracts.EventSourceOwner_Client, Concurrency: contracts.ConcurrencyDimensions(definition.Concurrency), Streams: streams})
	}
	return result, nil
}

func wireStreams(streams []Stream) ([]*contracts.EventStreamDefinition, error) {
	result := make([]*contracts.EventStreamDefinition, 0, len(streams))
	seen := make(map[string]bool, len(streams))
	for _, stream := range streams {
		if err := validateName(stream.Name, "event stream"); err != nil {
			return nil, err
		}
		if seen[stream.Name] {
			return nil, invalid("duplicate event stream name")
		}
		seen[stream.Name] = true
		if !stream.Concurrency.valid() {
			return nil, invalid("unknown concurrency dimensions")
		}
		result = append(result, &contracts.EventStreamDefinition{Name: stream.Name, Description: stream.Description, Concurrency: contracts.ConcurrencyDimensions(stream.Concurrency)})
	}
	return result, nil
}

func validateName(name, kind string) error {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return invalid(kind + " name required")
	}
	if trimmed != name {
		return invalid("padded " + kind + " name")
	}
	return nil
}

func (d ConcurrencyDimensions) valid() bool { return d&^allDimensions == 0 }

func invalid(message string) error {
	return fmt.Errorf("%w: %s", faults.ErrInvalidConfiguration, message)
}
