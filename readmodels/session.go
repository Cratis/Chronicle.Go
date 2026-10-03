// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"fmt"
	"strings"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/google/uuid"
)

// Session owns one projection hydration session, bound to one reader/key. Get and
// Close serialize with cancellable admission. It must not be copied. Close before
// closing the client; the kernel session is not a protected decision token or a
// snapshot guarantee, and is not automatically replayed after connection loss.
type Session[T any] struct {
	reader                     *Reader[T]
	descriptor                 Descriptor
	key                        Key
	id                         string
	gate                       chan struct{}
	closed, attempted, cleaned bool
}

// NewSession creates a lazy hydration session with an internally generated UUID.
// The first Get hydrates; each Get uses that same session. Reducers and models
// without a declared projection fail with ErrUnsupported. Call Close even after
// failed Get, because the kernel may already have allocated hydration state.
func (r *Reader[T]) NewSession(key Key) (*Session[T], error) {
	d, err := r.descriptor()
	if err != nil {
		return nil, err
	}
	kind, producer := d.Observer()
	if kind != Projection || strings.TrimSpace(producer) == "" {
		return nil, fmt.Errorf("%w: sessions require a declared projection", faults.ErrUnsupported)
	}
	if strings.TrimSpace(string(key)) == "" || key == "*" {
		return nil, invalid("session requires a concrete nonblank key")
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	return &Session[T]{reader: r, descriptor: d, key: key, id: id.String(), gate: make(chan struct{}, 1)}, nil
}
func (s *Session[T]) acquire(ctx context.Context) error {
	if s == nil || s.gate == nil {
		return invalid("uninitialized session")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *Session[T]) unlock() { <-s.gate }

// Get reads in this session. After cleanup begins it returns ErrClosed.
func (s *Session[T]) Get(ctx context.Context) (Instance[T], error) {
	if err := s.acquire(ctx); err != nil {
		return Instance[T]{}, err
	}
	defer s.unlock()
	if s.closed {
		return Instance[T]{}, faults.ErrClosed
	}
	s.attempted = true
	raw, err := s.reader.service.get(ctx, s.descriptor, s.key, s.id)
	if err != nil {
		return Instance[T]{}, err
	}
	return decode[T](raw, s.descriptor)
}

// Close dehydrates using the exact model, key, namespace, sequence and session ID.
// It accepts a cleanup context rather than hiding I/O behind an unbounded Close.
// Success is idempotent; failed cleanup is reported and may be retried with a fresh
// context. No further Get is admitted once cleanup starts.
func (s *Session[T]) Close(ctx context.Context) error {
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.unlock()
	s.closed = true
	if s.cleaned || !s.attempted {
		s.cleaned = true
		return nil
	}
	service := s.reader.service
	response, err := service.client.DehydrateSession(ctx, &contracts.DehydrateSessionRequest{EventStore: string(service.store), Namespace: string(service.namespace), ReadModelIdentifier: string(s.descriptor.Identifier()), EventSequenceId: string(s.descriptor.EventSequence()), ReadModelKey: string(s.key), SessionId: s.id})
	if err != nil {
		return wire.RPCError(err)
	}
	if response == nil {
		return faults.ErrProtocol
	}
	s.cleaned = true
	return nil
}
