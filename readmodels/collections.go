// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
)

// Collection owns present instances and the number of events reported by the
// operation. Deleted instances are omitted; present zero-valued models remain.
// ProcessedEventsCount is not a sequence position, sink watermark or freshness
// guarantee. Materialized retrieval reports zero even when instances exist.
type Collection[T any] struct {
	// Instances is non-nil on success, including an empty collection.
	Instances []Instance[T]
	// ProcessedEventsCount is the actual reported or locally folded event count.
	ProcessedEventsCount events.Count
}

// GetAll reads a collection. A nil count selects materialized state unless the
// model is passive. Every explicit reducer count folds locally, even Unlimited.
// Finite counts select the first N matching events globally, not per instance.
// Zero returns empty without I/O or reducer activation; finite counts above
// MaxInt32 fail rather than clamp. Local folds fetch complete matching history:
// the count bounds computation, not transport volume. Failures return no partial
// instances or progress. This operation allocates no hydration session.
func (s *Service) GetAll(ctx context.Context, model Identifier, count *events.Count) (Collection[json.RawMessage], error) {
	d, ok := s.catalog.LookupIdentifier(model)
	if !ok {
		return Collection[json.RawMessage]{}, notRegistered()
	}
	return s.getAll(ctx, d, count)
}

func (s *Service) getAll(ctx context.Context, d Descriptor, count *events.Count) (result Collection[json.RawMessage], err error) {
	defer func() {
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			result = Collection[json.RawMessage]{}
			err = readFailure(err)
		}
	}()
	if err = ctx.Err(); err != nil {
		return result, err
	}
	limit := events.UnlimitedCount
	if count != nil {
		limit = *count
	}
	if limit != events.UnlimitedCount && limit > math.MaxInt32 {
		return result, invalid("event count exceeds kernel limit")
	}
	if limit == 0 {
		return Collection[json.RawMessage]{Instances: []Instance[json.RawMessage]{}}, nil
	}
	kind, _ := d.Observer()
	if kind == Reducer && (count != nil || d.Sink().Type == NoSink) {
		if s.collectionReader == nil {
			return result, fmt.Errorf("%w: local reducer collection reader unavailable", faults.ErrUnsupported)
		}
		result, err = s.collectionReader(ctx, d, limit)
		if err != nil {
			return result, err
		}
		if limit != events.UnlimitedCount && result.ProcessedEventsCount > limit {
			return result, faults.ErrProtocol
		}
		if result.Instances == nil {
			result.Instances = []Instance[json.RawMessage]{}
		}
		for i := range result.Instances {
			instance := &result.Instances[i]
			if !instance.Exists || (instance.LastHandled != nil && *instance.LastHandled >= events.Unavailable-2) {
				return result, faults.ErrProtocol
			}
			instance.Value, err = releasedDocument(ctx, d, instance.Value)
			if err != nil {
				return result, err
			}
		}
		return result, nil
	}
	if kind == Projection && (limit != events.UnlimitedCount || d.Sink().Type == NoSink) {
		if err = s.validateProjectionReplay(ctx, d); err != nil {
			return result, err
		}
	}
	return s.readCollection(ctx, d, limit)
}

func (s *Service) readCollection(ctx context.Context, d Descriptor, count events.Count) (Collection[json.RawMessage], error) {
	response, err := s.client.GetAllInstances(ctx, &contracts.GetAllInstancesRequest{
		EventStore: string(s.store), Namespace: string(s.namespace), ReadModelIdentifier: string(d.Identifier()),
		EventSequenceId: string(d.EventSequence()), EventCount: uint64(count),
	})
	if err != nil {
		return Collection[json.RawMessage]{}, wire.RPCError(err)
	}
	if response == nil || (count != events.UnlimitedCount && response.ProcessedEventsCount > uint64(count)) {
		return Collection[json.RawMessage]{}, faults.ErrProtocol
	}
	result := Collection[json.RawMessage]{Instances: make([]Instance[json.RawMessage], 0, len(response.Instances)), ProcessedEventsCount: events.Count(response.ProcessedEventsCount)}
	for _, document := range response.Instances {
		data, err := releasedDocument(ctx, d, json.RawMessage(document))
		if err != nil {
			return Collection[json.RawMessage]{}, err
		}
		position, err := collectionPosition(data)
		if err != nil {
			return Collection[json.RawMessage]{}, err
		}
		result.Instances = append(result.Instances, Instance[json.RawMessage]{Value: data, Exists: true, LastHandled: position})
	}
	if err := ctx.Err(); err != nil {
		return Collection[json.RawMessage]{}, err
	}
	return result, nil
}

func collectionPosition(data json.RawMessage) (*events.SequenceNumber, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil {
		return nil, faults.ErrProtocol
	}
	raw, ok := fields["__lastHandledEventSequenceNumber"]
	if !ok {
		return nil, nil
	}
	var position uint64
	if string(raw) == "null" || json.Unmarshal(raw, &position) != nil {
		return nil, faults.ErrProtocol
	}
	if position == uint64(events.Unavailable) {
		return nil, nil
	}
	if position >= uint64(events.Unavailable-2) {
		return nil, faults.ErrProtocol
	}
	value := events.SequenceNumber(position)
	return &value, nil
}

// GetAll decodes the entire collection with the registered serialization plan.
// No partial collection or progress is returned after decoding or cancellation
// fails. Codecs run outside transport leases and may close their client.
func (r *Reader[T]) GetAll(ctx context.Context, count *events.Count) (Collection[T], error) {
	d, err := r.descriptor()
	if err != nil {
		return Collection[T]{}, err
	}
	raw, err := r.service.getAll(ctx, d, count)
	if err != nil {
		return Collection[T]{}, err
	}
	result := Collection[T]{Instances: make([]Instance[T], len(raw.Instances)), ProcessedEventsCount: raw.ProcessedEventsCount}
	for i, instance := range raw.Instances {
		if err := ctx.Err(); err != nil {
			return Collection[T]{}, err
		}
		result.Instances[i], err = decode[T](instance, d)
		if err != nil {
			return Collection[T]{}, readFailure(err)
		}
	}
	if err := ctx.Err(); err != nil {
		return Collection[T]{}, err
	}
	return result, nil
}

// Only admitted routes reach this boundary: materialized stores release using
// persisted lineage; local reducers fold released events; projection replay,
// history, sessions and immediate projections release with each value's original
// subject (kernel 19.32.2 or later, Chronicle#4561).
// Shape validation is not proof of decryption: strings accept ciphertext too.
// A second Compliance.Release can corrupt legitimate plaintext. Validate the
// final representation without another RPC, then normalize the root identity.
func releasedDocument(ctx context.Context, d Descriptor, data json.RawMessage) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data = bytes.TrimSpace(data)
	if !validDocument(data) {
		return nil, faults.ErrProtocol
	}
	if err := validateReleasedDocument(d, data); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return normalizeID(data, d)
}

type readError struct{ cause error }

func (*readError) Error() string   { return "chronicle: read-model read failed" }
func (e *readError) Unwrap() error { return e.cause }
func readFailure(err error) error  { return &readError{cause: err} }
