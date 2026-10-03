// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/protobuf/proto"
)

// ChangeType classifies a read-model change, not an event-log delivery.
type ChangeType uint8

const (
	// Added is a new model (or a model entering a materialized window).
	Added ChangeType = iota
	// Modified is a changed model already present.
	Modified
	// Removed is a deleted model, or one leaving a materialized window.
	Removed
)

// Change owns a change's key, optional model, and available causing-event metadata.
// Value is usable only with HasValue. Removed is always explicit in Type; a
// deletion may carry the old model, or no value. Context.EventType is unknown:
// the watch protocol does not identify it or provide full event audit metadata.
type Change[T any] struct {
	// Type distinguishes addition, modification and removal.
	Type ChangeType
	// Key identifies the changed model, including when Value is absent.
	Key Key
	// Value is owned by the consumer and usable only when HasValue is true.
	Value T
	// HasValue is false when a removal carries no document.
	HasValue bool
	// Context carries available metadata; unspecified fields remain unknown.
	Context events.Context
}

// Watch opens a projection change feed and returns only after Subscribed. This
// barrier is NOT a current model snapshot. Changes are ordered as received; an
// interruption is terminal, never resumed. The context owns the full lifetime.
// Watches cannot join hydration sessions: the wire has no session selector.
// Classified models fail with ErrUnsupported before opening a stream: the pinned
// kernel cannot reliably release projection changes, and local reducer changes
// have no delivery-time erasure fence. For unclassified reducers, Watch attaches
// to this client's plaintext fold notifications (local readiness, no server
// Subscribed or sink-write guarantee). Local changes are Modified/Removed;
// reactors infer first-seen Added.
func (s *Service) Watch(ctx context.Context, model Identifier, options ...WatchOption) (*Subscription[Change[json.RawMessage]], error) {
	d, ok := s.catalog.LookupIdentifier(model)
	if !ok {
		return nil, notRegistered()
	}
	return watch(ctx, s, d, rawValue, options)
}

// Watch is the typed equivalent of Service.Watch, including its ready barrier,
// bounded queue, terminal errors, namespace isolation and Close obligation.
func (r *Reader[T]) Watch(ctx context.Context, options ...WatchOption) (*Subscription[Change[T]], error) {
	d, err := r.descriptor()
	if err != nil {
		return nil, err
	}
	return watch(ctx, r.service, d, typedValue[T](d), options)
}

func watch[T any](ctx context.Context, s *Service, d Descriptor, decode func(json.RawMessage) (T, error), options []WatchOption) (*Subscription[Change[T]], error) {
	c, err := watchOptions(options)
	if err != nil {
		return nil, err
	}
	// Watch replies cannot establish reliable protected-value provenance. Do not
	// try another decrypt: legitimate plaintext can resemble ciphertext. This
	// also refuses local folds before attaching either bounded delivery queue.
	if len(d.definition.protected) != 0 {
		return nil, fmt.Errorf("%w: classified watch release ownership unavailable", faults.ErrUnsupported)
	}
	kind, id := d.Observer()
	if kind == Reducer && id != "" {
		return watchReductions(ctx, s, d, c, decode)
	}
	if kind != Projection || id == "" {
		return nil, fmt.Errorf("%w: Watch requires a projection; reducer notifications are local", faults.ErrUnsupported)
	}
	return startSubscription(ctx, c, func(ctx context.Context) (func() (Change[T], int, bool, error), error) {
		stream, err := s.client.Watch(ctx, &contracts.WatchRequest{EventStore: string(s.store), Namespace: string(s.namespace), ReadModelIdentifier: string(d.Identifier()), EventSequenceId: string(d.EventSequence())})
		if err != nil {
			return nil, err
		}
		return func() (Change[T], int, bool, error) {
			var change Change[T]
			message, err := stream.Recv()
			if err != nil {
				return change, 0, false, err
			}
			if message == nil {
				return change, 0, false, protocol("nil changeset")
			}
			if message.Subscribed {
				return change, 0, true, nil
			}
			if proto.Size(message) > c.bytes {
				return change, 0, false, ErrOverloaded
			}
			if metadata.Namespace(message.Namespace) != s.namespace {
				return change, 0, false, protocol("changeset namespace mismatch")
			}
			change.Key = Key(message.ModelKey)
			if change.Key == "" {
				return change, 0, false, protocol("empty changeset key")
			}
			change.Type = Modified // C# treats unknown future values as Modified.
			if message.ChangeType == contracts.ReadModelChangeType_Added {
				change.Type = Added
			}
			if message.Removed || message.ChangeType == contracts.ReadModelChangeType_Removed {
				change.Type = Removed
			}
			change.Context = changeContext(s, d, change.Key)
			change.Context.SequenceNumber = events.SequenceNumber(message.EventSequenceNumber)
			change.Context.CorrelationID = wire.Correlation(message.CorrelationId)
			if message.Occurred != nil && message.Occurred.Value != "" {
				change.Context.Occurred, err = time.Parse(time.RFC3339Nano, message.Occurred.Value)
				if err != nil {
					return Change[T]{}, 0, false, protocol("invalid change occurrence")
				}
			}
			data := bytes.TrimSpace([]byte(message.ReadModel))
			if bytes.Equal(data, []byte("null")) {
				if change.Type != Removed {
					return Change[T]{}, 0, false, protocol("null non-removal")
				}
			} else {
				if !validDocument(data) {
					return Change[T]{}, 0, false, protocol("invalid changeset document")
				}
				data, err = releasedDocument(ctx, d, data)
				if err == nil {
					change.Value, err = decode(data)
				}
				if err != nil {
					return Change[T]{}, 0, false, err
				}
				change.HasValue = true
			}
			return change, proto.Size(message), false, nil
		}, nil
	})
}

func changeContext(s *Service, d Descriptor, key Key) events.Context {
	return events.Context{Store: s.store, Namespace: s.namespace, Sequence: d.EventSequence(), SourceID: events.SourceID(key), SourceType: events.DefaultSourceType, StreamType: events.AllStreamTypes, StreamID: events.DefaultStreamID, SequenceNumber: events.Unavailable}
}
func rawValue(data json.RawMessage) (json.RawMessage, error) { return data, nil }
func typedValue[T any](d Descriptor) func(json.RawMessage) (T, error) {
	return func(data json.RawMessage) (T, error) {
		var zero T
		value, err := d.Unmarshal(data)
		if err != nil {
			return zero, err
		}
		// Typed readers validate that the catalog descriptor belongs to T.
		return *value.(*T), nil
	}
}
func protocol(message string) error { return fmt.Errorf("%w: %s", faults.ErrProtocol, message) }
