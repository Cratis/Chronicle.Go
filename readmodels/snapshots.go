// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	contracts "github.com/cratis/chronicle.go/contracts/readmodelexplorer"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
)

// Snapshot owns a correlation-grouped projection state and its contributions.
// The kernel groups correlation globally: A/B/A produces groups A then B, not
// three contiguous groups. Neither existence nor last-handled progress is present
// on this wire; an empty object after removal is indistinguishable from a present
// empty model. Snapshots are not optimistic-concurrency or decision-read evidence.
type Snapshot[T any] struct {
	// Instance is the projected state, not a presence-bearing Instance[T].
	Instance T
	// Events contains raw, server-released contributions with their full context.
	// Event IDs, original content, revisions and alternate generations are absent
	// from this RPC and are not fabricated. Unknown event types remain raw.
	Events []events.Appended
	// Occurred is the first contributing event's timestamp, not commit time.
	Occurred time.Time
	// CorrelationID belongs to this group, not the response envelope.
	CorrelationID metadata.CorrelationID
}

// GetSnapshots reads correlation-grouped projection history for a nonblank key.
// Reducers fail with ErrUnsupported before any RPC: kernel 19.29.4 returns an
// uninformative empty history for every non-projection. Replay fidelity admission
// also refuses unknown/defaulted/relationship producers. Success returns a non-nil
// slice. Any error or cancellation discards all results. No sessions are created.
func (s *Service) GetSnapshots(ctx context.Context, model Identifier, key Key) ([]Snapshot[json.RawMessage], error) {
	d, ok := s.catalog.LookupIdentifier(model)
	if !ok {
		return nil, notRegistered()
	}
	return s.getSnapshots(ctx, d, key)
}

func (s *Service) getSnapshots(ctx context.Context, d Descriptor, key Key) (result []Snapshot[json.RawMessage], err error) {
	defer func() {
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			result, err = nil, readFailure(err)
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(string(key)) == "" {
		return nil, invalid("nonblank read-model key required")
	}
	kind, _ := d.Observer()
	if kind != Projection || key == "*" {
		return nil, fmt.Errorf("%w: projection snapshot with a specific key required", faults.ErrUnsupported)
	}
	if err = s.validateProjectionReplay(ctx, d); err != nil {
		return nil, err
	}
	response, err := s.explorer.AllSnapshotsForReadModel(ctx, &contracts.AllSnapshotsForReadModelRequest{
		EventStore: string(s.store), Namespace: string(s.namespace), ReadModel: string(d.Identifier()),
		ReadModelKey: string(key), EventSequenceId: string(d.EventSequence()), Grouping: "Correlation",
	})
	if err != nil {
		return nil, wire.RPCError(err)
	}
	if err = wire.CheckEnvelope(response); err != nil {
		return nil, err
	}
	result = make([]Snapshot[json.RawMessage], 0, len(response.Data))
	for _, snapshot := range response.Data {
		if snapshot == nil {
			return nil, faults.ErrProtocol
		}
		occurred, err := snapshotTime(snapshot.Occurred)
		if err != nil {
			return nil, err
		}
		instance, err := releasedDocument(ctx, d, json.RawMessage(snapshot.Instance))
		if err != nil {
			return nil, err
		}
		contributions := make([]events.Appended, len(snapshot.Events))
		for i, contribution := range snapshot.Events {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			contributions[i], err = s.snapshotEvent(d, contribution)
			if err != nil {
				return nil, err
			}
			if descriptor, known := s.snapshotEvents[contributions[i].Context.EventType]; known {
				// The kernel released with this event generation's schema before
				// projecting. Validate its plaintext shape, never decrypt it again
				// with either the event schema or the read-model schema.
				if err := descriptor.Unmarshal(contributions[i].Content, reflect.New(descriptor.GoType()).Interface()); err != nil {
					return nil, &ReleaseError{Cause: err}
				}
			}
		}
		result = append(result, Snapshot[json.RawMessage]{Instance: instance, Events: contributions, Occurred: occurred, CorrelationID: wire.Correlation(snapshot.CorrelationId)})
	}
	return result, nil
}

// GetSnapshots decodes each state through the frozen model plan; contributions
// remain raw and can be decoded using events.Decode and the event catalog.
// No partial snapshot or metadata is returned on late codec failure/cancellation.
func (r *Reader[T]) GetSnapshots(ctx context.Context, key Key) ([]Snapshot[T], error) {
	d, err := r.descriptor()
	if err != nil {
		return nil, err
	}
	raw, err := r.service.getSnapshots(ctx, d, key)
	if err != nil {
		return nil, err
	}
	result := make([]Snapshot[T], len(raw))
	for i, snapshot := range raw {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		value, err := d.Unmarshal(snapshot.Instance)
		if err != nil {
			return nil, readFailure(err)
		}
		result[i] = Snapshot[T]{Instance: *value.(*T), Events: snapshot.Events, Occurred: snapshot.Occurred, CorrelationID: snapshot.CorrelationID}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
