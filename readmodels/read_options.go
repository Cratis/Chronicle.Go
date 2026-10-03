// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/serialization"
)

// ReducerCollectionReader folds released matching history locally. It returns
// owned plaintext, actual progress and present instances only. The callback must
// honor cancellation and support concurrent calls. It must not publish changes
// or acknowledge an observer. No counted transport lease spans this callback.
type ReducerCollectionReader func(context.Context, Descriptor, events.Count) (Collection[json.RawMessage], error)

// WithReducerCollectionReader installs the store's local collection fold. Unlike
// the legacy PassiveReader seam, results are already plaintext. Repeats fail.
func WithReducerCollectionReader(reader ReducerCollectionReader) ServiceOption {
	return func(s *Service) error {
		if reader == nil || s.collectionReader != nil {
			return invalid("one non-nil reducer collection reader required")
		}
		s.collectionReader = reader
		return nil
	}
}

// WithReleasedPassiveReader installs a passive reader that folds already released
// events into plaintext. It preserves the legacy PassiveReader signature without
// decrypting its result again. Do not combine with WithPassiveReader.
func WithReleasedPassiveReader(reader PassiveReader) ServiceOption {
	return func(s *Service) error {
		if err := WithPassiveReader(reader)(s); err != nil {
			return err
		}
		s.passiveReleased = true
		return nil
	}
}

// WithSnapshotEventCatalog installs frozen event schemas for protected snapshot
// contribution validation. Known protected generations validate their released
// representation before delivery; unknown types stay raw. The store supplies
// this automatically. Nil/repeated catalogs fail; no application codec runs here.
func WithSnapshotEventCatalog(catalog *events.Catalog) ServiceOption {
	return func(s *Service) error {
		if catalog == nil || s.snapshotEvents != nil {
			return invalid("one non-nil snapshot event catalog required")
		}
		protected := make(map[events.TypeRef]events.Descriptor)
		for _, descriptor := range catalog.Descriptors() {
			roots, err := serialization.ProtectionRoots(descriptor.Schema())
			if err != nil {
				return err
			}
			if len(roots) != 0 {
				protected[descriptor.Ref()] = descriptor
			}
		}
		s.snapshotEvents = protected
		return nil
	}
}

// ProjectionReplayValidator admits a model only when its producer's defaults and
// key/relationship shape are faithfully supported by kernel collection/history
// replay. It returns known=false, err=nil when no local producer is known. New
// collection/history APIs refuse unknown producers; legacy ReplayProjection
// preserves its caller-owned admission contract in that case.
type ProjectionReplayValidator func(context.Context, Descriptor) (known bool, err error)

// WithProjectionReplayValidator installs immutable producer admission for new
// collection/history reads. Repeated options fail. The callback must support
// concurrent calls and cancellation; it must not evaluate projections locally.
func WithProjectionReplayValidator(validator ProjectionReplayValidator) ServiceOption {
	return func(s *Service) error {
		if validator == nil || s.replayValidator != nil {
			return invalid("one non-nil projection replay validator required")
		}
		s.replayValidator = validator
		return nil
	}
}

func (s *Service) validateProjectionReplay(ctx context.Context, d Descriptor) error {
	if s.replayValidator == nil {
		return fmt.Errorf("%w: projection replay fidelity is unknown", faults.ErrUnsupported)
	}
	known, err := s.replayValidator(ctx, d)
	if err != nil {
		return err
	}
	if !known {
		return fmt.Errorf("%w: projection replay requires a local producer definition", faults.ErrUnsupported)
	}
	return nil
}
