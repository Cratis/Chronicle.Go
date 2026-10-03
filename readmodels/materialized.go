// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"google.golang.org/protobuf/proto"
)

// UnlimitedInstances selects all instances after Skip (up to the kernel int32 limit).
const UnlimitedInstances int32 = -1

// Window selects a skip/take range, not an event-count replay. A nil *Window uses
// Skip=0, Take=50. Negative Skip becomes zero; Take<=0 is empty except -1 (unlimited).
// Non-aligned ranges fetch a covering page from the start, capped at MaxInt32.
type Window struct {
	// Skip is the number of instances to skip; negative values mean zero.
	Skip int32
	// Take is the maximum count; -1 means unlimited, other nonpositive values empty.
	Take int32
}
type paging struct{ page, size, skip, take int32 }

func calculatePaging(window *Window) paging {
	w := Window{Take: 50}
	if window != nil {
		w = *window
	}
	w.Skip = max(0, w.Skip)
	if w.Take == UnlimitedInstances {
		return paging{size: math.MaxInt32, skip: w.Skip, take: math.MaxInt32}
	}
	if w.Take <= 0 {
		return paging{}
	}
	if w.Skip%w.Take == 0 {
		return paging{page: w.Skip / w.Take, size: w.Take, take: w.Take}
	}
	return paging{size: int32(min(int64(math.MaxInt32), int64(w.Skip)+int64(w.Take))), skip: w.Skip, take: w.Take}
}

// MaterializedService reads sink windows. It does not execute passive projections
// or reducers, and leaving a window does not prove that a document was deleted.
type MaterializedService struct{ service *Service }

// Materialized returns the raw materialized-window API for this namespace.
func (s *Service) Materialized() *MaterializedService { return &MaterializedService{s} }

// MaterializedReader is the typed materialized-window API. Construct through Reader.Materialized.
type MaterializedReader[T any] struct{ reader *Reader[T] }

// Materialized selects sink pages rather than projection changes or passive reads.
func (r *Reader[T]) Materialized() *MaterializedReader[T] { return &MaterializedReader[T]{r} }

func materializedDescriptor(s *Service, model Identifier) (Descriptor, error) {
	d, ok := s.catalog.LookupIdentifier(model)
	if !ok {
		return Descriptor{}, notRegistered()
	}
	if d.Sink().Type == NoSink {
		return Descriptor{}, fmt.Errorf("%w: passive models have no materialized window", faults.ErrUnsupported)
	}
	if err := materializedReleaseProfile(d); err != nil {
		return Descriptor{}, err
	}
	return d, nil
}

// materializedReleaseProfile admits only the protected wire profile witnessed
// with persisted MongoDB lineage. It is not a decryption test. Providers and type
// declarations are judged by their frozen schema, never re-evaluated here.
func materializedReleaseProfile(d Descriptor) error {
	if len(d.definition.protected) == 0 {
		return nil
	}
	unsupported := fmt.Errorf("%w: materialized protection profile unavailable", faults.ErrUnsupported)
	if d.Sink().Type != MongoDB {
		return unsupported
	}
	var schema struct {
		Properties map[string]map[string]json.RawMessage `json:"properties"`
	}
	if json.Unmarshal([]byte(d.Schema()), &schema) != nil {
		return unsupported
	}
	for name := range d.definition.protected {
		property := schema.Properties[name]
		if string(property["type"]) != `"string"` {
			return unsupported
		}
		// References, compositions, containers, nullable and custom scalar
		// representations need their own kernel witness, not string validation.
		for keyword := range property {
			switch keyword {
			case "type", "title", "description", "compliance", "security":
			default:
				return unsupported
			}
		}
		var pii, encrypted []struct {
			MetadataType string `json:"metadataType"`
		}
		if raw, ok := property["compliance"]; ok && json.Unmarshal(raw, &pii) != nil {
			return unsupported
		}
		if raw, ok := property["security"]; ok && json.Unmarshal(raw, &encrypted) != nil {
			return unsupported
		}
		personal := len(pii) == 1 && pii[0].MetadataType == "PII" && len(encrypted) == 0
		confidential := len(encrypted) == 1 && encrypted[0].MetadataType == "EncryptedNamespace" && len(pii) == 0
		if !personal && !confidential {
			return unsupported
		}
	}
	return nil
}

// GetInstances reads the exact requested window. Nil means 0/50. Results own
// their JSON. The kernel releases stored values; the SDK validates the final
// representation without decrypting again. Protected windows are limited to
// MongoDB root string PII and namespace-encrypted properties; other classified
// profiles fail with ErrUnsupported before RPC.
func (m *MaterializedService) GetInstances(ctx context.Context, model Identifier, window *Window) ([]json.RawMessage, error) {
	d, err := materializedDescriptor(m.service, model)
	if err != nil {
		return nil, err
	}
	p := calculatePaging(window)
	response, err := m.service.materialized.GetInstances(ctx, &contracts.GetInstancesRequest{EventStore: string(m.service.store), Namespace: string(m.service.namespace), ReadModel: string(model), Page: p.page, PageSize: p.size})
	if err != nil {
		return nil, wire.RPCError(err)
	}
	if response == nil {
		return nil, faults.ErrProtocol
	}
	return decodeWindow(d, p, response.Instances, releasedDecoder(ctx, d, rawValue))
}

// GetInstances is the typed equivalent of MaterializedService.GetInstances.
func (m *MaterializedReader[T]) GetInstances(ctx context.Context, window *Window) ([]T, error) {
	d, err := m.reader.descriptor()
	if err != nil {
		return nil, err
	}
	values, err := m.reader.service.Materialized().GetInstances(ctx, d.Identifier(), window)
	if err != nil {
		return nil, err
	}
	result := make([]T, len(values))
	decode := typedValue[T](d)
	for i, value := range values {
		result[i], err = decode(value)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

// ObserveInstances returns after the first snapshot has been decoded. Unlike
// Watch this RPC has no Subscribed marker; the first Recv returns that snapshot.
// Each subsequent value replaces the entire window. Server-side coalescing can
// omit intermediate states. No durable changes, cursor or gap-free resume exist.
// Cancellation, overload, terminal errors and Close follow Subscription's contract.
func (m *MaterializedService) ObserveInstances(ctx context.Context, model Identifier, window *Window, options ...WatchOption) (*Subscription[[]json.RawMessage], error) {
	d, err := materializedDescriptor(m.service, model)
	if err != nil {
		return nil, err
	}
	return observeWindow(ctx, m.service, d, window, rawValue, options)
}

// ObserveInstances is the typed equivalent of MaterializedService.ObserveInstances.
func (m *MaterializedReader[T]) ObserveInstances(ctx context.Context, window *Window, options ...WatchOption) (*Subscription[[]T], error) {
	d, err := m.reader.descriptor()
	if err != nil {
		return nil, err
	}
	if _, err = materializedDescriptor(m.reader.service, d.Identifier()); err != nil {
		return nil, err
	}
	return observeWindow(ctx, m.reader.service, d, window, typedValue[T](d), options)
}

func observeWindow[T any](ctx context.Context, s *Service, d Descriptor, window *Window, decode func(json.RawMessage) (T, error), options []WatchOption) (*Subscription[[]T], error) {
	c, err := watchOptions(options)
	if err != nil {
		return nil, err
	}
	p := calculatePaging(window)
	return startSubscription(ctx, c, func(ctx context.Context) (func() ([]T, int, bool, error), error) {
		stream, err := s.materialized.ObserveInstances(ctx, &contracts.ObserveInstancesRequest{EventStore: string(s.store), Namespace: string(s.namespace), ReadModel: string(d.Identifier()), Page: p.page, PageSize: p.size})
		if err != nil {
			return nil, err
		}
		var first []T
		firstSize := 0
		ready := false
		receive := func() ([]T, int, error) {
			response, err := stream.Recv()
			if err != nil {
				return nil, 0, err
			}
			if response == nil {
				return nil, 0, faults.ErrProtocol
			}
			size := proto.Size(response)
			if size > c.bytes {
				return nil, 0, ErrOverloaded
			}
			values, err := decodeWindow(d, p, response.Instances, releasedDecoder(ctx, d, decode))
			return values, size, err
		}
		return func() ([]T, int, bool, error) {
			if !ready {
				var err error
				first, firstSize, err = receive()
				if err != nil {
					return nil, 0, false, err
				}
				ready = true
				return nil, 0, true, nil
			}
			if first != nil {
				value := first
				first = nil
				return value, firstSize, false, nil
			}
			values, size, err := receive()
			return values, size, false, err
		}, nil
	})
}

func releasedDecoder[T any](ctx context.Context, d Descriptor, decode func(json.RawMessage) (T, error)) func(json.RawMessage) (T, error) {
	return func(data json.RawMessage) (T, error) {
		var zero T
		// A window is a complete document, not a changeset delta. Missing
		// classified roots cannot be accepted as successful release/erasure.
		if len(d.definition.protected) != 0 {
			var fields map[string]json.RawMessage
			if json.Unmarshal(data, &fields) != nil {
				return zero, &ReleaseError{Cause: faults.ErrProtocol}
			}
			for root := range d.definition.protected {
				if value, ok := fields[root]; !ok || string(value) == "null" {
					return zero, &ReleaseError{Cause: faults.ErrProtocol}
				}
			}
		}
		released, err := releasedDocument(ctx, d, data)
		if err != nil {
			return zero, err
		}
		return decode(released)
	}
}

func decodeWindow[T any](d Descriptor, p paging, values []string, decode func(json.RawMessage) (T, error)) ([]T, error) {
	start := min(int64(p.skip), int64(len(values)))
	end := min(start+int64(p.take), int64(len(values)))
	result := make([]T, 0, end-start)
	for _, value := range values[start:end] {
		if !validDocument([]byte(value)) {
			return nil, protocol("invalid window document")
		}
		data, err := normalizeID([]byte(value), d)
		if err != nil {
			return nil, err
		}
		decoded, err := decode(data)
		if err != nil {
			return nil, err
		}
		result = append(result, decoded)
	}
	return result, nil
}
