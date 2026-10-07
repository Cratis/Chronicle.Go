// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/cratis/chronicle.go/contracts/compliance"
	"github.com/cratis/chronicle.go/contracts/readmodelexplorer"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/decision"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/jsonstructure"
	"github.com/cratis/chronicle.go/internal/kernelcapability"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
)

// Service is a concurrency-safe namespace-bound reader. It borrows its transport
// and catalog. Use EventStore.ReadModels; New is for adapters and test transports.
type Service struct {
	store            metadata.StoreName
	namespace        metadata.Namespace
	catalog          *Catalog
	client           contracts.ReadModelsClient
	materialized     contracts.MaterializedReadModelsClient
	explorer         readmodelexplorer.ReadModelExplorerClient
	compliance       compliance.ComplianceClient
	passive          PassiveReader
	passiveReleased  bool
	collectionReader ReducerCollectionReader
	replayValidator  ProjectionReplayValidator
	snapshotEvents   map[events.TypeRef]events.Descriptor
	reductionChanges *ReductionChanges
	decisions        decision.Provider
	conn             grpc.ClientConnInterface
}

// New constructs a service without I/O. The caller owns the channel and any
// registration/readiness barriers; the store facade supplies those automatically.
func New(store metadata.StoreName, namespace metadata.Namespace, catalog *Catalog, conn grpc.ClientConnInterface, options ...ServiceOption) (*Service, error) {
	if strings.TrimSpace(string(store)) == "" || strings.TrimSpace(string(namespace)) == "" || catalog == nil || conn == nil || (reflect.ValueOf(conn).Kind() == reflect.Pointer && reflect.ValueOf(conn).IsNil()) {
		return nil, invalid("store, namespace, catalog and transport required")
	}
	service := &Service{store: store, namespace: namespace, catalog: catalog, client: contracts.NewReadModelsClient(conn), materialized: contracts.NewMaterializedReadModelsClient(conn), explorer: readmodelexplorer.NewReadModelExplorerClient(conn), compliance: compliance.NewComplianceClient(conn), conn: conn}
	service.decisions, _ = conn.(decision.Provider)
	for _, option := range options {
		if option == nil {
			return nil, invalid("nil service option")
		}
		if err := option(service); err != nil {
			return nil, err
		}
	}
	return service, nil
}

// Catalog returns the immutable set of registered models.
func (s *Service) Catalog() *Catalog { return s.catalog }

// Instance distinguishes absence from a valid zero-valued model. Value is usable
// only when Exists is true. LastHandled is nil only for the unavailable sentinel;
// an absent (removed) model can still have a last-handled position.
type Instance[T any] struct {
	// Value is usable only when Exists is true.
	Value T
	// Exists distinguishes absent state from a legitimate zero-valued model.
	Exists bool
	// LastHandled is the reported position, or nil when unavailable.
	LastHandled *events.SequenceNumber
}

// Get reads raw JSON by registered model identity and key. JSON null means absent;
// an empty/malformed/non-object response fails with ErrProtocol. Raw JSON is owned
// by the caller and retains kernel metadata except the root ID alias, which is
// normalized to the model's serialized property name. Materialized, projection
// immediate and session reads are server-released and validated without another
// decrypt. Classified projection immediate/session reads need Chronicle 19.32.2
// or later (Chronicle#4561) and fail with ErrUnsupported before RPC unless the
// connection reports such a kernel.
func (s *Service) Get(ctx context.Context, model Identifier, key Key) (Instance[json.RawMessage], error) {
	d, ok := s.catalog.LookupIdentifier(model)
	if !ok {
		return Instance[json.RawMessage]{}, notRegistered()
	}
	return s.get(ctx, d, key, "")
}
func (s *Service) get(ctx context.Context, d Descriptor, key Key, session string) (result Instance[json.RawMessage], err error) {
	// Needs added by admission travel with every RPC of this read and are
	// re-checked against the generation that dispatches it.
	ctx = kernelcapability.Track(ctx)
	defer func() {
		if err == nil {
			err = ctx.Err()
		}
		if err != nil {
			result, err = Instance[json.RawMessage]{}, readFailure(err)
		}
	}()
	if err := ctx.Err(); err != nil {
		return Instance[json.RawMessage]{}, err
	}
	if strings.TrimSpace(string(key)) == "" {
		return Instance[json.RawMessage]{}, invalid("nonblank read-model key required")
	}
	if key == "*" {
		return Instance[json.RawMessage]{}, fmt.Errorf("%w: unspecified-key replay is not a one-shot instance read", faults.ErrUnsupported)
	}
	if kind, _ := d.Observer(); kind == Reducer && d.Sink().Type == NoSink {
		if s.passive == nil || session != "" {
			return Instance[json.RawMessage]{}, fmt.Errorf("%w: passive reducer reader unavailable or session requested", faults.ErrUnsupported)
		}
		result, err := s.passive(ctx, d, key)
		if err != nil {
			return Instance[json.RawMessage]{}, err
		}
		if result.Exists {
			if s.passiveReleased {
				result.Value, err = releasedDocument(ctx, d, result.Value)
			} else {
				result.Value, err = s.Release(ctx, d.Identifier(), result.Value)
			}
			if err != nil {
				return Instance[json.RawMessage]{}, err
			}
		}
		return result, nil
	}
	if d.Sink().Type == NoSink || session != "" {
		if err := s.projectionReleaseAdmission(ctx, d); err != nil {
			return Instance[json.RawMessage]{}, err
		}
	}
	result, err = s.getInstance(ctx, d, key, session)
	if err != nil || !result.Exists {
		return result, err
	}
	// Pinned kernel materialized/keyed handlers own release. Never send their
	// plaintext through Compliance.Release again (it can resemble ciphertext).
	data, err := releasedDocument(ctx, d, result.Value)
	if err != nil {
		return Instance[json.RawMessage]{}, err
	}
	result.Value = data
	return result, nil
}

// getInstance reads and checks raw protocol shape only. Its caller must admit
// the release route and validate the final representation before publication.
func (s *Service) getInstance(ctx context.Context, d Descriptor, key Key, session string) (Instance[json.RawMessage], error) {
	response, err := s.client.GetInstanceByKey(ctx, &contracts.GetInstanceByKeyRequest{EventStore: string(s.store), Namespace: string(s.namespace), ReadModelIdentifier: string(d.Identifier()), EventSequenceId: string(d.EventSequence()), ReadModelKey: string(key), SessionId: session})
	if err != nil {
		return Instance[json.RawMessage]{}, wire.RPCError(err)
	}
	if response == nil {
		return Instance[json.RawMessage]{}, faults.ErrProtocol
	}
	data := bytes.TrimSpace([]byte(response.ReadModel))
	result := Instance[json.RawMessage]{}
	if response.LastHandledEventSequenceNumber != uint64(events.Unavailable) {
		if response.LastHandledEventSequenceNumber >= uint64(events.Unavailable-2) {
			return result, faults.ErrProtocol
		}
		position := events.SequenceNumber(response.LastHandledEventSequenceNumber)
		result.LastHandled = &position
	}
	if bytes.Equal(data, []byte("null")) {
		return result, nil
	}
	if !validDocument(data) {
		return Instance[json.RawMessage]{}, faults.ErrProtocol
	}
	result.Value, result.Exists = data, true
	return result, nil
}

// Reader is a typed, concurrency-safe reader for one immutable model declaration.
// Its zero value is invalid; construct it with For.
type Reader[T any] struct {
	service *Service
	model   Model[T]
}

// For creates a lazy typed reader. A nil service, empty or foreign declaration is
// rejected by operations with ErrNotRegistered; no network I/O occurs here.
func For[T any](service *Service, model Model[T]) *Reader[T] {
	return &Reader[T]{service: service, model: model}
}
func (r *Reader[T]) descriptor() (Descriptor, error) {
	if r == nil || r.service == nil || r.model.descriptor.GoType() == nil {
		return Descriptor{}, notRegistered()
	}
	d, ok := r.service.catalog.LookupIdentifier(r.model.Identifier())
	if !ok || !sameDeclaration(d, r.model.descriptor) {
		return Descriptor{}, notRegistered()
	}
	return d, nil
}

// Get reads one instance. Missing models remain Exists=false; declared slice
// properties in present models normalize missing/null to empty (pointer-to-slice
// declarations retain nullable presence). LastHandled is never folded into T.
func (r *Reader[T]) Get(ctx context.Context, key Key) (Instance[T], error) {
	d, err := r.descriptor()
	if err != nil {
		return Instance[T]{}, err
	}
	raw, err := r.service.get(ctx, d, key, "")
	if err != nil {
		return Instance[T]{}, err
	}
	return decode[T](raw, d)
}
func decode[T any](raw Instance[json.RawMessage], d Descriptor) (Instance[T], error) {
	result := Instance[T]{Exists: raw.Exists, LastHandled: raw.LastHandled}
	if !raw.Exists {
		return result, nil
	}
	value, err := d.Unmarshal(raw.Value)
	if err != nil {
		return Instance[T]{}, err
	}
	result.Value = *value.(*T)
	return result, nil
}

// normalizeID reconciles the MongoDB key alias without changing nested fields or
// overriding a document's declared ID. Leave unchanged documents byte-for-byte.
func normalizeID(data []byte, d Descriptor) ([]byte, error) {
	// ID alias rewriting must not discard evidence before plan validation.
	if err := jsonstructure.Validate(data); err != nil {
		return nil, err
	}
	name := idProperty(d)
	if name == "" {
		return data, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, faults.ErrProtocol
	}
	if _, exists := fields[name]; exists {
		return data, nil
	}
	declared := make(map[string]bool)
	for _, field := range d.definition.plan.Fields() {
		declared[field.Path] = true
	}
	for _, alias := range []string{"_id", "id", "Id", "ID", "iD"} {
		// An independently declared property is not a sink key alias, even
		// when its spelling differs from the key only by case (Id versus ID).
		if declared[alias] {
			continue
		}
		if value, exists := fields[alias]; exists {
			fields[name] = value
			delete(fields, alias)
			result, err := json.Marshal(fields)
			if err != nil {
				return nil, faults.ErrProtocol
			}
			return result, nil
		}
	}
	return data, nil
}

func notRegistered() error { return fmt.Errorf("%w: read-model declaration", faults.ErrNotRegistered) }
