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
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
	"google.golang.org/grpc"
)

// Service is a concurrency-safe namespace-bound reader. It borrows its transport
// and catalog. Use EventStore.ReadModels; New is for adapters and test transports.
type Service struct {
	store      metadata.StoreName
	namespace  metadata.Namespace
	catalog    *Catalog
	client     contracts.ReadModelsClient
	compliance compliance.ComplianceClient
}

// New constructs a service without I/O. The caller owns the channel and any
// registration/readiness barriers; the store facade supplies those automatically.
func New(store metadata.StoreName, namespace metadata.Namespace, catalog *Catalog, conn grpc.ClientConnInterface) (*Service, error) {
	if strings.TrimSpace(string(store)) == "" || strings.TrimSpace(string(namespace)) == "" || catalog == nil || conn == nil || (reflect.ValueOf(conn).Kind() == reflect.Pointer && reflect.ValueOf(conn).IsNil()) {
		return nil, invalid("store, namespace, catalog and transport required")
	}
	return &Service{store: store, namespace: namespace, catalog: catalog, client: contracts.NewReadModelsClient(conn), compliance: compliance.NewComplianceClient(conn)}, nil
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
// normalized to the model's serialized property name. Kernel-owned reads release protected
// values on the server; they are not decrypted a second time on the client.
func (s *Service) Get(ctx context.Context, model Identifier, key Key) (Instance[json.RawMessage], error) {
	d, ok := s.catalog.LookupIdentifier(model)
	if !ok {
		return Instance[json.RawMessage]{}, notRegistered()
	}
	return s.get(ctx, d, key, "")
}
func (s *Service) get(ctx context.Context, d Descriptor, key Key, session string) (Instance[json.RawMessage], error) {
	if err := ctx.Err(); err != nil {
		return Instance[json.RawMessage]{}, err
	}
	if strings.TrimSpace(string(key)) == "" {
		return Instance[json.RawMessage]{}, invalid("nonblank read-model key required")
	}
	if key == "*" {
		return Instance[json.RawMessage]{}, fmt.Errorf("%w: unspecified-key replay is not a one-shot instance read", faults.ErrUnsupported)
	}
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
	data, err = normalizeID(data, d)
	if err != nil {
		return Instance[json.RawMessage]{}, err
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
	return decode[T](raw)
}
func decode[T any](raw Instance[json.RawMessage]) (Instance[T], error) {
	result := Instance[T]{Exists: raw.Exists, LastHandled: raw.LastHandled}
	if !raw.Exists {
		return result, nil
	}
	if err := json.Unmarshal(raw.Value, &result.Value); err != nil {
		return Instance[T]{}, fmt.Errorf("%w: model document does not match declared type", faults.ErrProtocol)
	}
	normalizeCollections(reflect.ValueOf(&result.Value).Elem())
	return result, nil
}
func normalizeCollections(value reflect.Value) {
	switch value.Kind() {
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			f := value.Type().Field(i)
			if f.IsExported() && strings.Split(f.Tag.Get("json"), ",")[0] != "-" {
				normalizeCollections(value.Field(i))
			}
		}
	case reflect.Pointer:
		if !value.IsNil() {
			normalizeCollections(value.Elem())
		}
	case reflect.Slice:
		if value.IsNil() {
			value.Set(reflect.MakeSlice(value.Type(), 0, 0))
		}
		for i := 0; i < value.Len(); i++ {
			normalizeCollections(value.Index(i))
		}
	case reflect.Array:
		for i := 0; i < value.Len(); i++ {
			normalizeCollections(value.Index(i))
		}
	case reflect.Map:
		iterator := value.MapRange()
		for iterator.Next() {
			entry := reflect.New(value.Type().Elem()).Elem()
			entry.Set(iterator.Value())
			normalizeCollections(entry)
			value.SetMapIndex(iterator.Key(), entry)
		}
	}
}

// normalizeID reconciles the MongoDB key alias without changing nested fields or
// overriding a document's declared ID. Leave unchanged documents byte-for-byte.
func normalizeID(data []byte, d Descriptor) ([]byte, error) {
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
	for _, alias := range []string{"_id", "id", "Id", "ID", "iD"} {
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

func validDocument(data []byte) bool { return len(data) > 0 && data[0] == '{' && json.Valid(data) }
func notRegistered() error           { return fmt.Errorf("%w: read-model declaration", faults.ErrNotRegistered) }
