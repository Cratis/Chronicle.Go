// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"

	chronicle "github.com/cratis/chronicle.go"
	contracts "github.com/cratis/chronicle.go/contracts/readmodels"
	"github.com/cratis/chronicle.go/readmodels"
	"google.golang.org/grpc"
)

type seedKey struct {
	model readmodels.Identifier
	key   readmodels.Key
}
type seededModels struct {
	service *readmodels.Service
	catalog *readmodels.Catalog
	values  map[seedKey]json.RawMessage
	closed  bool
}

func newSeededModels(config Config, catalog *readmodels.Catalog) (*seededModels, error) {
	s := &seededModels{catalog: catalog, values: map[seedKey]json.RawMessage{}}
	var err error
	s.service, err = readmodels.New(config.Store, config.Namespace, catalog, s, readmodels.WithPassiveReader(func(ctx context.Context, model readmodels.Descriptor, key readmodels.Key) (readmodels.Instance[json.RawMessage], error) {
		if err := ctx.Err(); err != nil {
			return readmodels.Instance[json.RawMessage]{}, err
		}
		if s.closed {
			return readmodels.Instance[json.RawMessage]{}, chronicle.ErrClosed
		}
		value, ok := s.values[seedKey{model.Identifier(), key}]
		return readmodels.Instance[json.RawMessage]{Exists: ok, Value: append(json.RawMessage(nil), value...)}, nil
	}))
	return s, err
}
func (s *seededModels) seed(key readmodels.Key, value any) error {
	if strings.TrimSpace(string(key)) == "" {
		return chronicle.ErrInvalidConfiguration
	}
	model, ok := s.catalog.LookupType(reflect.TypeOf(value))
	if !ok {
		return chronicle.ErrNotRegistered
	}
	data, err := model.Marshal(value)
	if err != nil {
		return err
	}
	s.values[seedKey{model.Identifier(), key}] = data
	return nil
}
func (s *seededModels) Invoke(ctx context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return chronicle.ErrClosed
	}
	if method != contracts.ReadModels_GetInstanceByKey_FullMethodName {
		return chronicle.ErrUnsupported
	}
	request, ok := args.(*contracts.GetInstanceByKeyRequest)
	if !ok {
		return chronicle.ErrProtocol
	}
	response, ok := reply.(*contracts.GetInstanceByKeyResponse)
	if !ok {
		return chronicle.ErrProtocol
	}
	data, ok := s.values[seedKey{readmodels.Identifier(request.ReadModelIdentifier), readmodels.Key(request.ReadModelKey)}]
	response.ReadModel = "null"
	if ok {
		response.ReadModel = string(data)
	}
	response.LastHandledEventSequenceNumber = ^uint64(0)
	return nil
}
func (*seededModels) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, chronicle.ErrUnsupported
}
