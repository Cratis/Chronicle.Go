// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// protobuf-net initializes FullSet to true. Proto3 omits false, so additive
// requests must explicitly encode field 4 = 0. Keep this codec call-local.
type projectionRegistrationCodec struct{}

func (projectionRegistrationCodec) Name() string { return "proto" }
func (projectionRegistrationCodec) Marshal(value any) ([]byte, error) {
	message, ok := value.(proto.Message)
	if !ok {
		return nil, ErrProtocol
	}
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, err
	}
	if request, ok := value.(*contracts.RegisterRequest); ok && !request.FullSet {
		data = protowire.AppendTag(data, 4, protowire.VarintType)
		data = protowire.AppendVarint(data, 0)
	}
	return data, nil
}
func (projectionRegistrationCodec) Unmarshal(data []byte, value any) error {
	message, ok := value.(proto.Message)
	if !ok {
		return ErrProtocol
	}
	return proto.Unmarshal(data, message)
}
