// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// identityCodec is call-local evidence, never a globally registered codec.
// Proto3 alone cannot distinguish explicit false from absent authorization.
type identityCodec struct {
	decoded bool
	failed  bool
}

func (*identityCodec) Name() string { return "proto" }
func (*identityCodec) Marshal(value any) ([]byte, error) {
	message, ok := value.(proto.Message)
	if !ok || nilValue(message) {
		return nil, ErrProtocol
	}
	data, err := proto.Marshal(message)
	if err != nil {
		return nil, ErrProtocol
	}
	return data, nil
}
func (c *identityCodec) Unmarshal(data []byte, value any) error {
	c.decoded = false
	c.failed = true
	message, ok := value.(proto.Message)
	if !ok || nilValue(message) {
		return ErrProtocol
	}
	if !identityWireValid(data, message.ProtoReflect().Descriptor(), true, 0) {
		return ErrProtocol
	}
	if proto.Unmarshal(data, message) != nil {
		return ErrProtocol
	}
	c.decoded = true
	c.failed = false
	return nil
}

// Reject unknown fields, duplicate singular fields, unsupported encodings and
// noncanonical booleans before decoding. Nested metadata never escapes as errors.
func identityWireValid(data []byte, descriptor protoreflect.MessageDescriptor, envelope bool, depth int) bool {
	if depth > 100 {
		return false
	}
	seen := make(map[protowire.Number]bool)
	for len(data) > 0 {
		number, wireType, n := protowire.ConsumeTag(data)
		if n < 0 {
			return false
		}
		data = data[n:]
		field := descriptor.Fields().ByNumber(protoreflect.FieldNumber(number))
		if field == nil || (seen[number] && !field.IsList()) {
			return false
		}
		seen[number] = true
		switch field.Kind() {
		case protoreflect.BoolKind, protoreflect.EnumKind:
			if wireType != protowire.VarintType {
				return false
			}
			v, n := protowire.ConsumeVarint(data)
			if n < 0 || (field.Kind() == protoreflect.BoolKind && v > 1) {
				return false
			}
			if field.Kind() == protoreflect.EnumKind && field.Enum().Values().ByNumber(protoreflect.EnumNumber(v)) == nil {
				return false
			}
			data = data[n:]
		case protoreflect.Fixed64Kind:
			if wireType != protowire.Fixed64Type {
				return false
			}
			_, n := protowire.ConsumeFixed64(data)
			if n < 0 {
				return false
			}
			data = data[n:]
		case protoreflect.StringKind, protoreflect.MessageKind:
			if wireType != protowire.BytesType {
				return false
			}
			v, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return false
			}
			if field.Kind() == protoreflect.StringKind {
				if !utf8.Valid(v) {
					return false
				}
			} else if !identityWireValid(v, field.Message(), false, depth+1) {
				return false
			}
			data = data[n:]
		default:
			return false
		}
	}
	return !envelope || seen[2]
}
