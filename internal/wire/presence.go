// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package wire

import (
	"fmt"

	"github.com/cratis/chronicle.go/internal/faults"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// PresentZero marks an implicit-presence uint64 field as present with the value
// zero. protobuf-net reads such a field as a nullable value, so leaving zero out
// would arrive as null. Go's generated code omits a zero proto3 scalar, so the
// field is carried as an explicitly encoded unknown field instead; it is encoded
// once, as field number name with value 0. The field's own value must be zero.
func PresentZero(message proto.Message, name protoreflect.Name) error {
	m := message.ProtoReflect()
	field := m.Descriptor().Fields().ByName(name)
	if field == nil || field.Kind() != protoreflect.Uint64Kind || field.Cardinality() == protoreflect.Repeated {
		return fmt.Errorf("%w: %s is not a singular uint64 field", faults.ErrProtocol, name)
	}
	if m.Has(field) {
		return fmt.Errorf("%w: %s already has a nonzero value", faults.ErrProtocol, name)
	}
	encoded := protowire.AppendVarint(protowire.AppendTag(nil, field.Number(), protowire.VarintType), 0)
	m.SetUnknown(append(m.GetUnknown(), encoded...))
	return nil
}
