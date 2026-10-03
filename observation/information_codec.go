// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observation

import (
	contracts "github.com/cratis/chronicle.go/contracts/observation"
	"github.com/cratis/chronicle.go/internal/faults"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// informationCodec is used only by GetObservers and GetObserverInformation. The
// authoritative protobuf-net contract defaults field 10 to true and emits false
// explicitly. Its generated proto3 schema cannot represent that presence/default.
// Decode normally, then restore the default ONLY when field 10 is absent. Do not
// register a global codec or change how any other RPC interprets proto3 defaults.
type informationCodec struct{}

func (informationCodec) Name() string { return "proto" }
func (informationCodec) Marshal(value any) ([]byte, error) {
	message, ok := value.(proto.Message)
	if !ok {
		return nil, faults.ErrProtocol
	}
	return proto.Marshal(message)
}
func (informationCodec) Unmarshal(data []byte, value any) error {
	message, ok := value.(proto.Message)
	if !ok {
		return faults.ErrProtocol
	}
	if err := proto.Unmarshal(data, message); err != nil {
		return err
	}
	switch response := value.(type) {
	case *contracts.ObserverInformation:
		return restoreReplayableDefault(data, response)
	case *contracts.IEnumerable_ObserverInformation:
		index := 0
		return walkFields(data, func(number protowire.Number, kind protowire.Type, field []byte) error {
			if number != 1 || kind != protowire.BytesType {
				return nil
			}
			item, n := protowire.ConsumeBytes(field)
			if n < 0 || index >= len(response.Items) {
				return faults.ErrProtocol
			}
			err := restoreReplayableDefault(item, response.Items[index])
			index++
			return err
		})
	default:
		return faults.ErrProtocol
	}
}

func restoreReplayableDefault(data []byte, information *contracts.ObserverInformation) error {
	present := false
	if err := walkFields(data, func(number protowire.Number, kind protowire.Type, _ []byte) error {
		if number == 10 {
			if kind != protowire.VarintType {
				return faults.ErrProtocol
			}
			present = true
		}
		return nil
	}); err != nil {
		return err
	}
	if !present {
		information.IsReplayable = true
	}
	return nil
}

func walkFields(data []byte, visit func(protowire.Number, protowire.Type, []byte) error) error {
	for len(data) > 0 {
		number, kind, n := protowire.ConsumeTag(data)
		if n < 0 {
			return faults.ErrProtocol
		}
		data = data[n:]
		n = protowire.ConsumeFieldValue(number, kind, data)
		if n < 0 {
			return faults.ErrProtocol
		}
		if err := visit(number, kind, data[:n]); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}
