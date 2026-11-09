// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package wire_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/internal/faults"
	"github.com/cratis/chronicle.go/internal/wire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestPresentZeroEncodesTheFieldOnce(t *testing.T) {
	message := &sequences.AppendOnceResponse{}
	if err := wire.PresentZero(message, "SequenceNumber"); err != nil {
		t.Fatal(err)
	}
	encoded, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	// Field 2, varint wire type, value 0.
	if !bytes.Equal(encoded, []byte{0x10, 0x00}) {
		t.Fatalf("encoded = % x", encoded)
	}
	var decoded sequences.AppendOnceResponse
	if err := proto.Unmarshal(encoded, &decoded); err != nil || decoded.SequenceNumber != 0 || len(decoded.ProtoReflect().GetUnknown()) != 0 {
		t.Fatalf("decoded %v, %v", &decoded, err)
	}
}

func TestPresentZeroRefusesOtherFields(t *testing.T) {
	for name, message := range map[string]*sequences.AppendOnceResponse{"CorrelationId": {}, "Missing": {}, "SequenceNumber": {SequenceNumber: 3}} {
		if err := wire.PresentZero(message, protoreflect.Name(name)); !errors.Is(err, faults.ErrProtocol) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}
