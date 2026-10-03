// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package webhooks

import (
	"testing"

	"github.com/cratis/chronicle.go/events"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestFalseFlagsAreExplicitOnCSharpWire(t *testing.T) {
	catalog, err := events.NewCatalog()
	if err != nil {
		t.Fatal(err)
	}
	for _, enabled := range []bool{false, true} {
		d, err := Define(catalog, "notify", "https://example.test", WithActive(enabled), WithReplayable(enabled))
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := proto.Marshal(d.registrationDefinition())
		if err != nil {
			t.Fatal(err)
		}
		fields := map[protowire.Number][]uint64{}
		for len(encoded) > 0 {
			number, kind, n := protowire.ConsumeTag(encoded)
			if n < 0 {
				t.Fatal("invalid tag")
			}
			encoded = encoded[n:]
			if kind == protowire.VarintType {
				value, size := protowire.ConsumeVarint(encoded)
				if size < 0 {
					t.Fatal("invalid scalar")
				}
				fields[number] = append(fields[number], value)
			}
			size := protowire.ConsumeFieldValue(number, kind, encoded)
			if size < 0 {
				t.Fatal("invalid field")
			}
			encoded = encoded[size:]
		}
		var want uint64
		if enabled {
			want = 1
		}
		for _, number := range []protowire.Number{5, 6} {
			if len(fields[number]) != 1 || fields[number][0] != want {
				t.Fatalf("field %d = %v want one explicit %d", number, fields[number], want)
			}
		}
		if len(d.KernelDefinition().ProtoReflect().GetUnknown()) != 0 {
			t.Fatal("wire overlay escaped into mutable snapshot")
		}
	}
}
