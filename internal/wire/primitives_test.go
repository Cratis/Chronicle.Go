// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package wire_test

import (
	"testing"
	"time"

	"github.com/cratis/chronicle.go/internal/wire"
	"github.com/cratis/chronicle.go/metadata"
)

func TestDotNETGuidVector(t *testing.T) {
	id, err := metadata.ParseCorrelationID("00112233-4455-6677-8899-aabbccddeeff")
	if err != nil {
		t.Fatal(err)
	}
	value := wire.Guid(id)
	// Guid.ToByteArray(): 33 22 11 00 55 44 77 66 88 99 aa bb cc dd ee ff.
	if value.Lo != 0x6677445500112233 || value.Hi != 0xffeeddccbbaa9988 {
		t.Fatalf("lo=%x hi=%x", value.Lo, value.Hi)
	}
	if wire.Correlation(value) != id {
		t.Fatal("Guid did not round trip")
	}
}

func TestDateTimeOffsetPreservesOffsetAndTruncates(t *testing.T) {
	value := time.Date(2026, 10, 2, 15, 1, 2, 123456789, time.FixedZone("+02", 2*60*60))
	if got := wire.DateTimeOffset(value); got != "2026-10-02T15:01:02.1234567+02:00" {
		t.Fatal(got)
	}
}
