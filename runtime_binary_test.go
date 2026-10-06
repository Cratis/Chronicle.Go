// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle

import (
	"errors"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
)

type runtimeBinaryPayload struct{ Payload []byte }

func TestRuntimeOrdinaryProjectionProfilesRefuseBinary(t *testing.T) {
	model, err := readmodels.Define[runtimeBinaryPayload]()
	if err != nil {
		t.Fatal(err)
	}
	if err := ordinaryModel(model.Descriptor()); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("runtime binary model admitted: %v", err)
	}
	event, err := events.Define[runtimeBinaryPayload]()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		t.Fatal(err)
	}
	if err := ordinaryEvent(catalog, event.Descriptor().Ref()); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("runtime binary input admitted: %v", err)
	}
}
