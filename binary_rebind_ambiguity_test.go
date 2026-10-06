// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

type binaryKeyAliasFirst struct {
	Alias string `json:"data.payload"`
	Data  struct{ Payload []byte }
}
type binaryKeyAliasLast struct {
	Data  struct{ Payload []byte }
	Alias string `json:"data.payload"`
}

func TestBinaryProjectionRebindAmbiguityRefusesBeforeIO(t *testing.T) {
	t.Run("alias first", testBinaryEventNameAdmission[binaryKeyAliasFirst])
	t.Run("alias last", testBinaryEventNameAdmission[binaryKeyAliasLast])
}

func testBinaryEventNameAdmission[E any](t *testing.T) {
	t.Helper()
	// No descriptor can be constructed to rebind: dotted names are refused
	// even when they do not yet overlap another path under Preserve names.
	if _, err := events.Define[E](); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("event name admitted before registration: %v", err)
	}
	r := chronicle.NewRegistry()
	if _, err := chronicle.RegisterEvent[E](r); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("registry admitted binary property names: %v", err)
	}
}

func TestBinaryModelNamingAmbiguityRefusesBeforeIO(t *testing.T) {
	t.Run("alias first", testBinaryModelNameAdmission[binaryNamedAliasFirst])
	t.Run("alias last", testBinaryModelNameAdmission[binaryNamedAliasLast])
}
func testBinaryModelNameAdmission[M any](t *testing.T) {
	t.Helper()
	for _, options := range [][]readmodels.ModelOption{nil, {readmodels.WithIndexes("Data.Payload")}} {
		if _, err := readmodels.Define[M](options...); !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatalf("model name admitted before registration: %v", err)
		}
	}
}

type binaryAutoMapAliasFirst struct {
	Alias string `json:"Data.Payload"`
	Data  struct{ Payload []byte }
}
type binaryAutoMapAliasLast struct {
	Data  struct{ Payload []byte }
	Alias string `json:"Data.Payload"`
}

func TestBinaryAutoMapAliasCandidatesRefuseBeforeIO(t *testing.T) {
	t.Run("alias first", testBinaryEventNameAdmission[binaryAutoMapAliasFirst])
	t.Run("alias last", testBinaryEventNameAdmission[binaryAutoMapAliasLast])
}

func TestBinaryNamesRecheckEventAndModelDescriptors(t *testing.T) {
	event, err := events.Define[capabilityBinaryPayload]()
	if err != nil {
		t.Fatal(err)
	}
	model, err := readmodels.Define[capabilityBinaryPayload]()
	if err != nil {
		t.Fatal(err)
	}
	for _, policy := range []serialization.NamingPolicy{serialization.CamelCase, serialization.LegacyGoCamelCase} {
		if _, err := event.Descriptor().WithNamingPolicy(policy); err != nil {
			t.Fatal(err)
		}
		if _, err := model.Descriptor().WithNamingPolicy(policy); err != nil {
			t.Fatal(err)
		}
	}
}
