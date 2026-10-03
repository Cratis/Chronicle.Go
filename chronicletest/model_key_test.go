// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"encoding/json"
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/readmodels"
)

func TestScenarioKeyRequiresNormalizedDeclaredProperty(t *testing.T) {
	model, err := readmodels.Define[projectedPlannedModel]()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, document string
		key            readmodels.Key
		err            error
	}{
		{"declared key wins", `{"Id":"person","_id":"sink","ID":"external"}`, "person", nil},
		{"null key wins", `{"Id":null,"_id":"sink","ID":"external"}`, "", chronicle.ErrProtocol},
		{"empty key wins", `{"Id":"","_id":"sink","ID":"external"}`, "", chronicle.ErrProtocol},
		{"missing key", `{"ID":"external"}`, "", ErrFidelityUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, err := modelKey(json.RawMessage(tc.document), model.Descriptor())
			if key != tc.key || !errors.Is(err, tc.err) {
				t.Fatalf("key = %q, %v; want %q, %v", key, err, tc.key, tc.err)
			}
		})
	}
}

type externalOnlyKeyModel struct {
	ExternalID string `json:"ID"`
}

func TestScenarioKeyDoesNotInferIndependentIDWithoutDeclaredKey(t *testing.T) {
	model, err := readmodels.Define[externalOnlyKeyModel]()
	if err != nil {
		t.Fatal(err)
	}
	key, err := modelKey(json.RawMessage(`{"ID":"external"}`), model.Descriptor())
	if key != "" || !errors.Is(err, ErrFidelityUnavailable) {
		t.Fatalf("independent ID became key %q, %v", key, err)
	}
	key, err = modelKey(json.RawMessage(`{"_id":"sink","ID":"external"}`), model.Descriptor())
	if key != "sink" || err != nil {
		t.Fatalf("genuine sink key = %q, %v", key, err)
	}
}
