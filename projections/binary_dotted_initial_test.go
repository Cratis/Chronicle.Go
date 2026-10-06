// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/readmodels"
)

type binaryDottedInitialLeaf struct {
	Payload []byte `json:"payload.bytes"`
}
type binaryDottedInitialOwner struct {
	Data struct{ Payload []byte } `json:"data.bytes"`
}

func TestBinaryDottedInitialValuesRefuse(t *testing.T) {
	// Neither descriptor can reach WithInitialValues: naming admission now
	// refuses even unambiguous dotted binary leaves and owning objects.
	t.Run("leaf", func(t *testing.T) {
		if _, err := readmodels.Define[binaryDottedInitialLeaf](); !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatalf("dotted binary leaf admitted: %v", err)
		}
	})
	t.Run("owner", func(t *testing.T) {
		if _, err := readmodels.Define[binaryDottedInitialOwner](); !errors.Is(err, chronicle.ErrUnsupported) {
			t.Fatalf("dotted binary owner admitted: %v", err)
		}
	})
}
