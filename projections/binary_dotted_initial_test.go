// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	"github.com/cratis/chronicle.go/projections"
)

type binaryDottedInitialLeaf struct {
	Payload []byte `json:"payload.bytes"`
}
type binaryDottedInitialOwner struct {
	Data struct{ Payload []byte } `json:"data.bytes"`
}

func TestBinaryDottedInitialValuesRefuse(t *testing.T) {
	t.Run("leaf", func(t *testing.T) {
		b := projections.NewBuilder("binary-dotted-leaf", mustModel[binaryDottedInitialLeaf](t), projections.NoAutoMap(), projections.WithInitialValues(binaryDottedInitialLeaf{Payload: []byte{1}}))
		projections.From(b, mustEvent[binaryMappedEvent](t), nil)
		_, err := b.Build()
		binaryMappingFailure(t, err)
	})
	t.Run("owner", func(t *testing.T) {
		value := binaryDottedInitialOwner{}
		value.Data.Payload = []byte{1}
		b := projections.NewBuilder("binary-dotted-owner", mustModel[binaryDottedInitialOwner](t), projections.NoAutoMap(), projections.WithInitialValues(value))
		projections.From(b, mustEvent[binaryMappedEvent](t), nil)
		_, err := b.Build()
		binaryMappingFailure(t, err)
	})
}
