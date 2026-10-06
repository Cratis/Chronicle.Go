// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/projections"
)

type binaryJoinEvent struct {
	ID      string
	Payload []byte
}
type binaryVariantEntered struct{ ID string }

func TestBinaryJoinAutoMapRefusesBeforeRegistration(t *testing.T) {
	builder := projections.NewBuilder("binary-leaf-join", mustModel[binaryMappedModel](t))
	projections.Join(builder, mustEvent[binaryJoinEvent](t), projections.Path[binaryMappedModel, string]("Id"), nil)
	if _, err := builder.Build(); !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("unwitnessed binary Join copy admitted: %v", err)
	}
}

func TestBinaryVariantAutoMapRefusesBeforeRegistration(t *testing.T) {
	for _, enteringCopy := range []bool{false, true} {
		t.Run(map[bool]string{false: "generated join", true: "entering From"}[enteringCopy], func(t *testing.T) {
			event := mustEvent[binaryJoinEvent](t)
			entering := projections.EntersOn(mustEvent[binaryVariantEntered](t))
			if enteringCopy {
				entering = projections.EntersOn(event)
			}
			builder := projections.NewBuilder("binary-leaf-variant", mustModel[binaryMappedModel](t), projections.VariantOf[WorkItem](), projections.VariantKey(projections.Path[binaryMappedModel, string]("Id")), entering)
			projections.From(builder, event, nil)
			if _, err := builder.Build(); !errors.Is(err, chronicle.ErrUnsupported) {
				t.Fatalf("unwitnessed binary VariantOf copy admitted: %v", err)
			}
		})
	}
}
