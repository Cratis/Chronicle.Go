// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"errors"
	"fmt"
	"testing"

	chronicle "github.com/cratis/chronicle.go"
	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
)

type binaryJoinEvent struct {
	ID      string
	Payload []byte
}
type binaryVariantEntered struct{ ID string }

func assertBinaryCopyFailure(t *testing.T, err error, artifact, directive string, event events.TypeRef) {
	t.Helper()
	if !errors.Is(err, chronicle.ErrUnsupported) {
		t.Fatalf("unwitnessed binary copy should be unsupported: %v", err)
	}
	var located *projections.DeclarationError
	if !errors.As(err, &located) {
		t.Fatalf("binary copy refusal lacks declaration provenance: %v", err)
	}
	if located.Artifact != artifact || located.GoField != "Payload" || located.Path != "Payload" || located.Directive != directive || located.Offset != -1 || located.EventReference != fmt.Sprintf("%s,%d", event.ID, event.Generation) {
		t.Fatalf("binary copy provenance = %+v", located)
	}
}

func TestBinaryJoinAutoMapRefusesBeforeRegistration(t *testing.T) {
	builder := projections.NewBuilder("binary-leaf-join", mustModel[binaryMappedModel](t))
	event := mustEvent[binaryJoinEvent](t)
	projections.Join(builder, event, projections.Path[binaryMappedModel, string]("Id"), nil)
	_, err := builder.Build()
	assertBinaryCopyFailure(t, err, "binary-leaf-join", "join", event.Ref())
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
			_, err := builder.Build()
			directive := "join"
			if enteringCopy {
				directive = "VariantOf"
			}
			assertBinaryCopyFailure(t, err, "binary-leaf-variant", directive, event.Ref())
		})
	}
}
