// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
)

type plainCaseModel struct {
	ID     string `json:"id"`
	Status int32
}
type excludedEnumModel struct {
	ID     string         `json:"id"`
	Status projectionEnum `chronicle:"no-auto"`
}

func TestEnumAutoMapCaseInsensitiveEnumIntoIntegerRefused(t *testing.T) {
	b := projections.NewBuilder("enum-into-integer", mustModel[plainCaseModel](t))
	projections.From(b, mustEvent[enumCaseEvent](t, events.WithCodecs(projectionEnumCodecs(t, false))), nil)
	_, err := b.Build()
	enumBoundaryFailure(t, err, "Status")
}

func TestEnumAutoMapHonorsExplicitWritesAndExclusions(t *testing.T) {
	c := projectionEnumCodecs(t, false)
	event := mustEvent[enumIntegerCaseEvent](t)
	for _, join := range []bool{false, true} {
		b := projections.NewBuilder("enum-override", mustModel[enumCaseModel](t, readmodels.WithCodecs(c)))
		define := func(f *projections.FromBuilder[enumCaseModel, enumIntegerCaseEvent]) {
			projections.Value(f, projections.Path[enumCaseModel, projectionEnum]("Status"), projectionEnum(1))
		}
		if join {
			projections.Join(b, event, projections.Path[enumCaseModel, string]("ID"), define)
		} else {
			projections.From(b, event, define)
		}
		if _, err := b.Build(); err != nil {
			t.Fatal(err)
		}
	}
	b := projections.NewBuilder("enum-excluded", mustModel[excludedEnumModel](t, readmodels.WithCodecs(c)))
	projections.From(b, event, nil)
	if _, err := b.Build(); err != nil {
		t.Fatal(err)
	}
}
