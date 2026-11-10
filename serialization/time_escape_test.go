// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/cratis/chronicle.go/serialization"
)

type escapedCause struct {
	Occurred time.Time `json:"occurred"`
}

type escapedTimeEvent struct {
	At        time.Time      `json:"at"`
	Maybe     *time.Time     `json:"maybe"`
	Causation []escapedCause `json:"causation"`
}

// The kernel escapes the + of a time offset as +; that is valid JSON.
func TestUnmarshalDecodesEscapedTimeOffsets(t *testing.T) {
	plan, err := serialization.Compile(reflect.TypeFor[escapedTimeEvent]())
	if err != nil {
		t.Fatal(err)
	}
	var got escapedTimeEvent
	if err := plan.Unmarshal([]byte(`{"at":"2026-11-09T18:56:26.103+01:00","maybe":"2026-11-09T18:56:26Z","causation":[{"occurred":"2026-11-09T18:56:26.099\u002B01:00"}]}`), &got); err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 11, 9, 17, 56, 26, 103000000, time.UTC); !got.At.Equal(want) || got.Maybe == nil || len(got.Causation) != 1 || !got.Causation[0].Occurred.Equal(time.Date(2026, 11, 9, 17, 56, 26, 99000000, time.UTC)) {
		t.Fatalf("got %+v", got)
	}
}
