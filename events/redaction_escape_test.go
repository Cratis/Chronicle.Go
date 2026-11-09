// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package events_test

import (
	"testing"
	"time"

	"github.com/cratis/chronicle.go/events"
)

// The kernel escapes the + of a time offset as +. That is valid JSON, and
// the marker must decode on every supported Go toolchain.
func TestRedactionDecodesEscapedTimeOffsets(t *testing.T) {
	appended := events.Appended{Content: []byte(`{"reason":"r","originalEventType":"batch-opened","occurred":"2026-11-09T18:56:26.103\u002B00:00","correlationId":"9aa3b15a-6dca-40e5-b1f2-47a27b3bbcce","causation":[{"type":"ASP.NET Request","occurred":"2026-11-09T18:56:26.099\u002B01:00"}],"causedBy":[]}`)}
	appended.Context.EventType.ID = events.RedactedTypeID
	marker, err := appended.Redaction()
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 11, 9, 18, 56, 26, 103000000, time.UTC); !marker.Occurred.Equal(want) {
		t.Fatalf("occurred = %v", marker.Occurred)
	}
	if got := marker.Causation[0].Occurred; !got.Equal(time.Date(2026, 11, 9, 17, 56, 26, 99000000, time.UTC)) {
		t.Fatalf("causation occurred = %v", got)
	}
}
