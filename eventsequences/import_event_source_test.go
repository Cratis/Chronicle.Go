// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"slices"
	"testing"
)

func TestImportPreservesEventSourceRejectionReasonsOverGRPC(t *testing.T) {
	for _, reason := range []string{"UnknownEventSource", "EventSourceTypeDoesNotMatchEventSource", "UnknownEventStreamForEventSource"} {
		t.Run(reason, func(t *testing.T) {
			sequence, server := grpcImportFixture(t, true)
			server.rejectionReason = reason
			result, err := sequence.Import(t.Context(), ImportEntries([]Entry{{Source: "A", Event: imported{}, EventSource: "Billing"}}), WithImportID(server.id))
			if err != nil {
				t.Fatal(err)
			}
			assertImportSourceRejection(t, result, reason)
			if event := server.chunks[0].Events[0]; event.EventSource != "Billing" || event.EventSourceType != "Default" {
				t.Fatal(event)
			}
		})
	}
}

func assertImportSourceRejection(t *testing.T, result ImportResult, reason string) {
	t.Helper()
	if result.State != ImportFailed || result.IsSuccess() {
		t.Fatalf("result = %+v", result)
	}
	if result.RejectedCount != 1 || len(result.Rejections) != 1 {
		t.Fatal(result.Rejections)
	}
	rejection := result.Rejections[0]
	if rejection.Index != 0 || rejection.EventType != "imported+2" {
		t.Fatal(rejection)
	}
	if !slices.Equal(rejection.Reasons, []string{reason}) {
		t.Fatal(rejection.Reasons)
	}
}
