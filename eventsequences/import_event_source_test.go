// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"errors"
	"slices"
	"testing"

	"github.com/cratis/chronicle.go/internal/faults"
)

func TestImportPreservesEventSourceRejectionReasonsOverGRPC(t *testing.T) {
	cases := map[string]struct {
		route Route
		want  string
	}{
		"unknown source":  {want: "UnknownEventSource: The event source 'Billing' is not registered."},
		"mismatched type": {Route{SourceType: "Other"}, "EventSourceTypeDoesNotMatchEventSource: The event source type 'Other' does not match the event source 'Billing'."},
		"unknown stream":  {Route{StreamType: "Missing"}, "UnknownEventStreamForEventSource: The event stream 'Missing' does not belong to the event source 'Billing'."},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			sequence, server := grpcImportFixture(t, true)
			server.rejectionReason = c.want
			entry := Entry{Source: "A", Event: imported{}, EventSource: "Billing", Route: c.route}
			result, err := sequence.Import(t.Context(), ImportEntries([]Entry{entry}), WithImportID(server.id))
			if err != nil {
				t.Fatal(err)
			}
			assertImportSourceRejection(t, result, c.want)
			if event := server.chunks[0].Events[0]; event.EventSource != "Billing" || event.EventSourceType != string(normalizedRoute(c.route).SourceType) {
				t.Fatal(event)
			}
		})
	}
}

func TestImportRejectsPaddedEventSourceWithoutStagingOrCompleting(t *testing.T) {
	sequence, server := grpcImportFixture(t, true)
	entry := Entry{Source: "A", Event: imported{}, EventSource: "Billing "}
	_, err := sequence.Import(t.Context(), ImportEntries([]Entry{entry}), WithImportID(server.id))
	if !errors.Is(err, faults.ErrInvalidConfiguration) {
		t.Fatalf("error = %v", err)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if server.begun == nil || len(server.chunks) != 0 || server.complete != nil {
		t.Fatalf("began = %t, staged chunks = %d, completed = %t", server.begun != nil, len(server.chunks), server.complete != nil)
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
