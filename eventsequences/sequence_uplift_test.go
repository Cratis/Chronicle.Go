// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/cratis/chronicle.go/contracts/sequences"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/faults"
)

func TestSingleAppendRejectsNilOptionBeforeDispatch(t *testing.T) {
	sequence, calls := sequenceFixture(t, nil)
	notifications := 0
	defer sequence.OnAppend(func(eventsequences.AppendNotification) { notifications++ })()
	_, err := sequence.Append(testContext(t), "source", opened{}, nil)
	if !errors.Is(err, faults.ErrInvalidConfiguration) || err.Error() != "chronicle: invalid configuration: nil append option" {
		t.Fatalf("nil option error = %v", err)
	}
	if calls.Load() != 0 || notifications != 0 {
		t.Fatalf("calls=%d notifications=%d", calls.Load(), notifications)
	}
}

func TestSingleAppendMergesTagsInFirstOccurrenceOrder(t *testing.T) {
	sequence, calls := sequenceFixture(t, map[string]rpcHandler{"Append": func(_ context.Context, raw any) (any, error) {
		request := raw.(*sequences.AppendRequest)
		if !reflect.DeepEqual(request.Tags, []string{"first", "shared", "dynamic"}) {
			t.Errorf("tags = %v", request.Tags)
		}
		return &sequences.CommandResult_AppendResponse{IsAuthorized: true, Response: &sequences.AppendResponse{IsSuccess: true}}, nil
	}})
	result, err := sequence.Append(testContext(t), "source", opened{},
		eventsequences.WithTags("shared", "dynamic", "first", "dynamic"),
		eventsequences.WithScope(eventsequences.Scope{Expectation: eventsequences.NoCheck()}))
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != eventsequences.Committed || calls.Load() != 1 {
		t.Fatalf("result=%+v calls=%d", result, calls.Load())
	}
}
