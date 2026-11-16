// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/eventsequences"
	"github.com/cratis/chronicle.go/internal/faults"
)

func TestBatchValidationPreservesErrorPrecedenceBeforeDispatch(t *testing.T) {
	duplicate := eventsequences.WithScopes(eventsequences.LabeledScope{Label: "A"}, eventsequences.LabeledScope{Label: "A"})
	for _, test := range []struct {
		name    string
		entry   eventsequences.Entry
		options []eventsequences.BatchOption
		message string
	}{
		{"batch metadata before scopes", eventsequences.Entry{}, []eventsequences.BatchOption{duplicate, eventsequences.WithBatchNamedTags(events.NamedTag{Name: " "})}, "empty named tag name"},
		{"scope before entry", eventsequences.Entry{Source: "A", Event: struct{}{}}, []eventsequences.BatchOption{duplicate}, "blank or duplicate scope label"},
		{"import fields before registration", eventsequences.Entry{Source: "A", Event: struct{}{}, Generation: 1}, nil, "entry 0 sets a field only imports honor"},
	} {
		t.Run(test.name, func(t *testing.T) {
			sequence, calls := sequenceFixture(t, nil)
			_, err := sequence.AppendBatch(testContext(t), []eventsequences.Entry{test.entry}, test.options...)
			if !errors.Is(err, faults.ErrInvalidConfiguration) || !strings.Contains(err.Error(), test.message) || calls.Load() != 0 {
				t.Fatalf("error = %v, calls = %d", err, calls.Load())
			}
		})
	}
}
