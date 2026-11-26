// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences_test

import (
	"context"
	"testing"

	"github.com/cratis/chronicle.go/eventsequences"
)

func TestReadSourcePreservesEventSourceAttribution(t *testing.T) {
	for _, name := range []string{"", "Billing"} {
		t.Run(name, func(t *testing.T) {
			event := readEvent("A", 0)
			event.Context.EventSource, event.Context.EventSourceType = name, "Explicit"
			sequence, _ := sequenceFixture(t, map[string]rpcHandler{"ForEventSourceIdAndEventTypes": func(context.Context, any) (any, error) { return readResponse(event), nil }})
			result, err := sequence.ReadSource(testContext(t), "A", eventsequences.SourceFilter{})
			if err != nil {
				t.Fatal(err)
			}
			if len(result) != 1 {
				t.Fatal(result)
			}
			c := result[0].Context
			if string(c.EventSource) != name {
				t.Fatal(c)
			}
			if c.SourceType != "Explicit" || c.SourceID != "A" {
				t.Fatal(c)
			}
		})
	}
}
