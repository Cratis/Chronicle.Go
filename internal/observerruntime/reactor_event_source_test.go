// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package observerruntime

import "testing"

func TestReactorContextPreservesEventSourceAttribution(t *testing.T) {
	for _, name := range []string{"", "Billing"} {
		t.Run(name, func(t *testing.T) {
			context := appended("PageInput", "A", 0).Context
			context.EventSource, context.EventSourceType = name, "Explicit"
			result, err := eventContext(context, "store", "Default", "sequence")
			if err != nil {
				t.Fatal(err)
			}
			if string(result.EventSource) != name || result.SourceType != "Explicit" || result.SourceID != "A" {
				t.Fatal(result)
			}
		})
	}
}
