// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package eventsequences

import (
	"testing"

	"github.com/cratis/chronicle.go/events"
)

func TestEffectiveRouteNarrowsOnlyDefaultSourceTypeForNamedSources(t *testing.T) {
	cases := []struct {
		name   string
		route  Route
		source events.SourceName
		want   events.SourceType
	}{
		{"no source keeps default", Route{}, "", events.DefaultSourceType},
		{"empty type narrows to name", Route{}, "Billing", "Billing"},
		{"default type narrows to name", Route{SourceType: events.DefaultSourceType}, "Billing", "Billing"},
		{"matching explicit type stays", Route{SourceType: "Billing"}, "Billing", "Billing"},
		{"conflicting explicit type is preserved", Route{SourceType: "Other"}, "Billing", "Other"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := effectiveRoute(c.route, c.source).SourceType; got != c.want {
				t.Fatalf("SourceType = %q, want %q", got, c.want)
			}
		})
	}
}
