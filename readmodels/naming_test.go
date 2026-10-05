// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package readmodels

import "testing"

func TestContainerNamesMatchCSharpDefaultNamingPolicy(t *testing.T) {
	// DefaultNamingPolicy.GetReadModelName invokes Humanizer.Pluralize with known-singular=true.
	for input, want := range map[string]string{"Person": "People", "Customer": "Customers", "Status": "Statuses", "OrderSummary": "OrderSummaries", "Child": "Children", "SalesPerson": "SalesPeople", "Human": "Humans", "Series": "Series", "Fish": "Fish", "URL": "URLS", "Mouse": "Mice", "Bus": "Buses", "Metadata": "Metadata", "Analysis": "Analyses", "Index": "Indices", "CustomerAddress": "CustomerAddresses", "S": "Ss"} {
		if got := pluralize(input); got != want {
			t.Errorf("%s: %s != %s", input, got, want)
		}
	}
}
