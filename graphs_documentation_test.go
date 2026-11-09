// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicle_test

import "testing"

func TestGraphProjectionSnippetsMatchCompiledExample(t *testing.T) {
	checkProjectionSnippets(t, "examples/graphs/main.go", "Documentation/projections/graphs.md", []string{"graph-events", "graph-model"})
}
