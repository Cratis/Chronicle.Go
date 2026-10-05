// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package metadata_test

import (
	"testing"

	"github.com/cratis/chronicle.go/metadata"
)

func TestCanonicalNames(t *testing.T) {
	if metadata.NotSetStore != "[NotSet]" || metadata.SystemStore != "System" || metadata.NotSetNamespace != "[NotSet]" || metadata.DefaultNamespace != "Default" {
		t.Fatal("canonical names differ from C#")
	}
}
