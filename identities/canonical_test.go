// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package identities_test

import (
	"testing"

	"github.com/cratis/chronicle.go/identities"
)

func TestCanonicalIdentities(t *testing.T) {
	if identities.System() != (identities.Identity{Subject: "5d032c92-9d5e-41eb-947a-ee5314ed0032", Name: "[System]", UserName: "[System]"}) {
		t.Fatal("system identity")
	}
	if identities.Unknown() != (identities.Identity{Subject: "3321cf62-db16-425e-8173-99fcfefe11dd", Name: "[Unknown]", UserName: "[Unknown]"}) {
		t.Fatal("unknown identity")
	}
	actor := identities.System()
	actor.Name = "modified"
	actor.OnBehalfOf = &actor
	if identities.System().Name != "[System]" || identities.System().OnBehalfOf != nil {
		t.Fatal("identity shared mutable state")
	}
}
