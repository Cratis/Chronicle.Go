// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package metadata_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cratis/chronicle.go/identities"
	"github.com/cratis/chronicle.go/metadata"
)

func TestImmutableMetadata(t *testing.T) {
	base := context.Background()
	actor := identities.Identity{Subject: "service", OnBehalfOf: &identities.Identity{Subject: "user"}}
	cause := metadata.Causation{Type: "command", Properties: map[string]string{"name": "RegisterCustomer"}}
	ctx := metadata.WithCausation(metadata.WithIdentity(base, actor), cause)
	actor.OnBehalfOf.Subject, cause.Properties["name"] = "changed", "changed"
	if metadata.Identity(ctx).OnBehalfOf.Subject != "user" || metadata.CausationChain(ctx)[0].Properties["name"] != "RegisterCustomer" {
		t.Fatal("input mutation leaked")
	}
	chain := metadata.CausationChain(ctx)
	chain[0].Properties["name"] = "mutated copy"
	if metadata.CausationChain(ctx)[0].Properties["name"] != "RegisterCustomer" {
		t.Fatal("output mutation leaked")
	}
	child := metadata.WithCausation(ctx, metadata.Causation{Type: "append"})
	if len(metadata.CausationChain(ctx)) != 1 || len(metadata.CausationChain(child)) != 2 || len(metadata.CausationChain(base)) != 0 {
		t.Fatal("parent causation mutated")
	}
	if metadata.Identity(base).Subject != identities.NotSet().Subject {
		t.Fatal("wrong absent identity")
	}
}

func TestIdentityDeduplicatesCycles(t *testing.T) {
	first := identities.Identity{Subject: "first"}
	second := identities.Identity{Subject: "second", OnBehalfOf: &first}
	first.OnBehalfOf = &second
	result := first.Snapshot()
	if result.Subject != "first" || result.OnBehalfOf.Subject != "second" || result.OnBehalfOf.OnBehalfOf != nil {
		t.Fatal("cycle not cut")
	}
}

func TestCorrelationJSON(t *testing.T) {
	id, err := metadata.ParseCorrelationID("00112233-4455-6677-8899-aabbccddeeff")
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(id)
	if err != nil || string(data) != `"00112233-4455-6677-8899-aabbccddeeff"` {
		t.Fatalf("JSON = %s, err = %v", data, err)
	}
	var roundTrip metadata.CorrelationID
	if err = json.Unmarshal(data, &roundTrip); err != nil || roundTrip != id {
		t.Fatalf("round trip = %v, err = %v", roundTrip, err)
	}
}
