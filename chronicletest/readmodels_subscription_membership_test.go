// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package chronicletest

import (
	"slices"
	"testing"

	contracts "github.com/cratis/chronicle.go/contracts/projections"
	"github.com/cratis/chronicle.go/events"
)

func membershipFrom(id string) []*contracts.KeyValuePair_EventType_FromDefinition {
	return []*contracts.KeyValuePair_EventType_FromDefinition{{Key: &contracts.EventType{Id: id}, Value: &contracts.FromDefinition{}}}
}
func membershipJoin(id string) []*contracts.KeyValuePair_EventType_JoinDefinition {
	return []*contracts.KeyValuePair_EventType_JoinDefinition{{Key: &contracts.EventType{Id: id}, Value: &contracts.JoinDefinition{}}}
}
func membershipRemoval(id string) []*contracts.KeyValuePair_EventType_RemovedWithDefinition {
	return []*contracts.KeyValuePair_EventType_RemovedWithDefinition{{Key: &contracts.EventType{Id: id}, Value: &contracts.RemovedWithDefinition{}}}
}
func membershipJoinRemoval(id string) []*contracts.KeyValuePair_EventType_RemovedWithJoinDefinition {
	return []*contracts.KeyValuePair_EventType_RemovedWithJoinDefinition{{Key: &contracts.EventType{Id: id}, Value: &contracts.RemovedWithJoinDefinition{}}}
}
func membershipProperty(id string) *contracts.FromEventPropertyDefinition {
	return &contracts.FromEventPropertyDefinition{Event: &contracts.EventType{Id: id}}
}

// Golden sets are derived from Chronicle v19.29.4 ProjectionFactory.cs:1006-1131,
// not from this walker or the kernel observer response. Ordering is immaterial.
func TestStrictMembershipMatchesKernelKeyResolverSet(t *testing.T) {
	custom := membershipFrom("A")
	custom[0].Value.Key, custom[0].Value.ParentKey = "literal", "$eventContent.parent"
	customRemoval := membershipRemoval("B")
	customRemoval[0].Value.Key, customRemoval[0].Value.ParentKey = "other", "$eventContent.owner"
	cases := []struct {
		name string
		wire *contracts.ProjectionDefinition
		want []events.TypeID
		all  bool
	}{
		{"root from and removal", &contracts.ProjectionDefinition{From: membershipFrom("A"), RemovedWith: membershipRemoval("B")}, []events.TypeID{"A", "B"}, false},
		{"root join", &contracts.ProjectionDefinition{Join: membershipJoin("J")}, []events.TypeID{"J"}, false},
		{"root join removal excluded", &contracts.ProjectionDefinition{RemovedWithJoin: membershipJoinRemoval("R")}, nil, false},
		{"recursive children", &contracts.ProjectionDefinition{Children: map[string]*contracts.ChildrenDefinition{"children": {
			From: membershipFrom("C"), Join: membershipJoin("CJ"), RemovedWith: membershipRemoval("CR"), RemovedWithJoin: membershipJoinRemoval("CRJ"),
			Children: map[string]*contracts.ChildrenDefinition{"grandchildren": {From: membershipFrom("G")}},
		}}}, []events.TypeID{"C", "CJ", "CR", "CRJ", "G"}, false},
		{"root nested exclusions", &contracts.ProjectionDefinition{Nested: map[string]*contracts.ChildrenDefinition{"nested": {
			From: membershipFrom("N"), Join: membershipJoin("NJ"), RemovedWith: membershipRemoval("NR"), RemovedWithJoin: membershipJoinRemoval("NRJ"), FromEventProperty: membershipProperty("NP"),
			Nested:   map[string]*contracts.ChildrenDefinition{"nested": {From: membershipFrom("M"), Join: membershipJoin("MJ")}},
			Children: map[string]*contracts.ChildrenDefinition{"children": {From: membershipFrom("X")}},
		}}}, []events.TypeID{"M", "MJ", "N", "NJ", "NR"}, false},
		{"child nested joins excluded recursively", &contracts.ProjectionDefinition{Children: map[string]*contracts.ChildrenDefinition{"children": {
			Nested: map[string]*contracts.ChildrenDefinition{"nested": {From: membershipFrom("CN"), Join: membershipJoin("CNJ"),
				Nested: map[string]*contracts.ChildrenDefinition{"deep": {From: membershipFrom("DN"), Join: membershipJoin("DNJ")}},
			}},
		}}}, []events.TypeID{"CN", "DN"}, false},
		{"derivatives", &contracts.ProjectionDefinition{FromEvery: []*contracts.FromDerivativesDefinition{{EventTypes: []*contracts.EventType{{Id: "D1"}, {Id: "D2"}}}}}, []events.TypeID{"D1", "D2"}, false},
		{"root event property", &contracts.ProjectionDefinition{FromEventProperty: membershipProperty("P")}, []events.TypeID{"P"}, false},
		{"child event property", &contracts.ProjectionDefinition{Children: map[string]*contracts.ChildrenDefinition{"children": {FromEventProperty: membershipProperty("CP")}}}, []events.TypeID{"CP"}, false},
		{"all mappings do not subscribe", &contracts.ProjectionDefinition{All: &contracts.FromEveryDefinition{Properties: map[string]string{"name": "$eventContent.Name"}}}, nil, false},
		{"all flag", &contracts.ProjectionDefinition{SubscribesToAllEvents: true}, nil, true},
		{"mixed all with custom all key", &contracts.ProjectionDefinition{SubscribesToAllEvents: true, From: membershipFrom("A"), All: &contracts.FromEveryDefinition{Key: "constant"}}, []events.TypeID{"A"}, true},
		{"custom keys ignored", &contracts.ProjectionDefinition{From: custom, RemovedWith: customRemoval}, []events.TypeID{"A", "B"}, false},
		{"duplicate ID across generations and paths", &contracts.ProjectionDefinition{From: membershipFrom("A"), Children: map[string]*contracts.ChildrenDefinition{"children": {From: []*contracts.KeyValuePair_EventType_FromDefinition{{Key: &contracts.EventType{Id: "A", Generation: 2}, Value: &contracts.FromDefinition{}}}}}}, []events.TypeID{"A"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, all := keyResolverEventTypeIDs(tc.wire)
			slices.Sort(got)
			if !slices.Equal(got, tc.want) || all != tc.all {
				t.Fatalf("membership = %v, all=%t; want %v, all=%t", got, all, tc.want, tc.all)
			}
		})
	}
}
