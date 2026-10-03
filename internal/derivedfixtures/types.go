// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package derivedfixtures supplies the Go shapes corresponding to captured .NET
// fixtures for codec and typed-consumer contract tests.
package derivedfixtures

import "github.com/cratis/chronicle.go/serialization"

// Member is the explicitly registered family.
type Member interface{ member() }

// Detail is an ordinary nested object, governed by the configured naming policy.
type Detail struct{ DisplayName string }

// HumanValue is a pointer variant with recursive family collections.
type HumanValue struct {
	Name     string
	URLValue string
	Nested   Detail
	Children []Member
}

func (*HumanValue) member() {}

// RobotValue is a value variant.
type RobotValue struct{ Count int64 }

func (RobotValue) member() {}

// FamilyHolder retains declared family context inside an ordinary nested object.
type FamilyHolder struct{ Selected Member }

// MembersChanged matches the captured C# event, including plain concrete context.
type MembersChanged struct {
	Primary Member
	Members []Member
	Lookup  map[string]Member
	Holder  FamilyHolder
	Plain   HumanValue
}

// Codecs returns an isolated family set.
func Codecs() (*serialization.Codecs, error) {
	return serialization.NewCodecs(serialization.Derived[Member, *HumanValue]("human"), serialization.Derived[Member, RobotValue]("robot"))
}

// Sample is the value emitted by the .NET capture probe.
func Sample() MembersChanged {
	robot := RobotValue{Count: 42}
	human := &HumanValue{Name: "Ada <&é>", URLValue: "https://example.test", Nested: Detail{DisplayName: "nested"}, Children: []Member{robot}}
	return MembersChanged{Primary: human, Members: []Member{human, robot}, Lookup: map[string]Member{"one": robot}, Holder: FamilyHolder{Selected: robot}, Plain: *human}
}
