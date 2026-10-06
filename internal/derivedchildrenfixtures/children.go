// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

// Package derivedchildrenfixtures supplies the Go shapes corresponding to the
// captured C# derived children definition (projections/testdata/derived-children)
// for unit and real-kernel contract tests.
package derivedchildrenfixtures

import "github.com/cratis/chronicle.go/serialization"

// Child is the derived-type family of the Catalog.Items collection.
type Child interface{ Child() }

// Line is the single registered derivative of Child, mirroring the C# record
// Line([Key] string ItemId, [SetFrom<ItemAdded>, Join<ItemRenamed>] string Name).
type Line struct {
	ItemID string `json:"itemId" chronicle:"key"`
	Name   string `chronicle:"set(ItemAdded,from=Name);join(ItemRenamed,on=itemId,from=Name)"`
}

// Child marks Line as a member of the Child family.
func (*Line) Child() {}

// ItemAdded creates a Line child under the order identified by OrderID.
type ItemAdded struct {
	ItemID  string `json:"ItemId"`
	OrderID string `json:"OrderId"`
	Name    string
}

// ItemRemoved removes a Line child from the order identified by OrderID.
type ItemRemoved struct {
	ItemID  string `json:"ItemId"`
	OrderID string `json:"OrderId"`
}

// ItemUpdated renames the Line identified by ItemID under the order identified
// by OrderID. It is not a creator, yet the kernel adds the child when its
// identity is absent, so the update must carry the discriminator too.
type ItemUpdated struct {
	ItemID  string `json:"ItemId"`
	OrderID string `json:"OrderId"`
	Name    string
}

// ItemRenamed renames the Line whose itemId equals the event source.
type ItemRenamed struct{ Name string }

// Catalog is the read model with a derived children collection.
type Catalog struct {
	ID    string  `json:"Id" chronicle:"key"`
	Items []Child `chronicle:"children(ItemAdded,key=ItemId,parent-key=OrderId);remove(ItemRemoved,key=ItemId,parent-key=OrderId)"`
}

// Codecs registers Line as the only derivative of Child with identifier "line".
func Codecs() (*serialization.Codecs, error) {
	return serialization.NewCodecs(serialization.Derived[Child, *Line]("line"))
}
