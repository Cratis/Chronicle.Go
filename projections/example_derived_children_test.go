// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package projections_test

import (
	"fmt"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/projections"
	"github.com/cratis/chronicle.go/readmodels"
	"github.com/cratis/chronicle.go/serialization"
)

// Parcel is a derived-type family; Box is its single registered derivative.
type Parcel interface{ parcel() }

type Box struct {
	BoxID  string `json:"boxId" chronicle:"key"`
	Weight int    `chronicle:"set(BoxPacked,from=Weight)"`
}

func (*Box) parcel() {}

type BoxPacked struct {
	BoxID      string
	ShipmentID string
	Weight     int
}

type Shipment struct {
	ID      string   `json:"id" chronicle:"key"`
	Parcels []Parcel `chronicle:"children(BoxPacked,key=BoxID,parent-key=ShipmentID)"`
}

func ExampleChildren_derived() {
	codecs, err := serialization.NewCodecs(serialization.Derived[Parcel, *Box]("box"))
	if err != nil {
		fmt.Println(err)
		return
	}
	model, err := readmodels.Define[Shipment](readmodels.WithCodecs(codecs))
	if err != nil {
		fmt.Println(err)
		return
	}
	event, err := events.Define[BoxPacked]()
	if err != nil {
		fmt.Println(err)
		return
	}
	catalog, err := events.NewCatalog(event.Descriptor())
	if err != nil {
		fmt.Println(err)
		return
	}
	definition, err := projections.Compile(projections.ModelBound(model), catalog)
	if err != nil {
		fmt.Println(err)
		return
	}
	child := definition.KernelDefinition().Children["Parcels"]
	from := child.From[0].Value
	fmt.Println(child.IdentifiedBy, from.Key, from.ParentKey)
	fmt.Println(from.Properties["_derivedTypeId"], from.Properties["weight"])
	// Output:
	// boxId BoxID ShipmentID
	// $value(box) Weight
}
