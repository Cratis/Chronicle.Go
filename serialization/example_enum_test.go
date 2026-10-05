// Copyright (c) Cratis. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

package serialization_test

import (
	"fmt"

	"github.com/cratis/chronicle.go/events"
	"github.com/cratis/chronicle.go/serialization"
)

type ApprovalStatus int32

const (
	Pending  ApprovalStatus = 0
	Approved ApprovalStatus = 1
)

type ApprovalChanged struct{ Status ApprovalStatus }

func ExampleEnum() {
	codecs, err := serialization.NewCodecs(serialization.Enum(
		serialization.EnumMember[ApprovalStatus]{Name: "Pending", Value: Pending},
		serialization.EnumMember[ApprovalStatus]{Name: "Approved", Value: Approved},
	))
	if err != nil {
		fmt.Println(err)
		return
	}
	event, err := events.Define[ApprovalChanged](events.WithCodecs(codecs))
	if err != nil {
		fmt.Println(err)
		return
	}
	data, err := event.Descriptor().Marshal(ApprovalChanged{Status: Approved})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(string(data))
	var readback ApprovalChanged
	if err := event.Descriptor().Unmarshal([]byte(`{"Status":"approved"}`), &readback); err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(readback.Status == Approved)
	// Output:
	// {"Status":1}
	// true
}
